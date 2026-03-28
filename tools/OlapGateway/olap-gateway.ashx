<%@ WebHandler Language="C#" Class="OlapGateway" %>

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Web;
using System.Web.Script.Serialization;
using Microsoft.AnalysisServices;
using Microsoft.AnalysisServices.AdomdClient;

/// <summary>
/// OLAP Gateway — HTTP proxy for SSAS Multidimensional servers.
/// Deploy on IIS alongside SQL gateway. Uses AMO for metadata, ADOMD for MDX.
/// Auth: HMAC-SHA256 TOTP (same secret as SQL gateway).
/// </summary>
public class OlapGateway : IHttpHandler
{
    // ─── CONFIG ───────────────────────────────────────────────────────
    private const string SHARED_SECRET   = "cdOe_6JSZK4KOlqW2k9CkliSpeUsG9YYApC96dqBbz-4rrHY2JAZ1-KByVeA1KHT";
    private const string GATEWAY_VERSION = "1.0";
    private const int    TOKEN_TTL_SEC   = 300;
    private const string LOG_DIR         = @"C:\inetpub\logs\olap-gateway";
    private const int    MAX_MDX_ROWS    = 10000;

    // ─── ALLOWED SSAS SERVERS ────────────────────────────────────────
    private static readonly HashSet<string> ALLOWED_SERVERS = new HashSet<string>(StringComparer.OrdinalIgnoreCase)
    {
        "OLAP-VDI", "OLAP2-VDI", "OLAP3-VDI", "OPD-VDI", "LgOlap-VDI"
    };

    private static readonly JavaScriptSerializer Json = new JavaScriptSerializer { MaxJsonLength = int.MaxValue };

    public bool IsReusable { get { return false; } }

    // ─── ENTRY POINT ──────────────────────────────────────────────────
    public void ProcessRequest(HttpContext context)
    {
        context.Response.ContentType = "application/json; charset=utf-8";
        context.Response.AddHeader("X-Content-Type-Options", "nosniff");

        try
        {
            if (context.Request.HttpMethod != "POST")
            {
                Respond(context, 405, "Use POST");
                return;
            }

            string token = context.Request.Headers["X-Api-Key"];
            if (string.IsNullOrEmpty(token) || !ValidateToken(token))
            {
                Respond(context, 401, "Unauthorized");
                return;
            }

            string body;
            using (var reader = new StreamReader(context.Request.InputStream, Encoding.UTF8))
                body = reader.ReadToEnd();

            var request = Json.Deserialize<Dictionary<string, object>>(body);
            string action = GetStr(request, "action");
            string server = GetStr(request, "server") ?? "OLAP-VDI";

            if (!ALLOWED_SERVERS.Contains(server))
            {
                Respond(context, 400, "Server not allowed: " + server);
                return;
            }

            Log(action, context.Request.UserHostAddress, server);

            object result;
            switch (action)
            {
                case "ping":
                    result = new { status = "ok", version = GATEWAY_VERSION, server = server };
                    break;
                case "get_databases":
                    result = GetDatabases(server);
                    break;
                case "get_cubes":
                    result = GetCubes(server, GetStr(request, "database"));
                    break;
                case "get_dimensions":
                    result = GetDimensions(server, GetStr(request, "database"), GetStr(request, "cube"));
                    break;
                case "get_measures":
                    result = GetMeasures(server, GetStr(request, "database"), GetStr(request, "cube"));
                    break;
                case "get_dsv":
                    result = GetDsv(server, GetStr(request, "database"));
                    break;
                case "get_dsv_tables":
                    result = GetDsvTables(server, GetStr(request, "database"), GetStr(request, "dsv"));
                    break;
                case "get_named_query_sql":
                    result = GetNamedQuerySql(server, GetStr(request, "database"), GetStr(request, "dsv"), GetStr(request, "table"));
                    break;
                case "get_full_structure":
                    result = GetFullStructure(server, GetStr(request, "database"));
                    break;
                case "mdx":
                    result = ExecuteMdx(server, GetStr(request, "database"), GetStr(request, "query"));
                    break;
                default:
                    Respond(context, 400, "Unknown action: " + action);
                    return;
            }

            context.Response.Write(Json.Serialize(result));
        }
        catch (Exception ex)
        {
            Respond(context, 500, ex.Message);
        }
    }

    // ─── AMO ACTIONS ──────────────────────────────────────────────────

    private object GetDatabases(string serverName)
    {
        using (var srv = Connect(serverName))
        {
            return srv.Databases.Cast<Database>().Select(db => new
            {
                id = db.ID,
                name = db.Name,
                cubeCount = db.Cubes.Count,
                lastProcessed = db.LastProcessed.ToString("yyyy-MM-dd HH:mm:ss")
            }).ToList();
        }
    }

    private object GetCubes(string serverName, string dbName)
    {
        using (var srv = Connect(serverName))
        {
            var db = srv.Databases.FindByName(dbName);
            if (db == null) throw new Exception("Database not found: " + dbName);

            return db.Cubes.Cast<Cube>().Select(c => new
            {
                id = c.ID,
                name = c.Name,
                measureGroups = c.MeasureGroups.Count,
                dimensions = c.Dimensions.Count,
                lastProcessed = c.LastProcessed.ToString("yyyy-MM-dd HH:mm:ss")
            }).ToList();
        }
    }

    private object GetDimensions(string serverName, string dbName, string cubeName)
    {
        using (var srv = Connect(serverName))
        {
            var db = srv.Databases.FindByName(dbName);
            if (db == null) throw new Exception("Database not found: " + dbName);
            var cube = db.Cubes.FindByName(cubeName);
            if (cube == null) throw new Exception("Cube not found: " + cubeName);

            var result = new List<object>();
            foreach (CubeDimension cd in cube.Dimensions)
            {
                var dim = cd.Dimension;
                if (dim == null) continue;
                var attrs = dim.Attributes.Cast<DimensionAttribute>().Select(a =>
                {
                    var keyCol = a.KeyColumns.Count > 0 && a.KeyColumns[0].Source is ColumnBinding kb
                        ? new { tableId = kb.TableID, columnId = kb.ColumnID } : null;
                    var nameCol = a.NameColumn?.Source is ColumnBinding nb
                        ? new { tableId = nb.TableID, columnId = nb.ColumnID } : null;
                    return new
                    {
                        id = a.ID,
                        name = a.Name,
                        keyColumn = keyCol,
                        nameColumn = nameCol
                    };
                }).ToList();

                result.Add(new
                {
                    id = dim.ID,
                    name = dim.Name,
                    attributeCount = dim.Attributes.Count,
                    attributes = attrs
                });
            }
            return result;
        }
    }

    private object GetMeasures(string serverName, string dbName, string cubeName)
    {
        using (var srv = Connect(serverName))
        {
            var db = srv.Databases.FindByName(dbName);
            if (db == null) throw new Exception("Database not found: " + dbName);
            var cube = db.Cubes.FindByName(cubeName);
            if (cube == null) throw new Exception("Cube not found: " + cubeName);

            var result = new List<object>();
            foreach (MeasureGroup mg in cube.MeasureGroups)
            {
                var measures = mg.Measures.Cast<Measure>().Select(m =>
                {
                    var binding = m.Source?.Source is ColumnBinding cb
                        ? new { tableId = cb.TableID, columnId = cb.ColumnID } : null;
                    return new
                    {
                        id = m.ID,
                        name = m.Name,
                        aggregateFunction = m.AggregateFunction.ToString(),
                        sourceBinding = binding
                    };
                }).ToList();

                result.Add(new
                {
                    id = mg.ID,
                    name = mg.Name,
                    measureCount = mg.Measures.Count,
                    measures = measures
                });
            }
            return result;
        }
    }

    private object GetDsv(string serverName, string dbName)
    {
        using (var srv = Connect(serverName))
        {
            var db = srv.Databases.FindByName(dbName);
            if (db == null) throw new Exception("Database not found: " + dbName);

            return db.DataSourceViews.Cast<DataSourceView>().Select(dsv => new
            {
                id = dsv.ID,
                name = dsv.Name,
                tableCount = dsv.Schema?.Tables?.Count ?? 0
            }).ToList();
        }
    }

    private object GetDsvTables(string serverName, string dbName, string dsvName)
    {
        using (var srv = Connect(serverName))
        {
            var db = srv.Databases.FindByName(dbName);
            if (db == null) throw new Exception("Database not found: " + dbName);
            var dsv = db.DataSourceViews.FindByName(dsvName) ?? db.DataSourceViews[0];

            var result = new List<object>();
            foreach (System.Data.DataTable table in dsv.Schema.Tables)
            {
                bool isNQ = table.ExtendedProperties.ContainsKey("QueryDefinition");
                string dbTableName = table.ExtendedProperties.ContainsKey("DbTableName")
                    ? table.ExtendedProperties["DbTableName"]?.ToString() : table.TableName;
                string dbSchemaName = table.ExtendedProperties.ContainsKey("DbSchemaName")
                    ? table.ExtendedProperties["DbSchemaName"]?.ToString() : "dbo";

                var columns = table.Columns.Cast<System.Data.DataColumn>()
                    .Select(c => new { name = c.ColumnName, type = c.DataType.Name }).ToList();

                result.Add(new
                {
                    tableName = table.TableName,
                    isNamedQuery = isNQ,
                    hasQuery = isNQ,
                    dbTableName = dbTableName,
                    dbSchemaName = dbSchemaName,
                    columnCount = columns.Count,
                    columns = columns
                });
            }
            return result;
        }
    }

    private object GetNamedQuerySql(string serverName, string dbName, string dsvName, string tableName)
    {
        using (var srv = Connect(serverName))
        {
            var db = srv.Databases.FindByName(dbName);
            if (db == null) throw new Exception("Database not found: " + dbName);
            var dsv = db.DataSourceViews.FindByName(dsvName) ?? db.DataSourceViews[0];

            var table = dsv.Schema.Tables[tableName];
            if (table == null) throw new Exception("Table not found in DSV: " + tableName);

            string sql = table.ExtendedProperties.ContainsKey("QueryDefinition")
                ? table.ExtendedProperties["QueryDefinition"]?.ToString() : null;

            return new
            {
                tableName = tableName,
                isNamedQuery = sql != null,
                sql = sql
            };
        }
    }

    private object GetFullStructure(string serverName, string dbName)
    {
        using (var srv = Connect(serverName))
        {
            var db = srv.Databases.FindByName(dbName);
            if (db == null) throw new Exception("Database not found: " + dbName);

            // Compact structure dump
            var cubes = db.Cubes.Cast<Cube>().Select(cube => new
            {
                name = cube.Name,
                measureGroups = cube.MeasureGroups.Cast<MeasureGroup>().Select(mg => new
                {
                    name = mg.Name,
                    measures = mg.Measures.Cast<Measure>().Select(m => new
                    {
                        name = m.Name,
                        agg = m.AggregateFunction.ToString(),
                        source = m.Source?.Source is ColumnBinding cb ? cb.TableID + "." + cb.ColumnID : null
                    }).ToList()
                }).ToList(),
                dimensions = cube.Dimensions.Cast<CubeDimension>().Select(cd => new
                {
                    name = cd.Dimension?.Name,
                    attributes = cd.Dimension?.Attributes.Cast<DimensionAttribute>().Select(a => new
                    {
                        name = a.Name,
                        key = a.KeyColumns.Count > 0 && a.KeyColumns[0].Source is ColumnBinding kb
                            ? kb.TableID + "." + kb.ColumnID : null
                    }).ToList()
                }).ToList()
            }).ToList();

            var dsvs = db.DataSourceViews.Cast<DataSourceView>().Select(dsv => new
            {
                name = dsv.Name,
                tables = dsv.Schema.Tables.Cast<System.Data.DataTable>().Select(t => new
                {
                    name = t.TableName,
                    isNQ = t.ExtendedProperties.ContainsKey("QueryDefinition"),
                    columns = t.Columns.Cast<System.Data.DataColumn>().Select(c => c.ColumnName).ToList()
                }).ToList()
            }).ToList();

            var connString = db.DataSources.Count > 0 ? db.DataSources[0].ConnectionString : null;

            return new
            {
                server = serverName,
                database = db.Name,
                connectionString = connString,
                cubes = cubes,
                dataSourceViews = dsvs
            };
        }
    }

    // ─── ADOMD (MDX) ─────────────────────────────────────────────────

    private object ExecuteMdx(string serverName, string dbName, string mdxQuery)
    {
        if (string.IsNullOrEmpty(mdxQuery)) throw new Exception("Query is required");

        var connStr = string.Format("Data Source={0};Catalog={1};", serverName, dbName);
        using (var conn = new AdomdConnection(connStr))
        {
            conn.Open();
            using (var cmd = new AdomdCommand(mdxQuery, conn))
            {
                cmd.CommandTimeout = 120;
                using (var reader = cmd.ExecuteReader())
                {
                    var rows = new List<Dictionary<string, object>>();
                    int count = 0;
                    while (reader.Read() && count < MAX_MDX_ROWS)
                    {
                        var row = new Dictionary<string, object>();
                        for (int i = 0; i < reader.FieldCount; i++)
                        {
                            row[reader.GetName(i)] = reader.IsDBNull(i) ? null : reader.GetValue(i);
                        }
                        rows.Add(row);
                        count++;
                    }
                    return new { rows = rows, rowCount = count, truncated = count >= MAX_MDX_ROWS };
                }
            }
        }
    }

    // ─── HELPERS ──────────────────────────────────────────────────────

    private Server Connect(string serverName)
    {
        var srv = new Server();
        srv.Connect("Data Source=" + serverName + ";");
        return srv;
    }

    private bool ValidateToken(string token)
    {
        long now = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
        for (int offset = -TOKEN_TTL_SEC / 30; offset <= TOKEN_TTL_SEC / 30; offset++)
        {
            long window = (now / 30) + offset;
            string expected;
            using (var hmac = new HMACSHA256(Encoding.UTF8.GetBytes(SHARED_SECRET)))
            {
                expected = BitConverter.ToString(hmac.ComputeHash(Encoding.UTF8.GetBytes(window.ToString())))
                    .Replace("-", "").ToLowerInvariant();
            }
            if (string.Equals(token, expected, StringComparison.OrdinalIgnoreCase))
                return true;
        }
        return false;
    }

    private void Respond(HttpContext ctx, int code, string message)
    {
        ctx.Response.StatusCode = code;
        ctx.Response.Write(Json.Serialize(new { error = message }));
    }

    private string GetStr(Dictionary<string, object> dict, string key)
    {
        object val;
        return dict != null && dict.TryGetValue(key, out val) ? val?.ToString() : null;
    }

    private void Log(string action, string ip, string detail)
    {
        try
        {
            if (!Directory.Exists(LOG_DIR)) Directory.CreateDirectory(LOG_DIR);
            var line = string.Format("{0:yyyy-MM-dd HH:mm:ss}\t{1}\t{2}\t{3}\n",
                DateTime.UtcNow, ip, action, detail);
            File.AppendAllText(Path.Combine(LOG_DIR, DateTime.UtcNow.ToString("yyyy-MM-dd") + ".log"), line);
        }
        catch { }
    }
}
