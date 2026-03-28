using System.Text.Json;
using Microsoft.Data.SqlClient;
using AMO = Microsoft.AnalysisServices;
using Microsoft.AnalysisServices.AdomdClient;

Console.OutputEncoding = System.Text.Encoding.UTF8;
Console.WriteLine("=== OlapWorker ===");
Console.WriteLine("Polls DWHDEDEV-VDI.DWH.olapproxy.Request, executes SSAS commands, writes results back.");
Console.WriteLine("Running as: " + Environment.UserName);
Console.WriteLine("Press Ctrl+C to stop.\n");

const string CONN = "Server=DWHDEDEV-VDI;Database=DWH;Integrated Security=SSPI;TrustServerCertificate=true;";
const int POLL_MS = 2000;

var allowed = new HashSet<string>(StringComparer.OrdinalIgnoreCase)
    { "OLAP-VDI", "OLAP2-VDI", "OLAP3-VDI", "OPD-VDI", "LgOlap-VDI" };

var cts = new CancellationTokenSource();
Console.CancelKeyPress += (_, e) => { e.Cancel = true; cts.Cancel(); };

var jsonOpts = new JsonSerializerOptions { WriteIndented = false };

while (!cts.Token.IsCancellationRequested)
{
    try
    {
        await using var conn = new SqlConnection(CONN);
        await conn.OpenAsync(cts.Token);

        // Pick next pending request (atomic: set status to Processing)
        await using var pickCmd = new SqlCommand(@"
            UPDATE TOP(1) olapproxy.Request
            SET Status = 'Processing', PickedAt = SYSUTCDATETIME()
            OUTPUT INSERTED.RequestId, INSERTED.Server, INSERTED.Action, INSERTED.Parameters
            WHERE Status = 'Pending'", conn);

        await using var reader = await pickCmd.ExecuteReaderAsync(cts.Token);
        if (!await reader.ReadAsync(cts.Token))
        {
            reader.Close();
            await Task.Delay(POLL_MS, cts.Token);
            continue;
        }

        int requestId = reader.GetInt32(0);
        string server = reader.GetString(1);
        string action = reader.GetString(2);
        string? parameters = reader.IsDBNull(3) ? null : reader.GetString(3);
        reader.Close();

        Console.Write($"[{DateTime.Now:HH:mm:ss}] #{requestId} {action} @ {server}...");

        if (!allowed.Contains(server))
        {
            await WriteResult(conn, requestId, null, $"Server not allowed: {server}");
            Console.WriteLine(" BLOCKED");
            continue;
        }

        // Execute
        try
        {
            var parms = parameters != null
                ? JsonSerializer.Deserialize<Dictionary<string, string>>(parameters)
                : new Dictionary<string, string>();

            string resultJson = action switch
            {
                "ping" => JsonSerializer.Serialize(new { status = "ok", server, worker = Environment.MachineName }),
                "get_databases" => GetDatabases(server),
                "get_cubes" => GetCubes(server, parms!.GetValueOrDefault("database", "")),
                "get_dimensions" => GetDimensions(server, parms!["database"], parms["cube"]),
                "get_measures" => GetMeasures(server, parms!["database"], parms["cube"]),
                "get_dsv" => GetDsv(server, parms!["database"]),
                "get_dsv_tables" => GetDsvTables(server, parms!["database"], parms.GetValueOrDefault("dsv")),
                "get_named_query_sql" => GetNamedQuerySql(server, parms!["database"], parms.GetValueOrDefault("dsv"), parms["table"]),
                "get_full_structure" => GetFullStructure(server, parms!["database"]),
                "mdx" => ExecuteMdx(server, parms!["database"], parms!["query"]),
                _ => throw new Exception("Unknown action: " + action)
            };

            await WriteResult(conn, requestId, resultJson, null);
            Console.WriteLine($" OK ({resultJson.Length} chars)");
        }
        catch (Exception ex)
        {
            await WriteResult(conn, requestId, null, ex.Message);
            Console.WriteLine($" ERROR: {ex.Message[..Math.Min(100, ex.Message.Length)]}");
        }
    }
    catch (TaskCanceledException) { break; }
    catch (Exception ex)
    {
        Console.WriteLine($"[{DateTime.Now:HH:mm:ss}] Poll error: {ex.Message[..Math.Min(100, ex.Message.Length)]}");
        await Task.Delay(5000, cts.Token);
    }
}

Console.WriteLine("\nStopped.");

// ═══════════════════════════════════════════════════════════════════
static async Task WriteResult(SqlConnection conn, int requestId, string? result, string? error)
{
    var status = error != null ? "Error" : "Completed";
    await using var cmd = new SqlCommand(@"
        UPDATE olapproxy.Request
        SET Status = @Status, CompletedAt = SYSUTCDATETIME(),
            Result = @Result, Error = @Error
        WHERE RequestId = @Id", conn);
    cmd.Parameters.AddWithValue("@Id", requestId);
    cmd.Parameters.AddWithValue("@Status", status);
    cmd.Parameters.AddWithValue("@Result", (object?)result ?? DBNull.Value);
    cmd.Parameters.AddWithValue("@Error", (object?)error ?? DBNull.Value);
    await cmd.ExecuteNonQueryAsync();
}

// ═══════════════════════════════════════════════════════════════════
// AMO Actions
// ═══════════════════════════════════════════════════════════════════
static AMO.Server Connect(string s) { var srv = new AMO.Server(); srv.Connect($"Data Source={s};"); return srv; }

static string GetDatabases(string server)
{
    using var srv = Connect(server);
    var data = srv.Databases.Cast<AMO.Database>().Select(db => new
    { id = db.ID, name = db.Name, cubes = db.Cubes.Count }).ToList();
    return JsonSerializer.Serialize(data);
}

static string GetCubes(string server, string dbName)
{
    using var srv = Connect(server);
    var db = srv.Databases.FindByName(dbName) ?? throw new Exception("DB not found");
    var data = db.Cubes.Cast<AMO.Cube>().Select(c => new
    { id = c.ID, name = c.Name, mg = c.MeasureGroups.Count, dim = c.Dimensions.Count }).ToList();
    return JsonSerializer.Serialize(data);
}

static string GetDimensions(string server, string dbName, string cubeName)
{
    using var srv = Connect(server);
    var db = srv.Databases.FindByName(dbName) ?? throw new Exception("DB not found");
    var cube = db.Cubes.FindByName(cubeName) ?? throw new Exception("Cube not found");
    var data = cube.Dimensions.Cast<AMO.CubeDimension>().Where(cd => cd.Dimension != null).Select(cd => new
    {
        name = cd.Dimension.Name,
        attrs = cd.Dimension.Attributes.Cast<AMO.DimensionAttribute>().Select(a => new
        {
            name = a.Name,
            key = a.KeyColumns.Count > 0 && a.KeyColumns[0].Source is AMO.ColumnBinding kb
                ? $"{kb.TableID}.{kb.ColumnID}" : null,
            nameCol = a.NameColumn?.Source is AMO.ColumnBinding nb ? $"{nb.TableID}.{nb.ColumnID}" : null
        }).ToList()
    }).ToList();
    return JsonSerializer.Serialize(data);
}

static string GetMeasures(string server, string dbName, string cubeName)
{
    using var srv = Connect(server);
    var db = srv.Databases.FindByName(dbName) ?? throw new Exception("DB not found");
    var cube = db.Cubes.FindByName(cubeName) ?? throw new Exception("Cube not found");
    var data = cube.MeasureGroups.Cast<AMO.MeasureGroup>().Select(mg => new
    {
        name = mg.Name,
        measures = mg.Measures.Cast<AMO.Measure>().Select(m => new
        {
            name = m.Name, agg = m.AggregateFunction.ToString(),
            src = m.Source?.Source is AMO.ColumnBinding cb ? $"{cb.TableID}.{cb.ColumnID}" : null
        }).ToList()
    }).ToList();
    return JsonSerializer.Serialize(data);
}

static string GetDsv(string server, string dbName)
{
    using var srv = Connect(server);
    var db = srv.Databases.FindByName(dbName) ?? throw new Exception("DB not found");
    var data = db.DataSourceViews.Cast<AMO.DataSourceView>().Select(dsv => new
    {
        name = dsv.Name, tables = dsv.Schema?.Tables?.Count ?? 0,
        conn = db.DataSources.Count > 0 ? db.DataSources[0].ConnectionString : null
    }).ToList();
    return JsonSerializer.Serialize(data);
}

static string GetDsvTables(string server, string dbName, string? dsvName)
{
    using var srv = Connect(server);
    var db = srv.Databases.FindByName(dbName) ?? throw new Exception("DB not found");
    var dsv = dsvName != null ? db.DataSourceViews.FindByName(dsvName) : db.DataSourceViews[0];
    if (dsv?.Schema?.Tables == null) throw new Exception("DSV not found");
    var data = dsv.Schema.Tables.Cast<System.Data.DataTable>().Select(t => new
    {
        name = t.TableName,
        isNQ = t.ExtendedProperties.ContainsKey("QueryDefinition"),
        dbTable = t.ExtendedProperties.ContainsKey("DbTableName") ? t.ExtendedProperties["DbTableName"]?.ToString() : t.TableName,
        cols = t.Columns.Cast<System.Data.DataColumn>().Select(c => c.ColumnName).ToList()
    }).ToList();
    return JsonSerializer.Serialize(data);
}

static string GetNamedQuerySql(string server, string dbName, string? dsvName, string tableName)
{
    using var srv = Connect(server);
    var db = srv.Databases.FindByName(dbName) ?? throw new Exception("DB not found");
    var dsv = dsvName != null ? db.DataSourceViews.FindByName(dsvName) : db.DataSourceViews[0];
    var table = dsv?.Schema?.Tables?[tableName] ?? throw new Exception("Table not found");
    var sql = table.ExtendedProperties.ContainsKey("QueryDefinition")
        ? table.ExtendedProperties["QueryDefinition"]?.ToString() : null;
    return JsonSerializer.Serialize(new { tableName, isNQ = sql != null, sql });
}

static string GetFullStructure(string server, string dbName)
{
    using var srv = Connect(server);
    var db = srv.Databases.FindByName(dbName) ?? throw new Exception("DB not found");
    var data = new
    {
        server, database = db.Name,
        conn = db.DataSources.Count > 0 ? db.DataSources[0].ConnectionString : null,
        cubes = db.Cubes.Cast<AMO.Cube>().Select(c => new
        {
            name = c.Name,
            mg = c.MeasureGroups.Cast<AMO.MeasureGroup>().Select(mg => new
            {
                name = mg.Name,
                measures = mg.Measures.Cast<AMO.Measure>().Select(m => new
                {
                    name = m.Name, agg = m.AggregateFunction.ToString(),
                    src = m.Source?.Source is AMO.ColumnBinding cb ? $"{cb.TableID}.{cb.ColumnID}" : null
                }).ToList()
            }).ToList(),
            dims = c.Dimensions.Cast<AMO.CubeDimension>().Where(cd => cd.Dimension != null).Select(cd => new
            {
                name = cd.Dimension.Name,
                attrs = cd.Dimension.Attributes.Cast<AMO.DimensionAttribute>().Select(a => new
                {
                    name = a.Name,
                    key = a.KeyColumns.Count > 0 && a.KeyColumns[0].Source is AMO.ColumnBinding kb ? $"{kb.TableID}.{kb.ColumnID}" : null
                }).ToList()
            }).ToList()
        }).ToList(),
        dsvs = db.DataSourceViews.Cast<AMO.DataSourceView>().Select(dsv => new
        {
            name = dsv.Name,
            tables = dsv.Schema.Tables.Cast<System.Data.DataTable>().Select(t => new
            {
                name = t.TableName,
                isNQ = t.ExtendedProperties.ContainsKey("QueryDefinition"),
                sql = t.ExtendedProperties.ContainsKey("QueryDefinition") ? t.ExtendedProperties["QueryDefinition"]?.ToString() : null,
                cols = t.Columns.Cast<System.Data.DataColumn>().Select(c => c.ColumnName).ToList()
            }).ToList()
        }).ToList()
    };
    return JsonSerializer.Serialize(data);
}

static string ExecuteMdx(string server, string dbName, string mdx)
{
    using var conn = new AdomdConnection($"Data Source={server};Catalog={dbName};");
    conn.Open();
    using var cmd = new AdomdCommand(mdx, conn) { CommandTimeout = 120 };
    using var rdr = cmd.ExecuteReader();
    var rows = new List<Dictionary<string, object?>>();
    while (rdr.Read() && rows.Count < 10000)
    {
        var row = new Dictionary<string, object?>();
        for (int i = 0; i < rdr.FieldCount; i++)
            row[rdr.GetName(i)] = rdr.IsDBNull(i) ? null : rdr.GetValue(i);
        rows.Add(row);
    }
    return JsonSerializer.Serialize(new { rows, count = rows.Count });
}
