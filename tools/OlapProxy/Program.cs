using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using AMO = Microsoft.AnalysisServices;
using Microsoft.AnalysisServices.AdomdClient;

var builder = WebApplication.CreateBuilder(args);
builder.WebHost.UseUrls("http://*:5051"); // default port, override via --urls

var app = builder.Build();

const string SECRET = "cdOe_6JSZK4KOlqW2k9CkliSpeUsG9YYApC96dqBbz-4rrHY2JAZ1-KByVeA1KHT";
var ALLOWED = new HashSet<string>(StringComparer.OrdinalIgnoreCase)
    { "OLAP-VDI", "OLAP2-VDI", "OLAP3-VDI", "OPD-VDI", "LgOlap-VDI" };

var jsonOpts = new JsonSerializerOptions { WriteIndented = false, PropertyNamingPolicy = JsonNamingPolicy.CamelCase };

// ─── Single POST endpoint ─────────────────────────────────────────
app.MapPost("/", async (HttpContext ctx) =>
{
    ctx.Response.ContentType = "application/json; charset=utf-8";

    // Auth
    var token = ctx.Request.Headers["X-Api-Key"].FirstOrDefault();
    if (string.IsNullOrEmpty(token) || !ValidateToken(token, SECRET))
    {
        ctx.Response.StatusCode = 401;
        await ctx.Response.WriteAsync("""{"error":"Unauthorized"}""");
        return;
    }

    Dictionary<string, JsonElement>? req;
    try { req = await ctx.Request.ReadFromJsonAsync<Dictionary<string, JsonElement>>(); }
    catch { ctx.Response.StatusCode = 400; await ctx.Response.WriteAsync("""{"error":"Invalid JSON"}"""); return; }
    if (req == null) { ctx.Response.StatusCode = 400; return; }

    var action = Str(req, "action");
    var server = Str(req, "server") ?? "OLAP-VDI";

    if (!ALLOWED.Contains(server))
    {
        ctx.Response.StatusCode = 400;
        await ctx.Response.WriteAsync($$$"""{"error":"Server not allowed: {{{server}}}"}""");
        return;
    }

    try
    {
        object result = action switch
        {
            "ping" => new { status = "ok", version = "1.0", server },
            "get_databases" => GetDatabases(server),
            "get_cubes" => GetCubes(server, Str(req, "database")!),
            "get_dimensions" => GetDimensions(server, Str(req, "database")!, Str(req, "cube")!),
            "get_measures" => GetMeasures(server, Str(req, "database")!, Str(req, "cube")!),
            "get_dsv" => GetDsv(server, Str(req, "database")!),
            "get_dsv_tables" => GetDsvTables(server, Str(req, "database")!, Str(req, "dsv")),
            "get_named_query_sql" => GetNamedQuerySql(server, Str(req, "database")!, Str(req, "dsv"), Str(req, "table")!),
            "get_full_structure" => GetFullStructure(server, Str(req, "database")!),
            "mdx" => ExecuteMdx(server, Str(req, "database")!, Str(req, "query")!),
            _ => throw new ArgumentException("Unknown action: " + action)
        };
        await ctx.Response.WriteAsJsonAsync(result, jsonOpts);
    }
    catch (Exception ex)
    {
        ctx.Response.StatusCode = 500;
        await ctx.Response.WriteAsync(System.Text.Json.JsonSerializer.Serialize(new { error = ex.Message }, jsonOpts));
    }
});

app.Logger.LogInformation("OLAP Proxy starting on {Urls}", string.Join(", ", app.Urls));
app.Logger.LogInformation("Allowed servers: {Servers}", string.Join(", ", ALLOWED));
app.Run();

// ═══════════════════════════════════════════════════════════════════
// AMO Actions
// ═══════════════════════════════════════════════════════════════════

static object GetDatabases(string serverName)
{
    using var srv = Connect(serverName);
    return srv.Databases.Cast<AMO.Database>().Select(db => new
    {
        id = db.ID, name = db.Name,
        cubeCount = db.Cubes.Count,
        lastProcessed = db.LastProcessed.ToString("yyyy-MM-dd HH:mm:ss")
    }).ToList();
}

static object GetCubes(string serverName, string dbName)
{
    using var srv = Connect(serverName);
    var db = srv.Databases.FindByName(dbName) ?? throw new Exception("DB not found: " + dbName);
    return db.Cubes.Cast<AMO.Cube>().Select(c => new
    {
        id = c.ID, name = c.Name,
        measureGroups = c.MeasureGroups.Count,
        dimensions = c.Dimensions.Count,
        lastProcessed = c.LastProcessed.ToString("yyyy-MM-dd HH:mm:ss")
    }).ToList();
}

static object GetDimensions(string serverName, string dbName, string cubeName)
{
    using var srv = Connect(serverName);
    var db = srv.Databases.FindByName(dbName) ?? throw new Exception("DB not found");
    var cube = db.Cubes.FindByName(cubeName) ?? throw new Exception("Cube not found");

    return cube.Dimensions.Cast<AMO.CubeDimension>().Where(cd => cd.Dimension != null).Select(cd =>
    {
        var dim = cd.Dimension;
        return new
        {
            id = dim.ID, name = dim.Name,
            attributes = dim.Attributes.Cast<AMO.DimensionAttribute>().Select(a => new
            {
                id = a.ID, name = a.Name,
                keyColumn = a.KeyColumns.Count > 0 && a.KeyColumns[0].Source is AMO.ColumnBinding kb
                    ? new { tableId = kb.TableID, columnId = kb.ColumnID } : (object?)null,
                nameColumn = a.NameColumn?.Source is AMO.ColumnBinding nb
                    ? new { tableId = nb.TableID, columnId = nb.ColumnID } : (object?)null
            }).ToList()
        };
    }).ToList();
}

static object GetMeasures(string serverName, string dbName, string cubeName)
{
    using var srv = Connect(serverName);
    var db = srv.Databases.FindByName(dbName) ?? throw new Exception("DB not found");
    var cube = db.Cubes.FindByName(cubeName) ?? throw new Exception("Cube not found");

    return cube.MeasureGroups.Cast<AMO.MeasureGroup>().Select(mg => new
    {
        id = mg.ID, name = mg.Name,
        measures = mg.Measures.Cast<AMO.Measure>().Select(m => new
        {
            id = m.ID, name = m.Name,
            aggregateFunction = m.AggregateFunction.ToString(),
            sourceBinding = m.Source?.Source is AMO.ColumnBinding cb
                ? new { tableId = cb.TableID, columnId = cb.ColumnID } : (object?)null
        }).ToList()
    }).ToList();
}

static object GetDsv(string serverName, string dbName)
{
    using var srv = Connect(serverName);
    var db = srv.Databases.FindByName(dbName) ?? throw new Exception("DB not found");
    return db.DataSourceViews.Cast<AMO.DataSourceView>().Select(dsv => new
    {
        id = dsv.ID, name = dsv.Name,
        tableCount = dsv.Schema?.Tables?.Count ?? 0,
        connectionString = db.DataSources.Count > 0 ? db.DataSources[0].ConnectionString : null
    }).ToList();
}

static object GetDsvTables(string serverName, string dbName, string? dsvName)
{
    using var srv = Connect(serverName);
    var db = srv.Databases.FindByName(dbName) ?? throw new Exception("DB not found");
    var dsv = dsvName != null ? db.DataSourceViews.FindByName(dsvName) : db.DataSourceViews[0];
    if (dsv?.Schema?.Tables == null) throw new Exception("DSV not found");

    return dsv.Schema.Tables.Cast<System.Data.DataTable>().Select(t =>
    {
        bool isNQ = t.ExtendedProperties.ContainsKey("QueryDefinition");
        return new
        {
            tableName = t.TableName,
            isNamedQuery = isNQ,
            dbTableName = t.ExtendedProperties.ContainsKey("DbTableName")
                ? t.ExtendedProperties["DbTableName"]?.ToString() : t.TableName,
            dbSchemaName = t.ExtendedProperties.ContainsKey("DbSchemaName")
                ? t.ExtendedProperties["DbSchemaName"]?.ToString() : "dbo",
            columns = t.Columns.Cast<System.Data.DataColumn>()
                .Select(c => new { name = c.ColumnName, type = c.DataType.Name }).ToList()
        };
    }).ToList();
}

static object GetNamedQuerySql(string serverName, string dbName, string? dsvName, string tableName)
{
    using var srv = Connect(serverName);
    var db = srv.Databases.FindByName(dbName) ?? throw new Exception("DB not found");
    var dsv = dsvName != null ? db.DataSourceViews.FindByName(dsvName) : db.DataSourceViews[0];
    var table = dsv?.Schema?.Tables?[tableName] ?? throw new Exception("Table not found in DSV");

    return new
    {
        tableName,
        isNamedQuery = table.ExtendedProperties.ContainsKey("QueryDefinition"),
        sql = table.ExtendedProperties.ContainsKey("QueryDefinition")
            ? table.ExtendedProperties["QueryDefinition"]?.ToString() : null
    };
}

static object GetFullStructure(string serverName, string dbName)
{
    using var srv = Connect(serverName);
    var db = srv.Databases.FindByName(dbName) ?? throw new Exception("DB not found");

    var cubes = db.Cubes.Cast<AMO.Cube>().Select(cube => new
    {
        name = cube.Name,
        measureGroups = cube.MeasureGroups.Cast<AMO.MeasureGroup>().Select(mg => new
        {
            name = mg.Name,
            measures = mg.Measures.Cast<AMO.Measure>().Select(m => new
            {
                name = m.Name,
                agg = m.AggregateFunction.ToString(),
                source = m.Source?.Source is AMO.ColumnBinding cb ? $"{cb.TableID}.{cb.ColumnID}" : null
            }).ToList()
        }).ToList(),
        dimensions = cube.Dimensions.Cast<AMO.CubeDimension>()
            .Where(cd => cd.Dimension != null)
            .Select(cd => new
        {
            name = cd.Dimension.Name,
            attributes = cd.Dimension.Attributes.Cast<AMO.DimensionAttribute>().Select(a => new
            {
                name = a.Name,
                key = a.KeyColumns.Count > 0 && a.KeyColumns[0].Source is AMO.ColumnBinding kb
                    ? $"{kb.TableID}.{kb.ColumnID}" : null
            }).ToList()
        }).ToList()
    }).ToList();

    var dsvs = db.DataSourceViews.Cast<AMO.DataSourceView>().Select(dsv => new
    {
        name = dsv.Name,
        tables = dsv.Schema.Tables.Cast<System.Data.DataTable>().Select(t => new
        {
            name = t.TableName,
            isNQ = t.ExtendedProperties.ContainsKey("QueryDefinition"),
            sql = t.ExtendedProperties.ContainsKey("QueryDefinition")
                ? t.ExtendedProperties["QueryDefinition"]?.ToString() : null,
            columns = t.Columns.Cast<System.Data.DataColumn>().Select(c => c.ColumnName).ToList()
        }).ToList()
    }).ToList();

    return new
    {
        server = serverName,
        database = db.Name,
        connectionString = db.DataSources.Count > 0 ? db.DataSources[0].ConnectionString : null,
        cubes, dataSourceViews = dsvs
    };
}

static object ExecuteMdx(string serverName, string dbName, string mdxQuery)
{
    using var conn = new AdomdConnection($"Data Source={serverName};Catalog={dbName};");
    conn.Open();
    using var cmd = new AdomdCommand(mdxQuery, conn) { CommandTimeout = 120 };
    using var reader = cmd.ExecuteReader();

    var rows = new List<Dictionary<string, object?>>();
    while (reader.Read() && rows.Count < 10000)
    {
        var row = new Dictionary<string, object?>();
        for (int i = 0; i < reader.FieldCount; i++)
            row[reader.GetName(i)] = reader.IsDBNull(i) ? null : reader.GetValue(i);
        rows.Add(row);
    }
    return new { rows, rowCount = rows.Count, truncated = rows.Count >= 10000 };
}

// ═══════════════════════════════════════════════════════════════════
// Helpers
// ═══════════════════════════════════════════════════════════════════

static AMO.Server Connect(string serverName)
{
    var srv = new AMO.Server();
    srv.Connect($"Data Source={serverName};");
    return srv;
}

static bool ValidateToken(string token, string secret)
{
    long now = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
    for (int offset = -10; offset <= 10; offset++) // ±5 min
    {
        long window = (now / 30) + offset;
        using var hmac = new HMACSHA256(Encoding.UTF8.GetBytes(secret));
        var expected = Convert.ToHexString(hmac.ComputeHash(Encoding.UTF8.GetBytes(window.ToString()))).ToLowerInvariant();
        if (string.Equals(token, expected, StringComparison.OrdinalIgnoreCase)) return true;
    }
    return false;
}

static string? Str(Dictionary<string, JsonElement> d, string key) =>
    d.TryGetValue(key, out var v) && v.ValueKind == JsonValueKind.String ? v.GetString() : null;
