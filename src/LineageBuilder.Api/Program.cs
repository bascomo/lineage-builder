using System.Security.Cryptography;
using System.Text;
using System.Text.Json;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddCors(options =>
    options.AddDefaultPolicy(policy => policy.AllowAnyOrigin().AllowAnyMethod().AllowAnyHeader()));

builder.Services.AddSingleton<GatewayDb>();

var app = builder.Build();
app.UseCors();
app.UseStaticFiles();

var api = app.MapGroup("/api/lineage");

// Search
api.MapGet("/search", async (string q, int? limit, GatewayDb db) =>
{
    var rows = await db.SelectAsync(
        $"SELECT TOP {limit ?? 50} NodeId, nt.Name AS NodeType, n.FullyQualifiedName, n.DisplayName, n.LayerName " +
        $"FROM lineage_v2.Node n JOIN lineage_v2.NodeType nt ON n.NodeTypeId=nt.NodeTypeId " +
        $"WHERE n.IsDeleted=0 AND (n.FullyQualifiedName LIKE '%{Esc(q)}%' OR n.DisplayName LIKE '%{Esc(q)}%') " +
        $"ORDER BY CASE WHEN n.DisplayName LIKE '{Esc(q)}%' THEN 0 ELSE 1 END, n.DisplayName");
    // Map to camelCase for UI
    return Results.Ok(rows.Select(r => new {
        id = r.GetValueOrDefault("NodeId"),
        nodeType = r.GetValueOrDefault("NodeType"),
        fullyQualifiedName = r.GetValueOrDefault("FullyQualifiedName"),
        displayName = r.GetValueOrDefault("DisplayName"),
        layer = r.GetValueOrDefault("LayerName")
    }));
});

// Node detail
api.MapGet("/node/{nodeId:int}", async (int nodeId, GatewayDb db) =>
{
    var rows = await db.SelectAsync(
        $"SELECT n.NodeId, nt.Name AS NodeType, n.FullyQualifiedName, n.DisplayName, n.LayerName, n.SourceLocation, n.Description " +
        $"FROM lineage_v2.Node n JOIN lineage_v2.NodeType nt ON n.NodeTypeId=nt.NodeTypeId " +
        $"WHERE n.NodeId={nodeId} AND n.IsDeleted=0");
    return rows.Count > 0 ? Results.Ok(rows[0]) : Results.NotFound();
});

// Upstream
api.MapGet("/upstream/{nodeId:int}", async (int nodeId, int? depth, GatewayDb db) =>
{
    var g = await GetGraph(db, nodeId, depth ?? 5, upstream: true);
    return Results.Ok(new { elements = new {
        nodes = g.nodes.Select(n => new { data = n }),
        edges = g.edges.Select(e => new { data = e })
    }});
});

// Downstream
api.MapGet("/downstream/{nodeId:int}", async (int nodeId, int? depth, GatewayDb db) =>
{
    var g = await GetGraph(db, nodeId, depth ?? 5, upstream: false);
    return Results.Ok(new { elements = new {
        nodes = g.nodes.Select(n => new { data = n }),
        edges = g.edges.Select(e => new { data = e })
    }});
});

// Impact (both)
api.MapGet("/impact/{nodeId:int}", async (int nodeId, int? depth, GatewayDb db) =>
{
    var up = await GetGraph(db, nodeId, depth ?? 5, upstream: true);
    var down = await GetGraph(db, nodeId, depth ?? 5, upstream: false);

    // Merge
    var allNodes = new Dictionary<string, object>();
    foreach (var n in up.nodes.Concat(down.nodes))
        allNodes[n["id"]?.ToString() ?? ""] = n;
    var allEdges = new Dictionary<string, object>();
    foreach (var e in up.edges.Concat(down.edges))
        allEdges[e["id"]?.ToString() ?? ""] = e;

    return Results.Ok(new
    {
        elements = new
        {
            nodes = allNodes.Values.Select(n => new { data = n }),
            edges = allEdges.Values.Select(e => new { data = e })
        }
    });
});

// Stats
api.MapGet("/stats", async (GatewayDb db) =>
{
    var nodes = await db.SelectAsync("SELECT COUNT(*) AS Cnt FROM lineage_v2.Node WHERE IsDeleted=0");
    var edges = await db.SelectAsync("SELECT COUNT(*) AS Cnt FROM lineage_v2.Edge WHERE IsDeleted=0");
    var types = await db.SelectAsync(
        "SELECT nt.Name, COUNT(*) AS Cnt FROM lineage_v2.Node n JOIN lineage_v2.NodeType nt ON n.NodeTypeId=nt.NodeTypeId WHERE n.IsDeleted=0 GROUP BY nt.Name ORDER BY Cnt DESC");
    return Results.Ok(new { nodeCount = nodes[0]["Cnt"], edgeCount = edges[0]["Cnt"], byType = types });
});

app.MapFallbackToFile("index.html");
app.Run();

// ═══════════════════════════════════════════════════════════════════
async Task<(List<Dictionary<string, object?>> nodes, List<Dictionary<string, object?>> edges)>
    GetGraph(GatewayDb db, int nodeId, int depth, bool upstream)
{
    // BFS traversal through multiple gateway calls (CTE not reliable via gateway)
    var visited = new HashSet<long> { nodeId };
    var frontier = new List<long> { nodeId };
    var allEdgeRows = new List<Dictionary<string, object?>>();

    for (int d = 0; d < depth && frontier.Count > 0; d++)
    {
        var ids = string.Join(",", frontier);
        var edgeCol = upstream ? "TargetNodeId" : "SourceNodeId";
        var nextCol = upstream ? "SourceNodeId" : "TargetNodeId";

        var batchEdges = await db.SelectAsync(
            $"SELECT EdgeId AS id, SourceNodeId AS source, TargetNodeId AS target, EdgeType AS edgeType " +
            $"FROM lineage_v2.Edge WHERE {edgeCol} IN ({ids}) AND IsDeleted=0");

        allEdgeRows.AddRange(batchEdges);
        frontier = new List<long>();
        foreach (var e in batchEdges)
        {
            var nextId = Convert.ToInt64(e[upstream ? "source" : "target"]);
            if (visited.Add(nextId))
                frontier.Add(nextId);
        }
    }

    // Fetch node details for all visited nodes
    var nodeIds = string.Join(",", visited);
    var nodeRows = visited.Count > 0
        ? await db.SelectAsync(
            $"SELECT n.NodeId AS id, nt.Name AS nodeType, n.DisplayName AS label, n.LayerName AS layer, n.FullyQualifiedName AS fqn " +
            $"FROM lineage_v2.Node n JOIN lineage_v2.NodeType nt ON n.NodeTypeId=nt.NodeTypeId " +
            $"WHERE n.NodeId IN ({nodeIds})")
        : new List<Dictionary<string, object?>>();

    var edgeRows = allEdgeRows;

    // Convert to Cytoscape format
    var nodes = nodeRows.Select(n =>
    {
        n["id"] = $"n{n["id"]}";
        return n;
    }).ToList();

    var edges = edgeRows.Select(e =>
    {
        e["id"] = $"e{e["id"]}";
        e["source"] = $"n{e["source"]}";
        e["target"] = $"n{e["target"]}";
        return e;
    }).ToList();

    return (nodes, edges);
}

static string Esc(string s) => s.Replace("'", "''");

// ═══════════════════════════════════════════════════════════════════
// Gateway-based DB access
// ═══════════════════════════════════════════════════════════════════
class GatewayDb
{
    const string GW_URL = "https://ibmcognos-test.mhpost.ru/gateway/gateway.ashx";
    const string SECRET = "cdOe_6JSZK4KOlqW2k9CkliSpeUsG9YYApC96dqBbz-4rrHY2JAZ1-KByVeA1KHT";
    const string SERVER = "DWHDEDEV-VDI";
    const string DATABASE = "DWH";

    readonly HttpClient _http;

    public GatewayDb()
    {
        var h = new HttpClientHandler { ServerCertificateCustomValidationCallback = (_, _, _, _) => true };
        _http = new HttpClient(h) { Timeout = TimeSpan.FromMinutes(2) };
    }

    string Token()
    {
        var w = DateTimeOffset.UtcNow.ToUnixTimeSeconds() / 30;
        using var hmac = new HMACSHA256(Encoding.UTF8.GetBytes(SECRET));
        return Convert.ToHexString(hmac.ComputeHash(Encoding.UTF8.GetBytes(w.ToString()))).ToLowerInvariant();
    }

    public async Task<List<Dictionary<string, object?>>> SelectAsync(string query)
    {
        var payload = JsonSerializer.Serialize(new
        {
            action = "select_full",
            database = DATABASE,
            query,
            server = SERVER
        });

        var req = new HttpRequestMessage(HttpMethod.Post, GW_URL)
        {
            Content = new StringContent(payload, Encoding.UTF8, "application/json")
        };
        req.Headers.TryAddWithoutValidation("X-Api-Key", Token());

        var resp = await _http.SendAsync(req);
        var body = await resp.Content.ReadAsStringAsync();
        resp.EnsureSuccessStatusCode();

        // Gateway wraps in {"data":[...]}
        using var doc = JsonDocument.Parse(body);
        JsonElement array;
        if (doc.RootElement.TryGetProperty("data", out var dataEl))
            array = dataEl;
        else
            array = doc.RootElement;

        var result = new List<Dictionary<string, object?>>();
        foreach (var row in array.EnumerateArray())
        {
            var dict = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);
            foreach (var prop in row.EnumerateObject())
            {
                dict[prop.Name] = prop.Value.ValueKind switch
                {
                    JsonValueKind.Number => prop.Value.TryGetInt64(out var l) ? l : prop.Value.GetDouble(),
                    JsonValueKind.String => prop.Value.GetString(),
                    JsonValueKind.True => true,
                    JsonValueKind.False => false,
                    JsonValueKind.Null => null,
                    _ => prop.Value.GetRawText()
                };
            }
            result.Add(dict);
        }
        return result;
    }
}
