using LineageBuilder.Core.Interfaces;
using LineageBuilder.Persistence;

var builder = WebApplication.CreateBuilder(args);

// Register services
var metaMartConn = builder.Configuration.GetConnectionString("MetaMart")
    ?? "Server=DWH-VDI;Database=MetaMart;Integrated Security=SSPI;TrustServerCertificate=true;";

builder.Services.AddSingleton<ILineageRepository>(sp =>
    new LineageRepository(metaMartConn, sp.GetRequiredService<ILogger<LineageRepository>>()));

builder.Services.AddCors(options =>
    options.AddDefaultPolicy(policy => policy.AllowAnyOrigin().AllowAnyMethod().AllowAnyHeader()));

var app = builder.Build();

app.UseCors();
app.UseStaticFiles();

// ==================== API Endpoints ====================

var api = app.MapGroup("/api/lineage");

// Search nodes
api.MapGet("/search", async (string q, int? limit, ILineageRepository repo) =>
{
    var nodes = await repo.SearchNodesAsync(q, limit ?? 50);
    return Results.Ok(nodes.Select(n => new
    {
        n.Id,
        nodeType = n.NodeType.ToString(),
        n.FullyQualifiedName,
        n.DisplayName,
        layer = n.Layer?.ToString()
    }));
});

// Get node details
api.MapGet("/node/{nodeId:int}", async (int nodeId, ILineageRepository repo) =>
{
    var node = await repo.GetNodeAsync(nodeId);
    return node == null ? Results.NotFound() : Results.Ok(node);
});

// Upstream lineage → Cytoscape.js JSON
api.MapGet("/upstream/{nodeId:int}", async (int nodeId, int? depth, ILineageRepository repo) =>
{
    var graph = await repo.GetUpstreamGraphAsync(nodeId, depth ?? 10);
    return Results.Ok(ToCytoscapeJson(graph));
});

// Downstream lineage → Cytoscape.js JSON
api.MapGet("/downstream/{nodeId:int}", async (int nodeId, int? depth, ILineageRepository repo) =>
{
    var graph = await repo.GetDownstreamGraphAsync(nodeId, depth ?? 10);
    return Results.Ok(ToCytoscapeJson(graph));
});

// Impact = upstream + downstream
api.MapGet("/impact/{nodeId:int}", async (int nodeId, int? depth, ILineageRepository repo) =>
{
    var upstream = await repo.GetUpstreamGraphAsync(nodeId, depth ?? 10);
    var downstream = await repo.GetDownstreamGraphAsync(nodeId, depth ?? 10);

    // Merge both graphs
    var nodes = upstream.Nodes.Concat(downstream.Nodes)
        .DistinctBy(n => n.FullyQualifiedName).ToList();
    var edges = upstream.Edges.Concat(downstream.Edges)
        .DistinctBy(e => $"{e.SourceNodeId}-{e.TargetNodeId}-{e.EdgeType}").ToList();

    return Results.Ok(new
    {
        elements = new
        {
            nodes = nodes.Select(n => new
            {
                data = new
                {
                    id = $"n{n.Id}",
                    label = n.DisplayName,
                    nodeType = n.NodeType.ToString(),
                    layer = n.Layer?.ToString(),
                    fqn = n.FullyQualifiedName,
                    sourceLocation = n.SourceLocation
                }
            }),
            edges = edges.Select(e => new
            {
                data = new
                {
                    id = $"e{e.Id}",
                    source = $"n{e.SourceNodeId}",
                    target = $"n{e.TargetNodeId}",
                    edgeType = e.EdgeType.ToString(),
                    mechanism = e.MechanismType?.ToString(),
                    transform = e.TransformExpression
                }
            })
        }
    });
});

app.MapFallbackToFile("index.html");
app.Run();

// ==================== Helper ====================

static object ToCytoscapeJson(LineageBuilder.Core.Model.LineageGraph graph)
{
    return new
    {
        elements = new
        {
            nodes = graph.Nodes.Select(n => new
            {
                data = new
                {
                    id = $"n{n.Id}",
                    label = n.DisplayName,
                    nodeType = n.NodeType.ToString(),
                    layer = n.Layer?.ToString(),
                    fqn = n.FullyQualifiedName,
                    sourceLocation = n.SourceLocation
                }
            }),
            edges = graph.Edges.Select(e => new
            {
                data = new
                {
                    id = $"e{e.Id}",
                    source = $"n{e.SourceNodeId}",
                    target = $"n{e.TargetNodeId}",
                    edgeType = e.EdgeType.ToString(),
                    mechanism = e.MechanismType?.ToString(),
                    transform = e.TransformExpression
                }
            })
        }
    };
}
