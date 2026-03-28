using LineageBuilder.Core.Interfaces;
using LineageBuilder.Core.Model;
using LineageBuilder.Extractors;
using LineageBuilder.Persistence;
using LineageBuilder.SqlParser;
using LineageBuilder.SqlParser.Schema;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

// Build configuration
var configuration = new ConfigurationBuilder()
    .SetBasePath(Directory.GetCurrentDirectory())
    .AddJsonFile("appsettings.json", optional: true)
    .AddJsonFile($"appsettings.{Environment.GetEnvironmentVariable("DOTNET_ENVIRONMENT") ?? "Production"}.json", optional: true)
    .AddEnvironmentVariables()
    .AddCommandLine(args)
    .Build();

// Setup DI
var services = new ServiceCollection();
services.AddLogging(builder =>
{
    builder.AddConsole();
    builder.SetMinimumLevel(LogLevel.Information);
});

var sp = services.BuildServiceProvider();
var logger = sp.GetRequiredService<ILogger<Program>>();

// Read config
var metaMartConn = configuration["ConnectionStrings:MetaMart"]
    ?? "Server=DWH-VDI;Database=MetaMart;Integrated Security=SSPI;TrustServerCertificate=true;";
var tfsUrl = configuration["Tfs:Url"]
    ?? "http://tfs-tsum:8080/tfs/tfs_olapcollection";
var tfsSSISPath = configuration["Tfs:SSISPath"]
    ?? "$/";

logger.LogInformation("=== LineageBuilder Worker ===");
logger.LogInformation("MetaMart: {Conn}", metaMartConn.Split(';').First());
logger.LogInformation("TFS: {Url}", tfsUrl);

// Create schema provider and preload
var schemaProvider = new SqlServerSchemaProvider(metaMartConn,
    sp.GetRequiredService<ILogger<SqlServerSchemaProvider>>());
logger.LogInformation("Preloading schema...");
await schemaProvider.PreloadAsync();

// Create parser
var sqlParser = new TsqlLineageParser(schemaProvider);

// Create graph
var graph = new LineageGraph();

// Create repository
var repo = new LineageRepository(metaMartConn,
    sp.GetRequiredService<ILogger<LineageRepository>>());

// Start run
var runId = await repo.StartRunAsync();
logger.LogInformation("Started run {RunId}", runId);

try
{
    // Build extractors list
    var extractors = new List<IMetadataExtractor>
    {
        new MetlExtractor(metaMartConn, metaMartConn,
            sp.GetRequiredService<ILogger<MetlExtractor>>()),
        new SqlServerExtractor(metaMartConn, sqlParser,
            sp.GetRequiredService<ILogger<SqlServerExtractor>>()),
        new SqlAgentJobExtractor(metaMartConn,
            sp.GetRequiredService<ILogger<SqlAgentJobExtractor>>()),
    };

    // Run extractors
    foreach (var extractor in extractors)
    {
        logger.LogInformation("Running extractor: {Name}", extractor.Name);
        var sw = System.Diagnostics.Stopwatch.StartNew();
        await extractor.ExtractAsync(graph);
        sw.Stop();
        logger.LogInformation("Extractor {Name} completed in {Elapsed}s — graph: {Nodes} nodes, {Edges} edges",
            extractor.Name, sw.Elapsed.TotalSeconds.ToString("F1"), graph.Nodes.Count, graph.Edges.Count);
    }

    // Persist graph
    logger.LogInformation("Persisting {Nodes} nodes and {Edges} edges...", graph.Nodes.Count, graph.Edges.Count);
    await repo.MergeNodesAsync(graph.Nodes, runId);
    await repo.MergeEdgesAsync(graph.Edges, runId);

    // Mark stale
    await repo.MarkDeletedAsync(runId);

    // Complete run
    await repo.CompleteRunAsync(runId,
        nodesCreated: graph.Nodes.Count,
        nodesUpdated: 0,
        edgesCreated: graph.Edges.Count,
        edgesUpdated: 0);

    logger.LogInformation("=== Run {RunId} completed successfully ===", runId);
    logger.LogInformation("Total: {Nodes} nodes, {Edges} edges", graph.Nodes.Count, graph.Edges.Count);
}
catch (Exception ex)
{
    logger.LogError(ex, "Run {RunId} failed", runId);
    await repo.FailRunAsync(runId, ex.ToString());
    throw;
}
