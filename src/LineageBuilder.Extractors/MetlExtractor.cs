using Dapper;
using Microsoft.Data.SqlClient;
using LineageBuilder.Core.Interfaces;
using LineageBuilder.Core.Model;
using Microsoft.Extensions.Logging;

namespace LineageBuilder.Extractors;

/// <summary>
/// Extracts lineage from mETL (Mercury ETL) configuration.
/// mETL copies data 1:1 from source tables to staging without renaming columns or tables.
/// Metadata is stored in MetaMart.Metadata.Objects/ObjectFields/Sources.
/// </summary>
public class MetlExtractor : IMetadataExtractor
{
    private readonly string _metaMartConnectionString;
    private readonly string _stagingConnectionString;
    private readonly ILogger<MetlExtractor>? _logger;

    public string Name => "mETL";

    public MetlExtractor(string metaMartConnectionString, string stagingConnectionString,
        ILogger<MetlExtractor>? logger = null)
    {
        _metaMartConnectionString = metaMartConnectionString;
        _stagingConnectionString = stagingConnectionString;
        _logger = logger;
    }

    public async Task ExtractAsync(LineageGraph graph, CancellationToken cancellationToken = default)
    {
        _logger?.LogInformation("Starting mETL lineage extraction...");

        await using var conn = new SqlConnection(_metaMartConnectionString);
        await conn.OpenAsync(cancellationToken);

        // Get all active mETL source-to-staging mappings
        var mappings = await conn.QueryAsync<MetlMapping>(@"
            SELECT
                s.SourceCode,
                s.ServerName AS SourceServer,
                s.DBName AS SourceDB,
                o.ObjectOriginSchema AS SourceSchema,
                o.ObjectOriginName AS SourceTable,
                o.ObjectId
            FROM Metadata.Objects o
            INNER JOIN Metadata.Sources s ON s.SourceId = o.SourceId
            WHERE o.IsDeprecated = 0
              AND s.IsActive = 1");

        var mappingList = mappings.ToList();
        _logger?.LogInformation("Found {Count} mETL source-staging mappings", mappingList.Count);

        foreach (var mapping in mappingList)
        {
            if (cancellationToken.IsCancellationRequested) break;

            // Get columns for this object
            var columns = await conn.QueryAsync<string>(@"
                SELECT f.FieldOriginName
                FROM Metadata.ObjectFields f
                WHERE f.ObjectId = @ObjectId
                  AND f.IsDeprecated = 0
                ORDER BY f.FieldId",
                new { mapping.ObjectId });

            var columnList = columns.ToList();
            if (columnList.Count == 0) continue;

            // Source table FQN
            var sourceFqn = $"{mapping.SourceServer}.{mapping.SourceDB}.{mapping.SourceSchema}.{mapping.SourceTable}";
            // Staging table FQN — mETL copies 1:1, same schema/table name in StagingArea DB
            var stagingFqn = $"DWH-VDI.StagingArea.{mapping.SourceCode}.{mapping.SourceTable}";

            // Add source table node
            var sourceTableNode = graph.AddNode(new LineageNode
            {
                NodeTypeId = WellKnownNodeTypes.Table,
                NodeTypeName = nameof(WellKnownNodeTypes.Table),
                FullyQualifiedName = sourceFqn,
                DisplayName = $"{mapping.SourceDB}.{mapping.SourceSchema}.{mapping.SourceTable}",
                LayerName = Layers.Source,
                Metadata = new Dictionary<string, string>
                {
                    ["server"] = mapping.SourceServer,
                    ["database"] = mapping.SourceDB,
                    ["schema"] = mapping.SourceSchema
                }
            });

            // Add staging table node
            var stagingTableNode = graph.AddNode(new LineageNode
            {
                NodeTypeId = WellKnownNodeTypes.Table,
                NodeTypeName = nameof(WellKnownNodeTypes.Table),
                FullyQualifiedName = stagingFqn,
                DisplayName = $"StagingArea.{mapping.SourceCode}.{mapping.SourceTable}",
                LayerName = Layers.Staging,
                Metadata = new Dictionary<string, string>
                {
                    ["database"] = "StagingArea",
                    ["schema"] = mapping.SourceCode,
                    ["sourceCode"] = mapping.SourceCode
                }
            });

            // Add mETL mapping node as mechanism
            var metlMappingNode = graph.AddNode(new LineageNode
            {
                NodeTypeId = WellKnownNodeTypes.MetlMapping,
                NodeTypeName = nameof(WellKnownNodeTypes.MetlMapping),
                FullyQualifiedName = $"[mETL].{mapping.SourceCode}",
                DisplayName = $"mETL:{mapping.SourceCode}"
            });

            // Add column-level lineage (1:1 copy)
            foreach (var col in columnList)
            {
                var sourceColFqn = $"{sourceFqn}.{col}";
                var stagingColFqn = $"{stagingFqn}.{col}";

                var sourceColNode = graph.AddNode(new LineageNode
                {
                    NodeTypeId = WellKnownNodeTypes.Column,
                    NodeTypeName = nameof(WellKnownNodeTypes.Column),
                    FullyQualifiedName = sourceColFqn,
                    DisplayName = col,
                    LayerName = Layers.Source
                });

                var stagingColNode = graph.AddNode(new LineageNode
                {
                    NodeTypeId = WellKnownNodeTypes.Column,
                    NodeTypeName = nameof(WellKnownNodeTypes.Column),
                    FullyQualifiedName = stagingColFqn,
                    DisplayName = col,
                    LayerName = Layers.Staging
                });

                graph.AddEdge(new LineageEdge
                {
                    SourceNodeId = sourceColNode.Id,
                    TargetNodeId = stagingColNode.Id,
                    EdgeType = EdgeTypes.DirectCopy,
                    MechanismNodeId = metlMappingNode.Id,
                    TransformExpression = "1:1 copy"
                });
            }
        }

        _logger?.LogInformation("mETL extraction complete: {Nodes} nodes, {Edges} edges in graph",
            graph.Nodes.Count, graph.Edges.Count);
    }

    private class MetlMapping
    {
        public string SourceCode { get; set; } = "";
        public string SourceServer { get; set; } = "";
        public string SourceDB { get; set; } = "";
        public string SourceSchema { get; set; } = "";
        public string SourceTable { get; set; } = "";
        public int ObjectId { get; set; }
    }
}
