using System.Text.Json;
using Dapper;
using Microsoft.Data.SqlClient;
using LineageBuilder.Core.Interfaces;
using LineageBuilder.Core.Model;
using Microsoft.Extensions.Logging;

namespace LineageBuilder.Persistence;

/// <summary>
/// Repository for persisting lineage graph to SQL Server (lineage2 schema).
/// </summary>
public class LineageRepository : ILineageRepository
{
    private readonly string _connectionString;
    private readonly ILogger<LineageRepository>? _logger;

    public LineageRepository(string connectionString, ILogger<LineageRepository>? logger = null)
    {
        _connectionString = connectionString;
        _logger = logger;
    }

    public async Task<int> StartRunAsync(CancellationToken ct = default)
    {
        await using var conn = new SqlConnection(_connectionString);
        await conn.OpenAsync(ct);
        var runId = await conn.ExecuteScalarAsync<int>(
            "INSERT INTO lineage_v2.Run (StartedAt, Status) OUTPUT INSERTED.RunId VALUES (SYSUTCDATETIME(), 'Running')");
        _logger?.LogInformation("Started lineage run {RunId}", runId);
        return runId;
    }

    public async Task CompleteRunAsync(int runId, int nodesCreated, int nodesUpdated,
        int edgesCreated, int edgesUpdated, CancellationToken ct = default)
    {
        await using var conn = new SqlConnection(_connectionString);
        await conn.OpenAsync(ct);
        await conn.ExecuteAsync(@"
            UPDATE lineage_v2.Run
            SET CompletedAt = SYSUTCDATETIME(), Status = 'Completed',
                NodesCreated = @NodesCreated, NodesUpdated = @NodesUpdated,
                EdgesCreated = @EdgesCreated, EdgesUpdated = @EdgesUpdated
            WHERE RunId = @RunId",
            new { RunId = runId, NodesCreated = nodesCreated, NodesUpdated = nodesUpdated,
                  EdgesCreated = edgesCreated, EdgesUpdated = edgesUpdated });
        _logger?.LogInformation("Completed run {RunId}: {NodesCreated} nodes created, {EdgesCreated} edges created",
            runId, nodesCreated, edgesCreated);
    }

    public async Task FailRunAsync(int runId, string error, CancellationToken ct = default)
    {
        await using var conn = new SqlConnection(_connectionString);
        await conn.OpenAsync(ct);
        await conn.ExecuteAsync(@"
            UPDATE lineage_v2.Run
            SET CompletedAt = SYSUTCDATETIME(), Status = 'Failed', ErrorLog = @Error
            WHERE RunId = @RunId",
            new { RunId = runId, Error = error });
        _logger?.LogError("Failed run {RunId}: {Error}", runId, error);
    }

    public async Task MergeNodesAsync(IEnumerable<LineageNode> nodes, int runId, CancellationToken ct = default)
    {
        await using var conn = new SqlConnection(_connectionString);
        await conn.OpenAsync(ct);

        // Use temp table + MERGE for efficiency
        await conn.ExecuteAsync(@"
            CREATE TABLE #TempNodes (
                FullyQualifiedName NVARCHAR(1000),
                NodeTypeId INT,
                NodeTypeName VARCHAR(50),
                DisplayName NVARCHAR(500),
                SourceLocation NVARCHAR(1000),
                LayerName VARCHAR(100),
                Metadata NVARCHAR(MAX)
            )");

        foreach (var batch in nodes.Chunk(1000))
        {
            foreach (var node in batch)
            {
                await conn.ExecuteAsync(@"
                    INSERT INTO #TempNodes VALUES (@FQN, @NodeTypeId, @NodeTypeName, @DisplayName, @SourceLocation, @LayerName, @Metadata)",
                    new
                    {
                        FQN = node.FullyQualifiedName,
                        node.NodeTypeId,
                        node.NodeTypeName,
                        node.DisplayName,
                        node.SourceLocation,
                        node.LayerName,
                        Metadata = node.Metadata.Count > 0 ? JsonSerializer.Serialize(node.Metadata) : null
                    });
            }
        }

        var affected = await conn.ExecuteAsync($@"
            MERGE lineage_v2.Node AS tgt
            USING #TempNodes AS src ON tgt.FullyQualifiedName = src.FullyQualifiedName
            WHEN MATCHED THEN
                UPDATE SET
                    tgt.NodeTypeId = src.NodeTypeId,
                    tgt.DisplayName = src.DisplayName,
                    tgt.SourceLocation = src.SourceLocation,
                    tgt.LayerName = src.LayerName,
                    tgt.Metadata = src.Metadata,
                    tgt.UpdatedAt = SYSUTCDATETIME(),
                    tgt.LastSeenRunId = {runId},
                    tgt.IsDeleted = 0
            WHEN NOT MATCHED THEN
                INSERT (NodeTypeId, FullyQualifiedName, DisplayName, SourceLocation, LayerName, Metadata, LastSeenRunId)
                VALUES (src.NodeTypeId, src.FullyQualifiedName, src.DisplayName, src.SourceLocation, src.LayerName, src.Metadata, {runId});

            DROP TABLE #TempNodes;");

        _logger?.LogInformation("Merged {Count} nodes", affected);
    }

    public async Task MergeEdgesAsync(IEnumerable<LineageEdge> edges, int runId, CancellationToken ct = default)
    {
        await using var conn = new SqlConnection(_connectionString);
        await conn.OpenAsync(ct);

        foreach (var batch in edges.Chunk(500))
        {
            foreach (var edge in batch)
            {
                await conn.ExecuteAsync($@"
                    MERGE lineage_v2.Edge AS tgt
                    USING (SELECT @SourceNodeId AS SourceNodeId, @TargetNodeId AS TargetNodeId,
                                  @EdgeType AS EdgeType, @MechanismNodeId AS MechanismNodeId) AS src
                    ON tgt.SourceNodeId = src.SourceNodeId
                       AND tgt.TargetNodeId = src.TargetNodeId
                       AND tgt.EdgeType = src.EdgeType
                       AND ISNULL(tgt.MechanismNodeId, 0) = ISNULL(src.MechanismNodeId, 0)
                    WHEN MATCHED THEN
                        UPDATE SET
                            tgt.TransformExpression = @TransformExpression,
                            tgt.LastSeenRunId = {runId},
                            tgt.IsDeleted = 0
                    WHEN NOT MATCHED THEN
                        INSERT (SourceNodeId, TargetNodeId, EdgeType, MechanismNodeId, TransformExpression, LastSeenRunId)
                        VALUES (@SourceNodeId, @TargetNodeId, @EdgeType, @MechanismNodeId, @TransformExpression, {runId});",
                    new
                    {
                        edge.SourceNodeId,
                        edge.TargetNodeId,
                        edge.EdgeType,
                        edge.MechanismNodeId,
                        edge.TransformExpression
                    });
            }
        }
    }

    public async Task MarkDeletedAsync(int runId, CancellationToken ct = default)
    {
        await using var conn = new SqlConnection(_connectionString);
        await conn.OpenAsync(ct);

        var nodesDeleted = await conn.ExecuteAsync(
            "UPDATE lineage_v2.Node SET IsDeleted = 1 WHERE LastSeenRunId < @RunId AND IsDeleted = 0",
            new { RunId = runId });
        var edgesDeleted = await conn.ExecuteAsync(
            "UPDATE lineage_v2.Edge SET IsDeleted = 1 WHERE LastSeenRunId < @RunId AND IsDeleted = 0",
            new { RunId = runId });

        _logger?.LogInformation("Marked deleted: {Nodes} nodes, {Edges} edges", nodesDeleted, edgesDeleted);
    }

    public async Task<LineageGraph> GetUpstreamGraphAsync(int nodeId, int depth = 10, CancellationToken ct = default)
    {
        return await GetGraphAsync(nodeId, depth, upstream: true, ct);
    }

    public async Task<LineageGraph> GetDownstreamGraphAsync(int nodeId, int depth = 10, CancellationToken ct = default)
    {
        return await GetGraphAsync(nodeId, depth, upstream: false, ct);
    }

    private async Task<LineageGraph> GetGraphAsync(int nodeId, int depth, bool upstream, CancellationToken ct)
    {
        await using var conn = new SqlConnection(_connectionString);
        await conn.OpenAsync(ct);

        var direction = upstream
            ? "e.TargetNodeId = cte.NodeId"
            : "e.SourceNodeId = cte.NodeId";
        var nextNode = upstream ? "e.SourceNodeId" : "e.TargetNodeId";

        var sql = $@"
            ;WITH cte AS (
                SELECT NodeId, 0 AS Depth FROM lineage_v2.Node WHERE NodeId = @NodeId AND IsDeleted = 0
                UNION ALL
                SELECT {nextNode}, cte.Depth + 1
                FROM lineage_v2.Edge e
                INNER JOIN cte ON {direction}
                WHERE e.IsDeleted = 0 AND cte.Depth < @Depth
            )
            SELECT DISTINCT n.NodeId, n.NodeTypeId, n.FullyQualifiedName, n.DisplayName,
                   n.SourceLocation, n.LayerName, n.Metadata
            FROM cte
            INNER JOIN lineage_v2.Node n ON n.NodeId = cte.NodeId;

            ;WITH cte AS (
                SELECT NodeId, 0 AS Depth FROM lineage_v2.Node WHERE NodeId = @NodeId AND IsDeleted = 0
                UNION ALL
                SELECT {nextNode}, cte.Depth + 1
                FROM lineage_v2.Edge e
                INNER JOIN cte ON {direction}
                WHERE e.IsDeleted = 0 AND cte.Depth < @Depth
            )
            SELECT DISTINCT e.EdgeId, e.SourceNodeId, e.TargetNodeId, e.EdgeType,
                   e.MechanismNodeId, e.TransformExpression
            FROM cte
            INNER JOIN lineage_v2.Edge e ON ({(upstream ? "e.TargetNodeId" : "e.SourceNodeId")}) = cte.NodeId
            WHERE e.IsDeleted = 0;";

        var graph = new LineageGraph();

        using var multi = await conn.QueryMultipleAsync(sql, new { NodeId = nodeId, Depth = depth });

        var nodes = await multi.ReadAsync<dynamic>();
        foreach (var n in nodes)
        {
            var node = new LineageNode
            {
                Id = (int)n.NodeId,
                NodeTypeId = (int)n.NodeTypeId,
                FullyQualifiedName = (string)n.FullyQualifiedName,
                DisplayName = (string)n.DisplayName,
                SourceLocation = (string?)n.SourceLocation,
                LayerName = (string?)n.LayerName
            };
            graph.AddNode(node);
        }

        var edges = await multi.ReadAsync<dynamic>();
        foreach (var e in edges)
        {
            graph.AddEdge(new LineageEdge
            {
                Id = (int)e.EdgeId,
                SourceNodeId = (int)e.SourceNodeId,
                TargetNodeId = (int)e.TargetNodeId,
                EdgeType = (string)e.EdgeType,
                MechanismNodeId = (int?)e.MechanismNodeId,
                TransformExpression = (string?)e.TransformExpression
            });
        }

        return graph;
    }

    public async Task<IEnumerable<LineageNode>> SearchNodesAsync(string query, int limit = 50, CancellationToken ct = default)
    {
        await using var conn = new SqlConnection(_connectionString);
        await conn.OpenAsync(ct);

        var results = await conn.QueryAsync<dynamic>(@"
            SELECT TOP (@Limit) NodeId, NodeTypeId, FullyQualifiedName, DisplayName,
                   SourceLocation, LayerName
            FROM lineage_v2.Node
            WHERE IsDeleted = 0
              AND (FullyQualifiedName LIKE '%' + @Query + '%'
                   OR DisplayName LIKE '%' + @Query + '%')
            ORDER BY
                CASE WHEN DisplayName LIKE @Query + '%' THEN 0 ELSE 1 END,
                DisplayName",
            new { Query = query, Limit = limit });

        return results.Select(n => new LineageNode
        {
            Id = (int)n.NodeId,
            NodeTypeId = (int)n.NodeTypeId,
            FullyQualifiedName = (string)n.FullyQualifiedName,
            DisplayName = (string)n.DisplayName,
            SourceLocation = (string?)n.SourceLocation,
            LayerName = (string?)n.LayerName
        });
    }

    public async Task<LineageNode?> GetNodeAsync(int nodeId, CancellationToken ct = default)
    {
        await using var conn = new SqlConnection(_connectionString);
        await conn.OpenAsync(ct);

        var n = await conn.QueryFirstOrDefaultAsync<dynamic>(@"
            SELECT NodeId, NodeTypeId, FullyQualifiedName, DisplayName,
                   SourceLocation, LayerName, Metadata
            FROM lineage_v2.Node
            WHERE NodeId = @NodeId AND IsDeleted = 0",
            new { NodeId = nodeId });

        if (n == null) return null;

        return new LineageNode
        {
            Id = (int)n.NodeId,
            NodeTypeId = (int)n.NodeTypeId,
            FullyQualifiedName = (string)n.FullyQualifiedName,
            DisplayName = (string)n.DisplayName,
            SourceLocation = (string?)n.SourceLocation,
            LayerName = (string?)n.LayerName,
            Metadata = n.Metadata != null
                ? JsonSerializer.Deserialize<Dictionary<string, string>>((string)n.Metadata) ?? new()
                : new()
        };
    }
}
