using LineageBuilder.Core.Model;

namespace LineageBuilder.Core.Interfaces;

/// <summary>
/// Репозиторий для сохранения/чтения графа lineage в SQL Server.
/// </summary>
public interface ILineageRepository
{
    Task<int> StartRunAsync(CancellationToken ct = default);
    Task CompleteRunAsync(int runId, int nodesCreated, int nodesUpdated, int edgesCreated, int edgesUpdated, CancellationToken ct = default);
    Task FailRunAsync(int runId, string error, CancellationToken ct = default);

    Task MergeNodesAsync(IEnumerable<LineageNode> nodes, int runId, CancellationToken ct = default);
    Task MergeEdgesAsync(IEnumerable<LineageEdge> edges, int runId, CancellationToken ct = default);
    Task MarkDeletedAsync(int runId, CancellationToken ct = default);

    Task<LineageGraph> GetUpstreamGraphAsync(int nodeId, int depth = 10, CancellationToken ct = default);
    Task<LineageGraph> GetDownstreamGraphAsync(int nodeId, int depth = 10, CancellationToken ct = default);
    Task<IEnumerable<LineageNode>> SearchNodesAsync(string query, int limit = 50, CancellationToken ct = default);
    Task<LineageNode?> GetNodeAsync(int nodeId, CancellationToken ct = default);
}
