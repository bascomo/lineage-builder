namespace LineageBuilder.Core.Model;

/// <summary>
/// In-memory граф lineage с узлами и рёбрами.
/// </summary>
public class LineageGraph
{
    private readonly Dictionary<string, LineageNode> _nodesByFqn = new(StringComparer.OrdinalIgnoreCase);
    private readonly Dictionary<int, LineageNode> _nodesById = new();
    private readonly List<LineageEdge> _edges = new();
    private readonly Dictionary<int, List<LineageEdge>> _outgoing = new(); // sourceNodeId -> edges
    private readonly Dictionary<int, List<LineageEdge>> _incoming = new(); // targetNodeId -> edges
    private int _nextNodeId = 1;
    private int _nextEdgeId = 1;

    public IReadOnlyCollection<LineageNode> Nodes => _nodesById.Values;
    public IReadOnlyList<LineageEdge> Edges => _edges;

    /// <summary>
    /// Добавить узел. Если узел с таким FQN уже существует — возвращает существующий.
    /// </summary>
    public LineageNode AddNode(LineageNode node)
    {
        if (_nodesByFqn.TryGetValue(node.FullyQualifiedName, out var existing))
            return existing;

        node.Id = _nextNodeId++;
        _nodesByFqn[node.FullyQualifiedName] = node;
        _nodesById[node.Id] = node;
        return node;
    }

    /// <summary>
    /// Найти узел по FQN.
    /// </summary>
    public LineageNode? FindNode(string fullyQualifiedName) =>
        _nodesByFqn.GetValueOrDefault(fullyQualifiedName);

    /// <summary>
    /// Найти узел по Id.
    /// </summary>
    public LineageNode? GetNode(int nodeId) =>
        _nodesById.GetValueOrDefault(nodeId);

    /// <summary>
    /// Добавить ребро.
    /// </summary>
    public LineageEdge AddEdge(LineageEdge edge)
    {
        edge.Id = _nextEdgeId++;
        _edges.Add(edge);

        if (!_outgoing.TryGetValue(edge.SourceNodeId, out var outList))
        {
            outList = new List<LineageEdge>();
            _outgoing[edge.SourceNodeId] = outList;
        }
        outList.Add(edge);

        if (!_incoming.TryGetValue(edge.TargetNodeId, out var inList))
        {
            inList = new List<LineageEdge>();
            _incoming[edge.TargetNodeId] = inList;
        }
        inList.Add(edge);

        return edge;
    }

    /// <summary>
    /// Добавить ребро по FQN источника и назначения.
    /// </summary>
    public LineageEdge? AddEdge(string sourceFqn, string targetFqn, string edgeType,
        int? mechanismNodeId = null, string? transformExpression = null)
    {
        var source = FindNode(sourceFqn);
        var target = FindNode(targetFqn);
        if (source == null || target == null) return null;

        return AddEdge(new LineageEdge
        {
            SourceNodeId = source.Id,
            TargetNodeId = target.Id,
            EdgeType = edgeType,
            MechanismNodeId = mechanismNodeId,
            TransformExpression = transformExpression
        });
    }

    /// <summary>
    /// Получить все upstream-зависимости (откуда данные приходят в этот узел).
    /// </summary>
    public LineageGraph GetUpstream(int nodeId, int maxDepth = 10)
    {
        var subgraph = new LineageGraph();
        var visited = new HashSet<int>();
        TraverseUpstream(nodeId, maxDepth, 0, subgraph, visited);
        return subgraph;
    }

    /// <summary>
    /// Получить все downstream-зависимости (куда данные уходят из этого узла).
    /// </summary>
    public LineageGraph GetDownstream(int nodeId, int maxDepth = 10)
    {
        var subgraph = new LineageGraph();
        var visited = new HashSet<int>();
        TraverseDownstream(nodeId, maxDepth, 0, subgraph, visited);
        return subgraph;
    }

    /// <summary>
    /// Поиск узлов по подстроке в имени.
    /// </summary>
    public IEnumerable<LineageNode> Search(string query) =>
        _nodesByFqn.Values.Where(n =>
            n.FullyQualifiedName.Contains(query, StringComparison.OrdinalIgnoreCase) ||
            n.DisplayName.Contains(query, StringComparison.OrdinalIgnoreCase));

    private void TraverseUpstream(int nodeId, int maxDepth, int currentDepth,
        LineageGraph subgraph, HashSet<int> visited)
    {
        if (currentDepth > maxDepth || !visited.Add(nodeId)) return;

        var node = GetNode(nodeId);
        if (node == null) return;
        subgraph.AddNode(node);

        if (!_incoming.TryGetValue(nodeId, out var edges)) return;
        foreach (var edge in edges)
        {
            subgraph.AddEdge(edge);
            TraverseUpstream(edge.SourceNodeId, maxDepth, currentDepth + 1, subgraph, visited);
        }
    }

    private void TraverseDownstream(int nodeId, int maxDepth, int currentDepth,
        LineageGraph subgraph, HashSet<int> visited)
    {
        if (currentDepth > maxDepth || !visited.Add(nodeId)) return;

        var node = GetNode(nodeId);
        if (node == null) return;
        subgraph.AddNode(node);

        if (!_outgoing.TryGetValue(nodeId, out var edges)) return;
        foreach (var edge in edges)
        {
            subgraph.AddEdge(edge);
            TraverseDownstream(edge.TargetNodeId, maxDepth, currentDepth + 1, subgraph, visited);
        }
    }
}
