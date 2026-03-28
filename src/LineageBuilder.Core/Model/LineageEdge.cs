namespace LineageBuilder.Core.Model;

/// <summary>
/// Ребро графа lineage — связь между двумя узлами.
/// </summary>
public class LineageEdge
{
    public int Id { get; set; }
    public int SourceNodeId { get; set; }
    public int TargetNodeId { get; set; }

    /// <summary>Тип связи: DataFlow, Transform, Aggregation, Filter, Join, DirectCopy, ProcessExecution, Lookup.</summary>
    public string EdgeType { get; set; } = "DataFlow";

    /// <summary>FK на Node — узел-механизм, создающий эту связь (view, SP, SSIS-пакет).</summary>
    public int? MechanismNodeId { get; set; }

    /// <summary>Выражение трансформации (SUM(Amount), 1:1 copy, etc.).</summary>
    public string? TransformExpression { get; set; }

    public override string ToString() =>
        $"{SourceNodeId} --[{EdgeType}]--> {TargetNodeId}";
}
