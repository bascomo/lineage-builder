namespace LineageBuilder.Core.Model;

/// <summary>
/// Ребро графа lineage — связь между двумя узлами.
/// </summary>
public class LineageEdge
{
    public int Id { get; set; }
    public int SourceNodeId { get; set; }
    public int TargetNodeId { get; set; }
    public EdgeType EdgeType { get; set; }
    public MechanismType? MechanismType { get; set; }
    public string? MechanismLocation { get; set; }
    public string? TransformExpression { get; set; }

    public override string ToString() =>
        $"{SourceNodeId} --[{EdgeType}]--> {TargetNodeId}";
}
