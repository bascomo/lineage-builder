namespace LineageBuilder.Core.Model;

/// <summary>
/// Узел графа lineage.
/// </summary>
public class LineageNode
{
    public int Id { get; set; }
    public NodeType NodeType { get; set; }
    public string FullyQualifiedName { get; set; } = string.Empty;
    public string DisplayName { get; set; } = string.Empty;
    public string? SourceLocation { get; set; }
    public LayerName? Layer { get; set; }
    public Dictionary<string, string> Metadata { get; set; } = new();
    public int? ParentNodeId { get; set; }

    public override string ToString() => $"[{NodeType}] {FullyQualifiedName}";

    public override bool Equals(object? obj) =>
        obj is LineageNode other && FullyQualifiedName == other.FullyQualifiedName;

    public override int GetHashCode() => FullyQualifiedName.GetHashCode();
}
