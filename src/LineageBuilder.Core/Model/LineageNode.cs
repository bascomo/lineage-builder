namespace LineageBuilder.Core.Model;

/// <summary>
/// Узел графа lineage.
/// </summary>
public class LineageNode
{
    public int Id { get; set; }

    /// <summary>FK на lineage_v2.NodeType.</summary>
    public int NodeTypeId { get; set; }

    /// <summary>Имя типа (для удобства, не хранится — разрешается через справочник).</summary>
    public string NodeTypeName { get; set; } = string.Empty;

    public string FullyQualifiedName { get; set; } = string.Empty;
    public string DisplayName { get; set; } = string.Empty;
    public string? SourceLocation { get; set; }
    public string? LayerName { get; set; }
    public string? Description { get; set; }
    public Dictionary<string, string> Metadata { get; set; } = new();

    public override string ToString() => $"[{NodeTypeName}] {FullyQualifiedName}";

    public override bool Equals(object? obj) =>
        obj is LineageNode other && FullyQualifiedName.Equals(other.FullyQualifiedName, StringComparison.OrdinalIgnoreCase);

    public override int GetHashCode() => FullyQualifiedName.ToUpperInvariant().GetHashCode();
}
