using LineageBuilder.Core.Model;

namespace LineageBuilder.Core.Interfaces;

/// <summary>
/// Парсер T-SQL для извлечения column-level lineage.
/// </summary>
public interface ISqlLineageParser
{
    /// <summary>
    /// Распарсить SQL-запрос и вернуть найденные связи.
    /// </summary>
    /// <param name="sql">Текст SQL (view definition, procedure body, query).</param>
    /// <param name="contextDatabase">Имя БД по умолчанию.</param>
    /// <param name="contextSchema">Имя схемы по умолчанию.</param>
    /// <returns>Результат парсинга с найденными связями.</returns>
    SqlLineageResult Parse(string sql, string? contextDatabase = null, string? contextSchema = null);
}

/// <summary>
/// Результат парсинга SQL.
/// </summary>
public class SqlLineageResult
{
    /// <summary>Найденные связи column-level.</summary>
    public List<ColumnLineageEntry> Entries { get; set; } = new();

    /// <summary>Ошибки парсинга.</summary>
    public List<string> Errors { get; set; } = new();

    /// <summary>Предупреждения (SELECT *, dynamic SQL и т.д.).</summary>
    public List<string> Warnings { get; set; } = new();

    public bool HasErrors => Errors.Count > 0;
}

/// <summary>
/// Одна запись column-level lineage: откуда → куда.
/// </summary>
public class ColumnLineageEntry
{
    /// <summary>Исходная таблица (FQN).</summary>
    public string SourceTable { get; set; } = string.Empty;

    /// <summary>Исходная колонка.</summary>
    public string SourceColumn { get; set; } = string.Empty;

    /// <summary>Целевая таблица (FQN) — для INSERT, VIEW, или пустая для SELECT.</summary>
    public string TargetTable { get; set; } = string.Empty;

    /// <summary>Целевая колонка (или алиас в SELECT).</summary>
    public string TargetColumn { get; set; } = string.Empty;

    /// <summary>Тип связи.</summary>
    public string EdgeType { get; set; } = EdgeTypes.DataFlow;

    /// <summary>Выражение трансформации (если есть).</summary>
    public string? TransformExpression { get; set; }

    public override string ToString() =>
        $"{SourceTable}.{SourceColumn} --> {TargetTable}.{TargetColumn} [{EdgeType}]";
}
