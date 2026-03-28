using LineageBuilder.Core.Interfaces;

namespace LineageBuilder.SqlParser.Schema;

/// <summary>
/// In-memory реализация ISchemaProvider для тестирования и offline-режима.
/// </summary>
public class InMemorySchemaProvider : ISchemaProvider
{
    // Key: "server.database.schema.object" (lowercase), Value: list of column names
    private readonly Dictionary<string, List<string>> _schema = new(StringComparer.OrdinalIgnoreCase);

    /// <summary>
    /// Зарегистрировать таблицу/view с колонками.
    /// </summary>
    public void RegisterTable(string serverName, string databaseName, string schemaName,
        string objectName, IEnumerable<string> columns)
    {
        var key = MakeKey(serverName, databaseName, schemaName, objectName);
        _schema[key] = columns.ToList();
    }

    /// <summary>
    /// Зарегистрировать таблицу с упрощённым ключом (без сервера).
    /// </summary>
    public void RegisterTable(string databaseName, string schemaName,
        string objectName, IEnumerable<string> columns)
    {
        RegisterTable("", databaseName, schemaName, objectName, columns);
    }

    public IReadOnlyList<string> GetColumns(string serverName, string databaseName,
        string schemaName, string objectName)
    {
        var key = MakeKey(serverName, databaseName, schemaName, objectName);
        if (_schema.TryGetValue(key, out var columns))
            return columns;

        // Try without server
        key = MakeKey("", databaseName, schemaName, objectName);
        if (_schema.TryGetValue(key, out columns))
            return columns;

        return Array.Empty<string>();
    }

    public bool ColumnExists(string serverName, string databaseName,
        string schemaName, string objectName, string columnName)
    {
        var columns = GetColumns(serverName, databaseName, schemaName, objectName);
        return columns.Any(c => c.Equals(columnName, StringComparison.OrdinalIgnoreCase));
    }

    private static string MakeKey(string server, string db, string schema, string obj) =>
        $"{server}.{db}.{schema}.{obj}";
}
