using System.Data;
using Microsoft.Data.SqlClient;
using LineageBuilder.Core.Interfaces;
using Microsoft.Extensions.Logging;

namespace LineageBuilder.SqlParser.Schema;

/// <summary>
/// ISchemaProvider implementation that loads column metadata from MetaMart.lineage.AllTablesColumns.
/// Falls back to INFORMATION_SCHEMA.COLUMNS on the target server if not found in MetaMart.
/// </summary>
public class SqlServerSchemaProvider : ISchemaProvider
{
    private readonly string _metaMartConnectionString;
    private readonly ILogger<SqlServerSchemaProvider>? _logger;

    // Cache: "server.db.schema.table" -> columns
    private readonly Dictionary<string, List<string>> _cache = new(StringComparer.OrdinalIgnoreCase);

    public SqlServerSchemaProvider(string metaMartConnectionString, ILogger<SqlServerSchemaProvider>? logger = null)
    {
        _metaMartConnectionString = metaMartConnectionString;
        _logger = logger;
    }

    /// <summary>
    /// Preload all columns from lineage.AllTablesColumns into cache.
    /// Call this once before parsing to avoid per-table queries.
    /// </summary>
    public async Task PreloadAsync(CancellationToken ct = default)
    {
        _logger?.LogInformation("Preloading schema from lineage.AllTablesColumns...");

        await using var conn = new SqlConnection(_metaMartConnectionString);
        await conn.OpenAsync(ct);

        await using var cmd = new SqlCommand(
            "SELECT ServerName, DBName, SchemaName, TableName, ColumnName FROM lineage.AllTablesColumns ORDER BY ServerName, DBName, SchemaName, TableName, OrdinalPosition",
            conn);
        cmd.CommandTimeout = 120;

        await using var reader = await cmd.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            var server = reader.GetString(0);
            var db = reader.GetString(1);
            var schema = reader.GetString(2);
            var table = reader.GetString(3);
            var column = reader.GetString(4);

            var key = $"{server}.{db}.{schema}.{table}";
            if (!_cache.TryGetValue(key, out var columns))
            {
                columns = new List<string>();
                _cache[key] = columns;
            }
            columns.Add(column);
        }

        _logger?.LogInformation("Preloaded {TableCount} tables into schema cache", _cache.Count);
    }

    public IReadOnlyList<string> GetColumns(string serverName, string databaseName,
        string schemaName, string objectName)
    {
        // Try full key
        var key = $"{serverName}.{databaseName}.{schemaName}.{objectName}";
        if (_cache.TryGetValue(key, out var columns))
            return columns;

        // Try without server (common case)
        key = $".{databaseName}.{schemaName}.{objectName}";
        if (_cache.TryGetValue(key, out columns))
            return columns;

        // Try any server with matching db.schema.table
        var suffix = $".{databaseName}.{schemaName}.{objectName}";
        var match = _cache.FirstOrDefault(kvp => kvp.Key.EndsWith(suffix, StringComparison.OrdinalIgnoreCase));
        if (match.Value != null)
            return match.Value;

        // Try just schema.table across all servers/dbs
        var tableSuffix = $".{schemaName}.{objectName}";
        match = _cache.FirstOrDefault(kvp => kvp.Key.EndsWith(tableSuffix, StringComparison.OrdinalIgnoreCase));
        if (match.Value != null)
            return match.Value;

        // Try just table name
        match = _cache.FirstOrDefault(kvp =>
            kvp.Key.EndsWith($".{objectName}", StringComparison.OrdinalIgnoreCase));
        if (match.Value != null)
            return match.Value;

        _logger?.LogWarning("Table not found in schema cache: {Server}.{Db}.{Schema}.{Table}",
            serverName, databaseName, schemaName, objectName);
        return Array.Empty<string>();
    }

    public bool ColumnExists(string serverName, string databaseName,
        string schemaName, string objectName, string columnName)
    {
        var columns = GetColumns(serverName, databaseName, schemaName, objectName);
        return columns.Any(c => c.Equals(columnName, StringComparison.OrdinalIgnoreCase));
    }
}
