using Dapper;
using Microsoft.Data.SqlClient;
using LineageBuilder.Core.Interfaces;
using LineageBuilder.Core.Model;
using Microsoft.Extensions.Logging;

namespace LineageBuilder.Extractors;

/// <summary>
/// Extracts lineage from SQL Server views, stored procedures, and functions.
/// Reads object definitions from lineage.AllObjectsDefinition and
/// parses them through TsqlLineageParser to extract column-level lineage.
/// </summary>
public class SqlServerExtractor : IMetadataExtractor
{
    private readonly string _metaMartConnectionString;
    private readonly ISqlLineageParser _sqlParser;
    private readonly ILogger<SqlServerExtractor>? _logger;

    public string Name => "SqlServer";

    public SqlServerExtractor(string metaMartConnectionString, ISqlLineageParser sqlParser,
        ILogger<SqlServerExtractor>? logger = null)
    {
        _metaMartConnectionString = metaMartConnectionString;
        _sqlParser = sqlParser;
        _logger = logger;
    }

    public async Task ExtractAsync(LineageGraph graph, CancellationToken cancellationToken = default)
    {
        _logger?.LogInformation("Starting SQL Server lineage extraction...");

        await using var conn = new SqlConnection(_metaMartConnectionString);
        await conn.OpenAsync(cancellationToken);

        // 1. Build hierarchy: Server → DB → Schema → Table → Column from AllTablesColumns
        await BuildTableHierarchyAsync(conn, graph, cancellationToken);

        // 2. Get all views and procedures from AllObjectsDefinition
        var objects = await conn.QueryAsync<SqlObjectDef>(@"
            SELECT ServerName, DBName, SchemaName, ObjectName, ObjectType, ObjectDefinition
            FROM lineage.AllObjectsDefinition
            WHERE ObjectType IN ('V', 'P', 'FN', 'IF', 'TF')
              AND ObjectDefinition IS NOT NULL
            ORDER BY ServerName, DBName, SchemaName, ObjectName");

        var objectList = objects.ToList();
        _logger?.LogInformation("Found {Count} SQL objects to parse", objectList.Count);

        int parsed = 0, errors = 0;
        foreach (var obj in objectList)
        {
            if (cancellationToken.IsCancellationRequested) break;

            var objectFqn = $"{obj.ServerName}.{obj.DBName}.{obj.SchemaName}.{obj.ObjectName}";
            var nodeType = obj.ObjectType switch
            {
                "V" => NodeType.View,
                "P" => NodeType.StoredProcedure,
                "FN" or "IF" or "TF" => NodeType.TableFunction,
                _ => NodeType.View
            };
            var mechanismType = obj.ObjectType switch
            {
                "V" => MechanismType.View,
                "P" => MechanismType.StoredProcedure,
                _ => MechanismType.TableFunction
            };

            // Add the object node itself
            graph.AddNode(new LineageNode
            {
                NodeType = nodeType,
                FullyQualifiedName = objectFqn,
                DisplayName = $"{obj.SchemaName}.{obj.ObjectName}",
                SourceLocation = objectFqn,
                Layer = DetectLayer(obj.DBName, obj.SchemaName),
                Metadata = new Dictionary<string, string>
                {
                    ["server"] = obj.ServerName,
                    ["database"] = obj.DBName,
                    ["schema"] = obj.SchemaName,
                    ["objectType"] = obj.ObjectType
                }
            });

            // Parse the SQL definition
            try
            {
                var result = _sqlParser.Parse(obj.ObjectDefinition, obj.DBName, obj.SchemaName);

                foreach (var entry in result.Entries)
                {
                    // Qualify source/target table names if not fully qualified
                    var sourceTable = QualifyName(entry.SourceTable, obj.ServerName, obj.DBName, obj.SchemaName);
                    var targetTable = string.IsNullOrEmpty(entry.TargetTable)
                        ? objectFqn
                        : QualifyName(entry.TargetTable, obj.ServerName, obj.DBName, obj.SchemaName);

                    var sourceColFqn = $"{sourceTable}.{entry.SourceColumn}";
                    var targetColFqn = $"{targetTable}.{entry.TargetColumn}";

                    // Ensure source/target column nodes exist
                    var sourceNode = graph.AddNode(new LineageNode
                    {
                        NodeType = NodeType.Column,
                        FullyQualifiedName = sourceColFqn,
                        DisplayName = entry.SourceColumn,
                        Layer = DetectLayer(sourceTable)
                    });

                    var targetNode = graph.AddNode(new LineageNode
                    {
                        NodeType = NodeType.Column,
                        FullyQualifiedName = targetColFqn,
                        DisplayName = entry.TargetColumn,
                        Layer = DetectLayer(targetTable)
                    });

                    graph.AddEdge(new LineageEdge
                    {
                        SourceNodeId = sourceNode.Id,
                        TargetNodeId = targetNode.Id,
                        EdgeType = entry.EdgeType,
                        MechanismType = mechanismType,
                        MechanismLocation = objectFqn,
                        TransformExpression = entry.TransformExpression
                    });
                }

                parsed++;
            }
            catch (Exception ex)
            {
                errors++;
                _logger?.LogWarning("Failed to parse {Object}: {Error}", objectFqn, ex.Message);
            }

            if (parsed % 100 == 0 && parsed > 0)
                _logger?.LogInformation("Parsed {Parsed}/{Total} objects ({Errors} errors)",
                    parsed, objectList.Count, errors);
        }

        _logger?.LogInformation("SQL Server extraction complete: parsed {Parsed}, errors {Errors}, graph has {Nodes} nodes, {Edges} edges",
            parsed, errors, graph.Nodes.Count, graph.Edges.Count);
    }

    private async Task BuildTableHierarchyAsync(SqlConnection conn, LineageGraph graph, CancellationToken ct)
    {
        _logger?.LogInformation("Building table hierarchy from AllTablesColumns...");

        var tables = await conn.QueryAsync<dynamic>(@"
            SELECT DISTINCT ServerName, DBName, SchemaName, TableName
            FROM lineage.AllTablesColumns");

        foreach (var t in tables)
        {
            if (ct.IsCancellationRequested) break;

            string server = t.ServerName, db = t.DBName, schema = t.SchemaName, table = t.TableName;
            var tableFqn = $"{server}.{db}.{schema}.{table}";

            // Server node
            graph.AddNode(new LineageNode
            {
                NodeType = NodeType.SqlServer,
                FullyQualifiedName = server,
                DisplayName = server
            });

            // Database node
            graph.AddNode(new LineageNode
            {
                NodeType = NodeType.DatabaseRelational,
                FullyQualifiedName = $"{server}.{db}",
                DisplayName = db
            });

            // Schema node
            graph.AddNode(new LineageNode
            {
                NodeType = NodeType.Schema,
                FullyQualifiedName = $"{server}.{db}.{schema}",
                DisplayName = schema
            });

            // Table node
            graph.AddNode(new LineageNode
            {
                NodeType = NodeType.Table,
                FullyQualifiedName = tableFqn,
                DisplayName = $"{schema}.{table}",
                Layer = DetectLayer(db, schema)
            });
        }

        // Add columns
        var columns = await conn.QueryAsync<dynamic>(@"
            SELECT ServerName, DBName, SchemaName, TableName, ColumnName
            FROM lineage.AllTablesColumns
            ORDER BY ServerName, DBName, SchemaName, TableName, OrdinalPosition");

        foreach (var c in columns)
        {
            string server = c.ServerName, db = c.DBName, schema = c.SchemaName,
                   table = c.TableName, col = c.ColumnName;

            var tableFqn = $"{server}.{db}.{schema}.{table}";
            var tableNode = graph.FindNode(tableFqn);

            graph.AddNode(new LineageNode
            {
                NodeType = NodeType.Column,
                FullyQualifiedName = $"{tableFqn}.{col}",
                DisplayName = col,
                ParentNodeId = tableNode?.Id,
                Layer = DetectLayer(db, schema)
            });
        }

        _logger?.LogInformation("Built hierarchy: {Count} nodes", graph.Nodes.Count);
    }

    private static string QualifyName(string name, string defaultServer, string defaultDb, string defaultSchema)
    {
        if (string.IsNullOrEmpty(name)) return name;

        var parts = name.Replace("[", "").Replace("]", "").Split('.');
        return parts.Length switch
        {
            4 => $"{parts[0]}.{parts[1]}.{parts[2]}.{parts[3]}",
            3 => $"{defaultServer}.{parts[0]}.{parts[1]}.{parts[2]}",
            2 => $"{defaultServer}.{defaultDb}.{parts[0]}.{parts[1]}",
            1 => $"{defaultServer}.{defaultDb}.{defaultSchema}.{parts[0]}",
            _ => name
        };
    }

    private static LayerName? DetectLayer(string dbOrFqn, string? schema = null)
    {
        var db = dbOrFqn.Split('.').Last().ToUpperInvariant();
        var sch = schema?.ToUpperInvariant() ?? "";

        if (db.Contains("STAGING") || sch.StartsWith("S0")) return LayerName.Staging;
        if (db.Contains("DATAMART") || db.Contains("MART")) return LayerName.DataMart;
        if (db == "DWH" || db.Contains("CORE")) return LayerName.Core;
        return null;
    }

    private class SqlObjectDef
    {
        public string ServerName { get; set; } = "";
        public string DBName { get; set; } = "";
        public string SchemaName { get; set; } = "";
        public string ObjectName { get; set; } = "";
        public string ObjectType { get; set; } = "";
        public string ObjectDefinition { get; set; } = "";
    }
}
