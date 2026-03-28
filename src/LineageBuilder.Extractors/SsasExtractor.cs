using Microsoft.AnalysisServices;
using LineageBuilder.Core.Interfaces;
using LineageBuilder.Core.Model;
using Microsoft.Extensions.Logging;

namespace LineageBuilder.Extractors;

/// <summary>
/// Extracts lineage from SSAS Multidimensional cubes via AMO (Analysis Management Objects).
/// Connects directly to SSAS servers, traverses: Server → Database → Cube →
/// DSV (Named Queries + Tables) → Dimensions → Attributes, MeasureGroups → Measures.
/// Named Query SQL is parsed through TsqlLineageParser for column-level lineage.
/// </summary>
public class SsasExtractor : IMetadataExtractor
{
    private readonly string[] _ssasServers;
    private readonly ISqlLineageParser _sqlParser;
    private readonly ILogger<SsasExtractor>? _logger;

    public string Name => "SSAS";

    /// <param name="ssasServers">SSAS server names, e.g. ["OLAP-VDI", "OLAP2-VDI", "OLAP3-VDI", "OPD-VDI"]</param>
    public SsasExtractor(string[] ssasServers, ISqlLineageParser sqlParser,
        ILogger<SsasExtractor>? logger = null)
    {
        _ssasServers = ssasServers;
        _sqlParser = sqlParser;
        _logger = logger;
    }

    public async Task ExtractAsync(LineageGraph graph, CancellationToken cancellationToken = default)
    {
        foreach (var serverName in _ssasServers)
        {
            if (cancellationToken.IsCancellationRequested) break;
            try
            {
                _logger?.LogInformation("Connecting to SSAS server {Server}...", serverName);
                ExtractFromServer(serverName, graph, cancellationToken);
            }
            catch (Exception ex)
            {
                _logger?.LogError(ex, "Failed to extract from SSAS server {Server}", serverName);
            }
        }

        _logger?.LogInformation("SSAS extraction complete: {Nodes} nodes, {Edges} edges",
            graph.Nodes.Count, graph.Edges.Count);
        await Task.CompletedTask;
    }

    private void ExtractFromServer(string serverName, LineageGraph graph, CancellationToken ct)
    {
        using var server = new Server();
        server.Connect($"Data Source={serverName};");

        // Server node
        var serverNode = graph.AddNode(new LineageNode
        {
            NodeTypeId = WellKnownNodeTypes.SsasServer,
            NodeTypeName = "SsasServer",
            FullyQualifiedName = serverName,
            DisplayName = serverName,
            LayerName = Layers.Cube
        });

        foreach (Database db in server.Databases)
        {
            if (ct.IsCancellationRequested) break;

            var dbFqn = $"{serverName}.{db.ID}";
            var dbNode = graph.AddNode(new LineageNode
            {
                NodeTypeId = WellKnownNodeTypes.SsasDatabase,
                NodeTypeName = "SsasDatabase",
                FullyQualifiedName = dbFqn,
                DisplayName = db.Name,
                LayerName = Layers.Cube
            });

            // Process DSV
            foreach (DataSourceView dsv in db.DataSourceViews)
            {
                var dsvFqn = $"{dbFqn}.{dsv.ID}";
                var dsvNode = graph.AddNode(new LineageNode
                {
                    NodeTypeId = WellKnownNodeTypes.Dsv,
                    NodeTypeName = "DSV",
                    FullyQualifiedName = dsvFqn,
                    DisplayName = dsv.Name,
                    LayerName = Layers.Cube
                });

                // Extract connection string info for SQL context
                string? dbConnString = null;
                if (db.DataSources.Count > 0)
                    dbConnString = db.DataSources[0].ConnectionString;

                var (connDb, connServer) = ParseConnectionString(dbConnString);

                ProcessDsvTables(dsv, dsvFqn, connDb, connServer, graph);
            }

            // Process Cubes
            foreach (Cube cube in db.Cubes)
            {
                if (ct.IsCancellationRequested) break;
                ProcessCube(cube, dbFqn, db, graph);
            }
        }

        server.Disconnect();
        _logger?.LogInformation("Server {Server}: extracted {Dbs} databases", serverName, server.Databases.Count);
    }

    private void ProcessDsvTables(DataSourceView dsv, string dsvFqn,
        string? connDb, string? connServer, LineageGraph graph)
    {
        if (dsv.Schema?.Tables == null) return;

        foreach (System.Data.DataTable table in dsv.Schema.Tables)
        {
            var tableName = table.TableName;
            var isNamedQuery = table.ExtendedProperties.ContainsKey("QueryDefinition");
            var queryText = isNamedQuery
                ? table.ExtendedProperties["QueryDefinition"]?.ToString()
                : null;
            var dbTableName = table.ExtendedProperties.ContainsKey("DbTableName")
                ? table.ExtendedProperties["DbTableName"]?.ToString()
                : tableName;
            var dbSchemaName = table.ExtendedProperties.ContainsKey("DbSchemaName")
                ? table.ExtendedProperties["DbSchemaName"]?.ToString()
                : "dbo";

            var nodeTypeId = isNamedQuery ? WellKnownNodeTypes.DsvNamedQuery : WellKnownNodeTypes.DsvTable;
            var nodeTypeName = isNamedQuery ? "DsvNamedQuery" : "DsvTable";

            var tableFqn = $"{dsvFqn}.{tableName}";
            var dsvTableNode = graph.AddNode(new LineageNode
            {
                NodeTypeId = nodeTypeId,
                NodeTypeName = nodeTypeName,
                FullyQualifiedName = tableFqn,
                DisplayName = tableName,
                LayerName = Layers.Cube,
                Metadata = new Dictionary<string, string>
                {
                    ["dbTableName"] = dbTableName ?? "",
                    ["dbSchemaName"] = dbSchemaName ?? "",
                    ["isNamedQuery"] = isNamedQuery.ToString()
                }
            });

            // Add columns from DSV schema
            foreach (System.Data.DataColumn col in table.Columns)
            {
                var fieldTypeId = isNamedQuery ? WellKnownNodeTypes.DsvNqField : WellKnownNodeTypes.DsvTableField;
                var fieldTypeName = isNamedQuery ? "DsvNqField" : "DsvTableField";
                var colFqn = $"{tableFqn}.{col.ColumnName}";

                var dsvColNode = graph.AddNode(new LineageNode
                {
                    NodeTypeId = fieldTypeId,
                    NodeTypeName = fieldTypeName,
                    FullyQualifiedName = colFqn,
                    DisplayName = col.ColumnName,
                    LayerName = Layers.Cube
                });

                // If NOT a named query, link DSV field → physical table column
                if (!isNamedQuery && !string.IsNullOrEmpty(connDb))
                {
                    var physColFqn = $"{connServer ?? "DWH-VDI"}.{connDb}.{dbSchemaName}.{dbTableName}.{col.ColumnName}";
                    var physColNode = graph.FindNode(physColFqn);
                    if (physColNode != null)
                    {
                        graph.AddEdge(new LineageEdge
                        {
                            SourceNodeId = physColNode.Id,
                            TargetNodeId = dsvColNode.Id,
                            EdgeType = EdgeTypes.DataFlow,
                            MechanismNodeId = dsvTableNode.Id
                        });
                    }
                }
            }

            // If named query, parse SQL for column-level lineage
            if (isNamedQuery && !string.IsNullOrEmpty(queryText))
            {
                ParseNamedQueryLineage(queryText, tableFqn, dsvTableNode, connDb, connServer, graph);
            }
        }
    }

    private void ParseNamedQueryLineage(string sql, string dsvTableFqn,
        LineageNode dsvTableNode, string? connDb, string? connServer, LineageGraph graph)
    {
        try
        {
            var result = _sqlParser.Parse(sql, connDb, "dbo");

            foreach (var entry in result.Entries)
            {
                // Qualify source table
                var sourceTable = QualifyName(entry.SourceTable, connServer ?? "DWH-VDI", connDb ?? "", "dbo");
                var sourceColFqn = $"{sourceTable}.{entry.SourceColumn}";
                var targetColFqn = $"{dsvTableFqn}.{entry.TargetColumn}";

                var sourceNode = graph.FindNode(sourceColFqn);
                var targetNode = graph.FindNode(targetColFqn);

                if (sourceNode != null && targetNode != null)
                {
                    graph.AddEdge(new LineageEdge
                    {
                        SourceNodeId = sourceNode.Id,
                        TargetNodeId = targetNode.Id,
                        EdgeType = entry.EdgeType,
                        MechanismNodeId = dsvTableNode.Id,
                        TransformExpression = entry.TransformExpression
                    });
                }
            }
        }
        catch (Exception ex)
        {
            _logger?.LogWarning("Failed to parse Named Query SQL for {Table}: {Error}",
                dsvTableFqn, ex.Message);
        }
    }

    private void ProcessCube(Cube cube, string dbFqn, Database db, LineageGraph graph)
    {
        var cubeFqn = $"{dbFqn}.{cube.ID}";
        var cubeNode = graph.AddNode(new LineageNode
        {
            NodeTypeId = WellKnownNodeTypes.Cube,
            NodeTypeName = "Cube",
            FullyQualifiedName = cubeFqn,
            DisplayName = cube.Name,
            LayerName = Layers.Cube
        });

        // Process MeasureGroups → Measures
        foreach (MeasureGroup mg in cube.MeasureGroups)
        {
            var mgFqn = $"{cubeFqn}.{mg.ID}";
            var mgNode = graph.AddNode(new LineageNode
            {
                NodeTypeId = WellKnownNodeTypes.MeasureGroup,
                NodeTypeName = "MeasureGroup",
                FullyQualifiedName = mgFqn,
                DisplayName = mg.Name,
                LayerName = Layers.Cube
            });

            foreach (Measure measure in mg.Measures)
            {
                var measureFqn = $"{mgFqn}.{measure.ID}";
                var measureNode = graph.AddNode(new LineageNode
                {
                    NodeTypeId = WellKnownNodeTypes.Measure,
                    NodeTypeName = "Measure",
                    FullyQualifiedName = measureFqn,
                    DisplayName = measure.Name,
                    LayerName = Layers.Cube,
                    Metadata = new Dictionary<string, string>
                    {
                        ["aggregateFunction"] = measure.AggregateFunction.ToString()
                    }
                });

                // Link measure → DSV column via source column binding
                if (measure.Source?.Source is ColumnBinding colBinding)
                {
                    var dsvTableId = colBinding.TableID;
                    var dsvColName = colBinding.ColumnID;

                    // Find DSV table in this database's DSVs
                    foreach (DataSourceView dsv in db.DataSourceViews)
                    {
                        var dsvColFqn = $"{dbFqn}.{dsv.ID}.{dsvTableId}.{dsvColName}";
                        var dsvColNode = graph.FindNode(dsvColFqn);
                        if (dsvColNode != null)
                        {
                            graph.AddEdge(new LineageEdge
                            {
                                SourceNodeId = dsvColNode.Id,
                                TargetNodeId = measureNode.Id,
                                EdgeType = measure.AggregateFunction == AggregationFunction.Count
                                    ? EdgeTypes.Aggregation : EdgeTypes.DataFlow,
                                MechanismNodeId = cubeNode.Id,
                                TransformExpression = $"{measure.AggregateFunction}({dsvColName})"
                            });
                            break;
                        }
                    }
                }
            }
        }

        // Process Dimensions → Attributes
        foreach (CubeDimension cubeDim in cube.Dimensions)
        {
            var dim = cubeDim.Dimension;
            if (dim == null) continue;

            var dimFqn = $"{cubeFqn}.{dim.ID}";
            var dimNode = graph.AddNode(new LineageNode
            {
                NodeTypeId = WellKnownNodeTypes.Dimension,
                NodeTypeName = "Dimension",
                FullyQualifiedName = dimFqn,
                DisplayName = dim.Name,
                LayerName = Layers.Cube
            });

            foreach (DimensionAttribute attr in dim.Attributes)
            {
                var attrFqn = $"{dimFqn}.{attr.ID}";
                var attrNode = graph.AddNode(new LineageNode
                {
                    NodeTypeId = WellKnownNodeTypes.DimensionAttribute,
                    NodeTypeName = "DimensionAttribute",
                    FullyQualifiedName = attrFqn,
                    DisplayName = attr.Name,
                    LayerName = Layers.Cube
                });

                // Link attribute → DSV column via key columns
                foreach (DataItem keyCol in attr.KeyColumns)
                {
                    if (keyCol.Source is ColumnBinding kb)
                    {
                        foreach (DataSourceView dsv in db.DataSourceViews)
                        {
                            var dsvColFqn = $"{dbFqn}.{dsv.ID}.{kb.TableID}.{kb.ColumnID}";
                            var dsvColNode = graph.FindNode(dsvColFqn);
                            if (dsvColNode != null)
                            {
                                graph.AddEdge(new LineageEdge
                                {
                                    SourceNodeId = dsvColNode.Id,
                                    TargetNodeId = attrNode.Id,
                                    EdgeType = EdgeTypes.DataFlow,
                                    MechanismNodeId = cubeNode.Id
                                });
                                break;
                            }
                        }
                    }
                }

                // Link attribute name column if different
                if (attr.NameColumn?.Source is ColumnBinding nb)
                {
                    foreach (DataSourceView dsv in db.DataSourceViews)
                    {
                        var dsvColFqn = $"{dbFqn}.{dsv.ID}.{nb.TableID}.{nb.ColumnID}";
                        var dsvColNode = graph.FindNode(dsvColFqn);
                        if (dsvColNode != null)
                        {
                            graph.AddEdge(new LineageEdge
                            {
                                SourceNodeId = dsvColNode.Id,
                                TargetNodeId = attrNode.Id,
                                EdgeType = EdgeTypes.DataFlow,
                                MechanismNodeId = cubeNode.Id,
                                TransformExpression = "NameColumn"
                            });
                            break;
                        }
                    }
                }
            }
        }
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

    private static (string? db, string? server) ParseConnectionString(string? connString)
    {
        if (string.IsNullOrEmpty(connString)) return (null, null);

        string? db = null, server = null;
        foreach (var part in connString.Split(';'))
        {
            var kv = part.Split('=', 2);
            if (kv.Length != 2) continue;
            var key = kv[0].Trim().ToUpperInvariant();
            var val = kv[1].Trim();
            if (key is "INITIAL CATALOG" or "DATABASE") db = val;
            if (key is "DATA SOURCE" or "SERVER") server = val;
        }
        return (db, server);
    }
}
