using System.Xml.Linq;
using LineageBuilder.Core.Interfaces;
using LineageBuilder.Core.Model;
using Microsoft.Extensions.Logging;

namespace LineageBuilder.Extractors;

/// <summary>
/// Extracts lineage from SSIS packages (.dtsx files).
/// Downloads packages from TFS via TfsClient, parses XML structure.
/// Extracts: Packages → Executables → Components → Columns, Data Flow mappings.
/// SQL queries from OLE DB Source / Execute SQL Task are fed to TsqlLineageParser.
/// </summary>
public class SsisPackageExtractor : IMetadataExtractor
{
    private readonly TfsClient _tfsClient;
    private readonly string _ssisRootPath;
    private readonly ISqlLineageParser _sqlParser;
    private readonly ILogger<SsisPackageExtractor>? _logger;

    // SSIS XML namespaces
    private static readonly XNamespace DtsNs = "www.microsoft.com/SqlServer/Dts";
    private static readonly XNamespace SqlTaskNs = "www.microsoft.com/sqlserver/dts/tasks/sqltask";

    public string Name => "SSIS";

    public SsisPackageExtractor(TfsClient tfsClient, string ssisRootPath,
        ISqlLineageParser sqlParser, ILogger<SsisPackageExtractor>? logger = null)
    {
        _tfsClient = tfsClient;
        _ssisRootPath = ssisRootPath;
        _sqlParser = sqlParser;
        _logger = logger;
    }

    public async Task ExtractAsync(LineageGraph graph, CancellationToken cancellationToken = default)
    {
        _logger?.LogInformation("Searching for SSIS packages in TFS: {Path}", _ssisRootPath);

        var dtsxFiles = await _tfsClient.FindDtsxFilesAsync(_ssisRootPath, cancellationToken);
        _logger?.LogInformation("Found {Count} .dtsx files", dtsxFiles.Count);

        int parsed = 0, errors = 0;
        foreach (var file in dtsxFiles)
        {
            if (cancellationToken.IsCancellationRequested) break;

            try
            {
                var xmlContent = await _tfsClient.DownloadFileAsync(file.Path, cancellationToken);
                var packageName = Path.GetFileNameWithoutExtension(file.Path);

                ProcessPackage(xmlContent, packageName, file.Path, graph);
                parsed++;
            }
            catch (Exception ex)
            {
                errors++;
                _logger?.LogWarning("Failed to parse {Path}: {Error}", file.Path, ex.Message);
            }

            if (parsed % 20 == 0 && parsed > 0)
                _logger?.LogInformation("Parsed {Parsed}/{Total} packages ({Errors} errors)",
                    parsed, dtsxFiles.Count, errors);
        }

        _logger?.LogInformation("SSIS extraction complete: {Parsed} parsed, {Errors} errors, {Nodes} nodes, {Edges} edges",
            parsed, errors, graph.Nodes.Count, graph.Edges.Count);
    }

    private void ProcessPackage(string xml, string packageName, string filePath, LineageGraph graph)
    {
        var doc = XDocument.Parse(xml);
        var root = doc.Root;
        if (root == null) return;

        var packageFqn = $"[SSIS].{packageName}";
        var packageNode = graph.AddNode(new LineageNode
        {
            NodeTypeId = WellKnownNodeTypes.SsisPackage,
            NodeTypeName = "SsisPackage",
            FullyQualifiedName = packageFqn,
            DisplayName = packageName,
            SourceLocation = filePath,
            Metadata = new Dictionary<string, string>
            {
                ["tfsPath"] = filePath
            }
        });

        // Connection Managers → extract server/database mappings
        var connMappings = ExtractConnectionManagers(root);

        // Process executables recursively
        var executables = root.Elements(DtsNs + "Executables")
            .Elements(DtsNs + "Executable");
        foreach (var exec in executables)
        {
            ProcessExecutable(exec, packageFqn, packageNode, connMappings, graph);
        }
    }

    private void ProcessExecutable(XElement exec, string parentFqn, LineageNode packageNode,
        Dictionary<string, ConnInfo> connMappings, LineageGraph graph)
    {
        var execName = GetDtsProperty(exec, "ObjectName") ?? "Unknown";
        var execType = GetDtsProperty(exec, "ExecutableType") ?? "";
        var execFqn = $"{parentFqn}.{execName}";

        var execNode = graph.AddNode(new LineageNode
        {
            NodeTypeId = WellKnownNodeTypes.SsisExecutable,
            NodeTypeName = "SsisExecutable",
            FullyQualifiedName = execFqn,
            DisplayName = execName,
            Metadata = new Dictionary<string, string>
            {
                ["executableType"] = execType
            }
        });

        // Link package → executable
        graph.AddEdge(new LineageEdge
        {
            SourceNodeId = packageNode.Id,
            TargetNodeId = execNode.Id,
            EdgeType = EdgeTypes.ProcessExecution,
            MechanismNodeId = packageNode.Id
        });

        // Check if this is a Data Flow Task (Pipeline)
        if (execType.Contains("Pipeline", StringComparison.OrdinalIgnoreCase) ||
            execType.Contains("SSIS.Pipeline", StringComparison.OrdinalIgnoreCase))
        {
            ProcessDataFlow(exec, execFqn, execNode, connMappings, graph);
        }

        // Check for Execute SQL Task
        if (execType.Contains("ExecuteSQLTask", StringComparison.OrdinalIgnoreCase))
        {
            ProcessExecuteSqlTask(exec, execFqn, execNode, connMappings, graph);
        }

        // Recurse into nested executables (containers, sequences)
        var nestedExecs = exec.Elements(DtsNs + "Executables")
            .Elements(DtsNs + "Executable");
        foreach (var nested in nestedExecs)
        {
            ProcessExecutable(nested, execFqn, packageNode, connMappings, graph);
        }
    }

    private void ProcessDataFlow(XElement exec, string execFqn, LineageNode execNode,
        Dictionary<string, ConnInfo> connMappings, LineageGraph graph)
    {
        // Data Flow components are in ObjectData/pipeline/components
        var objectData = exec.Element(DtsNs + "ObjectData");
        var pipeline = objectData?.Element("pipeline") ?? objectData?.Element(DtsNs + "pipeline");
        if (pipeline == null) return;

        var components = pipeline.Descendants("component").ToList();
        if (!components.Any())
            components = pipeline.Descendants(DtsNs + "component").ToList();

        foreach (var component in components)
        {
            var compName = component.Attribute("name")?.Value ?? "";
            var compClassId = component.Attribute("componentClassID")?.Value ?? "";
            var compFqn = $"{execFqn}.{compName}";

            var compNode = graph.AddNode(new LineageNode
            {
                NodeTypeId = WellKnownNodeTypes.SsisComponent,
                NodeTypeName = "SsisComponent",
                FullyQualifiedName = compFqn,
                DisplayName = compName,
                Metadata = new Dictionary<string, string>
                {
                    ["componentClassID"] = compClassId
                }
            });

            // Extract output columns
            var outputs = component.Descendants("output").Concat(component.Descendants("outputColumn"));
            foreach (var output in component.Descendants("outputColumn"))
            {
                var colName = output.Attribute("name")?.Value ?? "";
                if (string.IsNullOrEmpty(colName)) continue;

                var colFqn = $"{compFqn}.{colName}";
                graph.AddNode(new LineageNode
                {
                    NodeTypeId = WellKnownNodeTypes.SsisComponentColumn,
                    NodeTypeName = "SsisComponentColumn",
                    FullyQualifiedName = colFqn,
                    DisplayName = colName
                });
            }

            // If this is an OLE DB Source, extract SQL query and parse it
            if (compClassId.Contains("OLEDBSource", StringComparison.OrdinalIgnoreCase) ||
                compClassId.Contains("Microsoft.OleDBSource", StringComparison.OrdinalIgnoreCase))
            {
                var sqlCommand = GetComponentProperty(component, "SqlCommand")
                    ?? GetComponentProperty(component, "OpenRowset");

                if (!string.IsNullOrEmpty(sqlCommand) && sqlCommand.Contains("SELECT", StringComparison.OrdinalIgnoreCase))
                {
                    // Determine database context from connection manager
                    var connName = GetComponentConnectionManagerName(component);
                    var connInfo = connName != null && connMappings.TryGetValue(connName, out var ci)
                        ? ci : null;

                    try
                    {
                        var parseResult = _sqlParser.Parse(sqlCommand, connInfo?.Database, "dbo");
                        // Create edges from parsed SQL sources to this component
                        foreach (var entry in parseResult.Entries)
                        {
                            var sourceColFqn = $"{connInfo?.Server ?? "DWH-VDI"}.{connInfo?.Database ?? ""}.dbo.{entry.SourceTable}.{entry.SourceColumn}";
                            var targetColFqn = $"{compFqn}.{entry.TargetColumn}";

                            var sourceNode = graph.FindNode(sourceColFqn);
                            var targetNode = graph.FindNode(targetColFqn);
                            if (sourceNode != null && targetNode != null)
                            {
                                graph.AddEdge(new LineageEdge
                                {
                                    SourceNodeId = sourceNode.Id,
                                    TargetNodeId = targetNode.Id,
                                    EdgeType = entry.EdgeType,
                                    MechanismNodeId = compNode.Id,
                                    TransformExpression = entry.TransformExpression
                                });
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        _logger?.LogDebug("Failed to parse SQL in {Comp}: {Error}", compFqn, ex.Message);
                    }
                }
            }

            // If OLE DB Destination, record target table
            if (compClassId.Contains("OLEDBDest", StringComparison.OrdinalIgnoreCase) ||
                compClassId.Contains("Microsoft.OleDBDestination", StringComparison.OrdinalIgnoreCase))
            {
                var targetTable = GetComponentProperty(component, "OpenRowset");
                if (!string.IsNullOrEmpty(targetTable))
                {
                    compNode.Metadata["targetTable"] = targetTable;
                }
            }
        }
    }

    private void ProcessExecuteSqlTask(XElement exec, string execFqn, LineageNode execNode,
        Dictionary<string, ConnInfo> connMappings, LineageGraph graph)
    {
        // SQL is in ObjectData/SqlTaskData
        var objectData = exec.Element(DtsNs + "ObjectData");
        var sqlTaskData = objectData?.Element(SqlTaskNs + "SqlTaskData");
        if (sqlTaskData == null) return;

        var sqlStatement = sqlTaskData.Attribute(SqlTaskNs + "SqlStatementSource")?.Value;
        if (string.IsNullOrEmpty(sqlStatement)) return;

        // Get connection for context
        var connId = sqlTaskData.Attribute(SqlTaskNs + "Connection")?.Value;
        ConnInfo? connInfo = null;
        if (connId != null) connMappings.TryGetValue(connId, out connInfo);

        try
        {
            var result = _sqlParser.Parse(sqlStatement, connInfo?.Database, "dbo");
            foreach (var entry in result.Entries)
            {
                // Add lineage entries as graph edges
                if (!string.IsNullOrEmpty(entry.TargetTable))
                {
                    var sourceColFqn = $"{connInfo?.Server ?? "DWH-VDI"}.{connInfo?.Database ?? ""}.dbo.{entry.SourceTable}.{entry.SourceColumn}";
                    var targetColFqn = $"{connInfo?.Server ?? "DWH-VDI"}.{connInfo?.Database ?? ""}.dbo.{entry.TargetTable}.{entry.TargetColumn}";

                    var sourceNode = graph.FindNode(sourceColFqn);
                    var targetNode = graph.FindNode(targetColFqn);
                    if (sourceNode != null && targetNode != null)
                    {
                        graph.AddEdge(new LineageEdge
                        {
                            SourceNodeId = sourceNode.Id,
                            TargetNodeId = targetNode.Id,
                            EdgeType = entry.EdgeType,
                            MechanismNodeId = execNode.Id,
                            TransformExpression = entry.TransformExpression
                        });
                    }
                }
            }
        }
        catch (Exception ex)
        {
            _logger?.LogDebug("Failed to parse Execute SQL in {Exec}: {Error}", execFqn, ex.Message);
        }
    }

    // ==================== Helpers ====================

    private Dictionary<string, ConnInfo> ExtractConnectionManagers(XElement root)
    {
        var result = new Dictionary<string, ConnInfo>(StringComparer.OrdinalIgnoreCase);

        var connManagers = root.Elements(DtsNs + "ConnectionManagers")
            .Elements(DtsNs + "ConnectionManager");

        foreach (var cm in connManagers)
        {
            var name = GetDtsProperty(cm, "ObjectName") ?? "";
            var dtsId = GetDtsProperty(cm, "DTSID") ?? "";

            var objectData = cm.Element(DtsNs + "ObjectData");
            var connManagerData = objectData?.Element(DtsNs + "ConnectionManager");
            var connString = connManagerData?.Attribute(DtsNs + "ConnectionString")?.Value ?? "";

            var info = ParseConnString(connString);
            if (info != null)
            {
                result[name] = info;
                if (!string.IsNullOrEmpty(dtsId))
                    result[dtsId] = info;
            }
        }

        return result;
    }

    private static ConnInfo? ParseConnString(string connString)
    {
        if (string.IsNullOrEmpty(connString)) return null;

        string? server = null, database = null;
        foreach (var part in connString.Split(';'))
        {
            var kv = part.Split('=', 2);
            if (kv.Length != 2) continue;
            var key = kv[0].Trim().ToUpperInvariant();
            var val = kv[1].Trim();
            if (key is "DATA SOURCE" or "SERVER") server = val;
            if (key is "INITIAL CATALOG" or "DATABASE") database = val;
        }

        return server != null || database != null
            ? new ConnInfo { Server = server ?? "", Database = database ?? "" }
            : null;
    }

    private static string? GetDtsProperty(XElement element, string propertyName)
    {
        return element.Attribute(DtsNs + propertyName)?.Value;
    }

    private static string? GetComponentProperty(XElement component, string propertyName)
    {
        var props = component.Descendants("property")
            .FirstOrDefault(p => p.Attribute("name")?.Value == propertyName);
        return props?.Value;
    }

    private static string? GetComponentConnectionManagerName(XElement component)
    {
        var conn = component.Descendants("connection").FirstOrDefault();
        return conn?.Attribute("connectionManagerID")?.Value;
    }

    internal class ConnInfo
    {
        public string Server { get; set; } = "";
        public string Database { get; set; } = "";
    }
}
