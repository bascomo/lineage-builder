using Dapper;
using Microsoft.Data.SqlClient;
using LineageBuilder.Core.Interfaces;
using LineageBuilder.Core.Model;
using Microsoft.Extensions.Logging;

namespace LineageBuilder.Extractors;

/// <summary>
/// Extracts SQL Agent Job → Step → SSIS Package / Stored Procedure relationships.
/// Reads from lineage.Jobs, lineage.JobSteps, lineage.JobSchedules in MetaMart.
/// </summary>
public class SqlAgentJobExtractor : IMetadataExtractor
{
    private readonly string _metaMartConnectionString;
    private readonly ILogger<SqlAgentJobExtractor>? _logger;

    public string Name => "SqlAgentJobs";

    public SqlAgentJobExtractor(string metaMartConnectionString, ILogger<SqlAgentJobExtractor>? logger = null)
    {
        _metaMartConnectionString = metaMartConnectionString;
        _logger = logger;
    }

    public async Task ExtractAsync(LineageGraph graph, CancellationToken cancellationToken = default)
    {
        _logger?.LogInformation("Starting SQL Agent Job extraction...");

        await using var conn = new SqlConnection(_metaMartConnectionString);
        await conn.OpenAsync(cancellationToken);

        // Get all jobs
        var jobs = await conn.QueryAsync<dynamic>(@"
            SELECT job_id, name, enabled, description, server_name
            FROM lineage.Jobs");

        var jobList = jobs.ToList();
        _logger?.LogInformation("Found {Count} SQL Agent Jobs", jobList.Count);

        foreach (var job in jobList)
        {
            if (cancellationToken.IsCancellationRequested) break;

            string jobName = job.name;
            string serverName = job.server_name ?? "DWH-VDI";
            string jobId = job.job_id?.ToString() ?? "";

            var jobNode = graph.AddNode(new LineageNode
            {
                NodeType = NodeType.SqlAgentJob,
                FullyQualifiedName = $"[SqlAgent].{serverName}.{jobName}",
                DisplayName = jobName,
                Metadata = new Dictionary<string, string>
                {
                    ["enabled"] = job.enabled?.ToString() ?? "0",
                    ["description"] = job.description ?? "",
                    ["server"] = serverName
                }
            });

            // Get steps for this job
            var steps = await conn.QueryAsync<dynamic>(@"
                SELECT step_id, step_name, subsystem, command, database_name, server, proxy_name
                FROM lineage.JobSteps
                WHERE job_id = @JobId
                ORDER BY step_id",
                new { JobId = job.job_id });

            foreach (var step in steps)
            {
                string stepName = step.step_name;
                string subsystem = step.subsystem ?? "";
                string command = step.command ?? "";
                string database = step.database_name ?? "";
                int stepId = step.step_id;

                var stepNode = graph.AddNode(new LineageNode
                {
                    NodeType = NodeType.SqlAgentJobStep,
                    FullyQualifiedName = $"[SqlAgent].{serverName}.{jobName}.Step{stepId}",
                    DisplayName = $"{jobName} → {stepName}",
                    ParentNodeId = jobNode.Id,
                    Metadata = new Dictionary<string, string>
                    {
                        ["subsystem"] = subsystem,
                        ["database"] = database,
                        ["stepId"] = stepId.ToString()
                    }
                });

                // Job → Step edge
                graph.AddEdge(new LineageEdge
                {
                    SourceNodeId = jobNode.Id,
                    TargetNodeId = stepNode.Id,
                    EdgeType = EdgeType.ProcessExecution,
                    MechanismType = MechanismType.SqlAgentJob,
                    MechanismLocation = $"[SqlAgent].{serverName}.{jobName}"
                });

                // Link step to target based on subsystem
                LinkStepToTarget(graph, stepNode, subsystem, command, database, serverName);
            }
        }

        _logger?.LogInformation("SQL Agent Job extraction complete: {Nodes} nodes, {Edges} edges",
            graph.Nodes.Count, graph.Edges.Count);
    }

    private void LinkStepToTarget(LineageGraph graph, LineageNode stepNode,
        string subsystem, string command, string database, string serverName)
    {
        switch (subsystem.ToUpperInvariant())
        {
            case "SSIS":
            {
                // Extract package name from SSIS command
                var packageName = ExtractSsisPackageName(command);
                if (!string.IsNullOrEmpty(packageName))
                {
                    var packageNode = graph.FindNode(packageName);
                    if (packageNode == null)
                    {
                        // Create placeholder node
                        packageNode = graph.AddNode(new LineageNode
                        {
                            NodeType = NodeType.SsisPackage,
                            FullyQualifiedName = packageName,
                            DisplayName = packageName.Split('\\').Last()
                        });
                    }
                    graph.AddEdge(new LineageEdge
                    {
                        SourceNodeId = stepNode.Id,
                        TargetNodeId = packageNode.Id,
                        EdgeType = EdgeType.ProcessExecution,
                        MechanismType = MechanismType.SqlAgentJob
                    });
                }
                break;
            }
            case "TSQL":
            {
                // Try to find referenced stored procedures
                var procNames = ExtractProcedureNames(command);
                foreach (var procName in procNames)
                {
                    var fqn = procName.Contains('.')
                        ? $"{serverName}.{database}.{procName}"
                        : $"{serverName}.{database}.dbo.{procName}";

                    var procNode = graph.FindNode(fqn);
                    if (procNode == null)
                    {
                        procNode = graph.AddNode(new LineageNode
                        {
                            NodeType = NodeType.StoredProcedure,
                            FullyQualifiedName = fqn,
                            DisplayName = procName
                        });
                    }
                    graph.AddEdge(new LineageEdge
                    {
                        SourceNodeId = stepNode.Id,
                        TargetNodeId = procNode.Id,
                        EdgeType = EdgeType.ProcessExecution,
                        MechanismType = MechanismType.SqlAgentJob,
                        MechanismLocation = stepNode.FullyQualifiedName
                    });
                }
                break;
            }
            case "CMDEXEC":
            {
                // CmdExec may invoke CubeMetaData.exe, mETL, etc.
                _logger?.LogDebug("CmdExec step: {Command}", command.Length > 200 ? command[..200] : command);
                break;
            }
        }
    }

    private static string ExtractSsisPackageName(string command)
    {
        // SSIS commands often contain /ISSERVER "\"\SSISDB\...\PackageName.dtsx\"" or /FILE "path\Package.dtsx"
        // Also: /ISSERVER "\"\\folder\\project\\package.dtsx\""
        var idx = command.IndexOf(".dtsx", StringComparison.OrdinalIgnoreCase);
        if (idx < 0) return "";

        // Walk backwards to find start of package path
        var start = command.LastIndexOf('\\', idx);
        if (start < 0) start = command.LastIndexOf('/', idx);
        if (start < 0) start = 0;
        else start++;

        var name = command[start..(idx + 5)]; // include .dtsx
        // Clean quotes and backslashes
        name = name.Trim('"', '\\', ' ');
        // Return just the package filename without extension
        if (name.EndsWith(".dtsx", StringComparison.OrdinalIgnoreCase))
            name = name[..^5];
        return name;
    }

    private static List<string> ExtractProcedureNames(string command)
    {
        var names = new List<string>();
        if (string.IsNullOrWhiteSpace(command)) return names;

        // Simple pattern: EXEC[UTE] [schema.]procedure_name
        var lines = command.Split('\n', '\r');
        foreach (var line in lines)
        {
            var trimmed = line.Trim();
            if (trimmed.StartsWith("EXEC ", StringComparison.OrdinalIgnoreCase) ||
                trimmed.StartsWith("EXECUTE ", StringComparison.OrdinalIgnoreCase))
            {
                var afterExec = trimmed.IndexOf(' ') + 1;
                var rest = trimmed[afterExec..].Trim();
                // Take first word (procedure name)
                var endIdx = rest.IndexOfAny(new[] { ' ', ';', '(', '\t', '\r', '\n' });
                var procName = endIdx > 0 ? rest[..endIdx] : rest;
                procName = procName.Trim('[', ']', '"');
                if (!string.IsNullOrEmpty(procName) && !procName.StartsWith("@") && !procName.StartsWith("sp_"))
                    names.Add(procName);
            }
        }
        return names;
    }
}
