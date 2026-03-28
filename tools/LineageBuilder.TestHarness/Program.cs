using System.Diagnostics;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using LineageBuilder.Core.Interfaces;
using LineageBuilder.Core.Model;
using LineageBuilder.SqlParser;
using LineageBuilder.SqlParser.Schema;

Console.OutputEncoding = Encoding.UTF8;
Console.WriteLine("=== LineageBuilder Test Harness ===");
Console.WriteLine("Reads from DWH-VDI.MetaMart via gateway, parses SQL, writes to DWHDEDEV-VDI.DWH.lineage_v2");
Console.WriteLine();

var gw = new GatewayClient();

// Step 1: Fetch SQL object definitions (by database to keep under select_full 10K limit)
Console.WriteLine("Step 1: Fetching SQL objects from AllObjectsDefinition...");

// First get list of databases (use select_full to avoid 10 row limit)
var databases = await gw.SelectFullAsync<DbInfo>("MetaMart",
    "SELECT ServerName, DBName, COUNT(*) AS Cnt FROM lineage.AllObjectsDefinition " +
    "WHERE ObjectType IN ('V','P') AND ObjectDefinition IS NOT NULL " +
    "GROUP BY ServerName, DBName ORDER BY Cnt DESC");
Console.WriteLine($"  Found {databases.Count} server.database combinations");

var allObjects = new List<SqlObjectDef>();
foreach (var db in databases)
{
    try
    {
        var batch = await gw.SelectFullAsync<SqlObjectDef>("MetaMart",
            $"SELECT ServerName, DBName, SchemaName, ObjectName, ObjectType, ObjectDefinition " +
            $"FROM lineage.AllObjectsDefinition WHERE ObjectType IN ('V','P') AND ObjectDefinition IS NOT NULL " +
            $"AND ServerName = '{db.ServerName}' AND DBName = '{db.DBName}'");
        allObjects.AddRange(batch);
        Console.Write($"\r  Fetched {allObjects.Count} ({db.ServerName}.{db.DBName}: {batch.Count})...");
    }
    catch (Exception ex)
    {
        Console.WriteLine($"\n  Skip {db.ServerName}.{db.DBName}: {ex.Message[..Math.Min(80, ex.Message.Length)]}");
    }
}

// Deduplicate
var unique = allObjects
    .GroupBy(o => $"{o.ServerName}.{o.DBName}.{o.SchemaName}.{o.ObjectName}")
    .Select(g => g.First())
    .ToList();
Console.WriteLine($"\r  Fetched {allObjects.Count} total, {unique.Count} unique objects.          ");

// Step 2: Parse all
Console.WriteLine("\nStep 2: Parsing...");
var parser = new TsqlLineageParser(new InMemorySchemaProvider());

int success = 0, parseErrors = 0, totalEntries = 0;
var errorTypes = new Dictionary<string, int>();
var allEntries = new List<(string ObjFqn, SqlLineageResult Result)>();

var sw = Stopwatch.StartNew();

foreach (var obj in unique)
{
    var result = parser.Parse(obj.ObjectDefinition, obj.DBName, obj.SchemaName);
    var objFqn = $"{obj.ServerName}.{obj.DBName}.{obj.SchemaName}.{obj.ObjectName}";

    if (result.HasErrors)
    {
        parseErrors++;
        var firstError = result.Errors[0];
        var errorKey = firstError.Length > 80 ? firstError[..80] : firstError;
        errorTypes[errorKey] = errorTypes.GetValueOrDefault(errorKey) + 1;
    }
    else
    {
        success++;
        totalEntries += result.Entries.Count;
        if (result.Entries.Count > 0)
            allEntries.Add((objFqn, result));
    }

    if ((success + parseErrors) % 200 == 0)
        Console.Write($"\r  {success + parseErrors}/{unique.Count} (OK: {success}, Err: {parseErrors}, Entries: {totalEntries})...");
}
sw.Stop();

Console.WriteLine($"\r  Parsed {unique.Count} in {sw.Elapsed.TotalSeconds:F1}s                              ");
Console.WriteLine($"  OK: {success} ({100.0 * success / unique.Count:F1}%)");
Console.WriteLine($"  Errors: {parseErrors} ({100.0 * parseErrors / unique.Count:F1}%)");
Console.WriteLine($"  Lineage entries: {totalEntries}");
Console.WriteLine($"  Speed: {unique.Count / sw.Elapsed.TotalSeconds:F0} obj/sec");

if (errorTypes.Count > 0)
{
    Console.WriteLine("\n  Top errors:");
    foreach (var et in errorTypes.OrderByDescending(kv => kv.Value).Take(10))
        Console.WriteLine($"    [{et.Value}x] {et.Key}");
}

// Step 3: Write to lineage_v2
Console.WriteLine($"\nStep 3: Writing to lineage_v2 ({allEntries.Count} objects with entries)...");

await gw.ExecuteAsync("DWH",
    "INSERT INTO lineage_v2.Run (StartedAt, Status, ExtractorName) VALUES (SYSUTCDATETIME(), 'Running', 'TestHarness')",
    server: "DWHDEDEV-VDI");

int nodesWritten = 0, edgesWritten = 0;
var writtenNodes = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
var writeLimit = 3000;

foreach (var (objFqn, result) in allEntries)
{
    foreach (var entry in result.Entries)
    {
        if (nodesWritten >= writeLimit) break;

        var srcColFqn = $"{entry.SourceTable}.{entry.SourceColumn}";
        var targetTable = string.IsNullOrEmpty(entry.TargetTable) ? objFqn : entry.TargetTable;
        var tgtColFqn = $"{targetTable}.{entry.TargetColumn}";

        foreach (var (fqn, dn, typeId) in new[]
        {
            (entry.SourceTable, entry.SourceTable, 5),
            (srcColFqn, entry.SourceColumn, 6),
            (targetTable, targetTable, 7),
            (tgtColFqn, entry.TargetColumn, 6)
        })
        {
            if (!string.IsNullOrEmpty(fqn) && writtenNodes.Add(fqn))
            {
                var f = fqn.Replace("'", "''");
                var d = dn.Replace("'", "''");
                try
                {
                    await gw.ExecuteAsync("DWH",
                        $"IF NOT EXISTS (SELECT 1 FROM lineage_v2.Node WHERE FullyQualifiedName = N'{f}') " +
                        $"INSERT INTO lineage_v2.Node (NodeTypeId, FullyQualifiedName, DisplayName) VALUES ({typeId}, N'{f}', N'{d}')",
                        server: "DWHDEDEV-VDI");
                    nodesWritten++;
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"  Node write error: {ex.Message[..Math.Min(100, ex.Message.Length)]}");
                }
            }
        }
    }
    if (nodesWritten >= writeLimit) break;

    if (nodesWritten % 100 == 0 && nodesWritten > 0)
        Console.Write($"\r  Nodes: {nodesWritten}...");
}
Console.WriteLine($"\r  Nodes written: {nodesWritten}                  ");

// Write edges
Console.WriteLine("  Writing edges...");
foreach (var (objFqn, result) in allEntries)
{
    foreach (var entry in result.Entries)
    {
        if (edgesWritten >= writeLimit * 2) break;

        var srcColFqn = $"{entry.SourceTable}.{entry.SourceColumn}";
        var targetTable = string.IsNullOrEmpty(entry.TargetTable) ? objFqn : entry.TargetTable;
        var tgtColFqn = $"{targetTable}.{entry.TargetColumn}";

        if (!writtenNodes.Contains(srcColFqn) || !writtenNodes.Contains(tgtColFqn)) continue;

        var s = srcColFqn.Replace("'", "''");
        var t = tgtColFqn.Replace("'", "''");
        var et = entry.EdgeType.Replace("'", "''");
        try
        {
            await gw.ExecuteAsync("DWH",
                $"IF NOT EXISTS (SELECT 1 FROM lineage_v2.Edge e JOIN lineage_v2.Node s ON e.SourceNodeId=s.NodeId JOIN lineage_v2.Node t ON e.TargetNodeId=t.NodeId WHERE s.FullyQualifiedName=N'{s}' AND t.FullyQualifiedName=N'{t}' AND e.EdgeType='{et}') " +
                $"INSERT INTO lineage_v2.Edge (SourceNodeId, TargetNodeId, EdgeType) " +
                $"SELECT s.NodeId, t.NodeId, '{et}' FROM lineage_v2.Node s CROSS JOIN lineage_v2.Node t " +
                $"WHERE s.FullyQualifiedName=N'{s}' AND t.FullyQualifiedName=N'{t}'",
                server: "DWHDEDEV-VDI");
            edgesWritten++;
        }
        catch { }
    }
    if (edgesWritten >= writeLimit * 2) break;

    if (edgesWritten % 100 == 0 && edgesWritten > 0)
        Console.Write($"\r  Edges: {edgesWritten}...");
}

await gw.ExecuteAsync("DWH",
    $"UPDATE lineage_v2.Run SET Status='Completed', CompletedAt=SYSUTCDATETIME(), NodesCreated={nodesWritten}, EdgesCreated={edgesWritten} WHERE Status='Running'",
    server: "DWHDEDEV-VDI");

Console.WriteLine($"\r  Edges written: {edgesWritten}                  ");

// Verify
var nc = await gw.SelectAsync<CountResult>("DWH", "SELECT COUNT(*) AS Cnt FROM lineage_v2.Node", "DWHDEDEV-VDI");
var ec = await gw.SelectAsync<CountResult>("DWH", "SELECT COUNT(*) AS Cnt FROM lineage_v2.Edge", "DWHDEDEV-VDI");
Console.WriteLine($"\n=== lineage_v2: {nc[0].Cnt} nodes, {ec[0].Cnt} edges ===");
Console.WriteLine("Done!");

// ==================== Gateway Client ====================
class GatewayClient
{
    const string Url = "https://ibmcognos-test.mhpost.ru/gateway/gateway.ashx";
    const string Secret = "cdOe_6JSZK4KOlqW2k9CkliSpeUsG9YYApC96dqBbz-4rrHY2JAZ1-KByVeA1KHT";
    readonly HttpClient _http;

    public GatewayClient()
    {
        var h = new HttpClientHandler { ServerCertificateCustomValidationCallback = (_, _, _, _) => true };
        _http = new HttpClient(h) { Timeout = TimeSpan.FromMinutes(5) };
    }

    string Token()
    {
        var w = DateTimeOffset.UtcNow.ToUnixTimeSeconds() / 30;
        using var hmac = new HMACSHA256(Encoding.UTF8.GetBytes(Secret));
        return Convert.ToHexString(hmac.ComputeHash(Encoding.UTF8.GetBytes(w.ToString()))).ToLower();
    }

    public Task<List<T>> SelectAsync<T>(string db, string query, string server = "DWH-VDI") =>
        PostAsync<List<T>>(new { action = "select", database = db, query, server });

    public Task<List<T>> SelectFullAsync<T>(string db, string query, string server = "DWH-VDI") =>
        PostAsync<List<T>>(new { action = "select_full", database = db, query, server });

    public async Task ExecuteAsync(string db, string sql, string server = "DWH-VDI")
    {
        var json = JsonSerializer.Serialize(new { action = "execute_sql", database = db, sql, server });
        var req = new HttpRequestMessage(HttpMethod.Post, Url)
        {
            Content = new StringContent(json, Encoding.UTF8, "application/json")
        };
        req.Headers.Add("X-Api-Key", Token());
        var resp = await _http.SendAsync(req);
        if (!resp.IsSuccessStatusCode)
        {
            var body = await resp.Content.ReadAsStringAsync();
            throw new Exception($"GW {resp.StatusCode}: {body[..Math.Min(200, body.Length)]}");
        }
    }

    async Task<T> PostAsync<T>(object payload)
    {
        var json = JsonSerializer.Serialize(payload);
        var req = new HttpRequestMessage(HttpMethod.Post, Url)
        {
            Content = new StringContent(json, Encoding.UTF8, "application/json")
        };
        req.Headers.TryAddWithoutValidation("X-Api-Key", Token());
        var resp = await _http.SendAsync(req);
        var body = await resp.Content.ReadAsStringAsync();
        if (!resp.IsSuccessStatusCode)
        {
            throw new Exception($"GW {resp.StatusCode}: {body[..Math.Min(300, body.Length)]}");
        }
        try
        {
            var opts = new JsonSerializerOptions { PropertyNameCaseInsensitive = true };
            // Gateway wraps result in {"data":[...]} for select_full
            using var doc = JsonDocument.Parse(body);
            if (doc.RootElement.TryGetProperty("data", out var dataElement))
                return JsonSerializer.Deserialize<T>(dataElement.GetRawText(), opts)!;
            return JsonSerializer.Deserialize<T>(body, opts)!;
        }
        catch (JsonException ex)
        {
            throw new Exception($"JSON parse error ({typeof(T).Name}): {ex.Message}. Body: {body[..Math.Min(200, body.Length)]}");
        }
    }
}

record CountResult(int Cnt);
record DbInfo(string ServerName, string DBName, int Cnt);
record SqlObjectDef(string ServerName, string DBName, string SchemaName, string ObjectName, string ObjectType, string ObjectDefinition);
