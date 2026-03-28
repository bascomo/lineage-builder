using System.Net.Http.Json;
using System.Text.Json;
using System.Text.Json.Serialization;
using Microsoft.Extensions.Logging;

namespace LineageBuilder.Extractors;

/// <summary>
/// Client for TFS REST API to download SSIS packages from source control.
/// TFS Server: http://tfs-tsum:8080/tfs/tfs_olapcollection
/// Uses Windows Integrated Authentication.
/// </summary>
public class TfsClient : IDisposable
{
    private readonly HttpClient _httpClient;
    private readonly string _baseUrl;
    private readonly ILogger<TfsClient>? _logger;

    /// <summary>
    /// Create TFS client.
    /// </summary>
    /// <param name="tfsUrl">TFS collection URL, e.g. http://tfs-tsum:8080/tfs/tfs_olapcollection</param>
    public TfsClient(string tfsUrl, ILogger<TfsClient>? logger = null)
    {
        _baseUrl = tfsUrl.TrimEnd('/');
        _logger = logger;

        var handler = new HttpClientHandler
        {
            UseDefaultCredentials = true // Windows Integrated Auth (Kerberos/NTLM)
        };
        _httpClient = new HttpClient(handler)
        {
            Timeout = TimeSpan.FromMinutes(5)
        };
    }

    /// <summary>
    /// List all projects in the TFS collection.
    /// </summary>
    public async Task<List<TfsProject>> GetProjectsAsync(CancellationToken ct = default)
    {
        var url = $"{_baseUrl}/_apis/projects?api-version=7.0";
        var response = await _httpClient.GetFromJsonAsync<TfsListResponse<TfsProject>>(url, ct);
        return response?.Value ?? new List<TfsProject>();
    }

    /// <summary>
    /// List items (files/folders) at a given path in TFVC source control.
    /// </summary>
    public async Task<List<TfsItem>> GetItemsAsync(string scopePath, bool recursionFull = false, CancellationToken ct = default)
    {
        var recursion = recursionFull ? "Full" : "OneLevel";
        var url = $"{_baseUrl}/_apis/tfvc/items?scopePath={Uri.EscapeDataString(scopePath)}&recursionLevel={recursion}&api-version=7.0";
        _logger?.LogDebug("GET {Url}", url);

        var response = await _httpClient.GetFromJsonAsync<TfsListResponse<TfsItem>>(url, ct);
        return response?.Value ?? new List<TfsItem>();
    }

    /// <summary>
    /// Download a file content from TFVC by path.
    /// </summary>
    public async Task<string> DownloadFileAsync(string path, CancellationToken ct = default)
    {
        var url = $"{_baseUrl}/_apis/tfvc/items?path={Uri.EscapeDataString(path)}&api-version=7.0";
        _logger?.LogDebug("Downloading {Path}", path);

        var response = await _httpClient.GetAsync(url, ct);
        response.EnsureSuccessStatusCode();
        return await response.Content.ReadAsStringAsync(ct);
    }

    /// <summary>
    /// Download a file as bytes from TFVC.
    /// </summary>
    public async Task<byte[]> DownloadFileBytesAsync(string path, CancellationToken ct = default)
    {
        var url = $"{_baseUrl}/_apis/tfvc/items?path={Uri.EscapeDataString(path)}&api-version=7.0";
        var response = await _httpClient.GetAsync(url, ct);
        response.EnsureSuccessStatusCode();
        return await response.Content.ReadAsByteArrayAsync(ct);
    }

    /// <summary>
    /// Find all .dtsx files under a given path.
    /// </summary>
    public async Task<List<TfsItem>> FindDtsxFilesAsync(string rootPath, CancellationToken ct = default)
    {
        _logger?.LogInformation("Searching for .dtsx files under {Path}", rootPath);
        var allItems = await GetItemsAsync(rootPath, recursionFull: true, ct);
        var dtsxFiles = allItems
            .Where(i => !i.IsFolder && i.Path.EndsWith(".dtsx", StringComparison.OrdinalIgnoreCase))
            .ToList();
        _logger?.LogInformation("Found {Count} .dtsx files", dtsxFiles.Count);
        return dtsxFiles;
    }

    public void Dispose() => _httpClient.Dispose();
}

public class TfsListResponse<T>
{
    [JsonPropertyName("count")]
    public int Count { get; set; }

    [JsonPropertyName("value")]
    public List<T> Value { get; set; } = new();
}

public class TfsProject
{
    [JsonPropertyName("id")]
    public string Id { get; set; } = "";

    [JsonPropertyName("name")]
    public string Name { get; set; } = "";
}

public class TfsItem
{
    [JsonPropertyName("path")]
    public string Path { get; set; } = "";

    [JsonPropertyName("url")]
    public string Url { get; set; } = "";

    [JsonPropertyName("isFolder")]
    public bool IsFolder { get; set; }

    [JsonPropertyName("size")]
    public long Size { get; set; }

    [JsonPropertyName("version")]
    public int Version { get; set; }
}
