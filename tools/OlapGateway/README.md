# OLAP Gateway

HTTP proxy for SSAS Multidimensional servers. Deploy on IIS alongside SQL gateway.

## Deploy

1. Copy `olap-gateway.ashx` to IIS virtual directory (e.g. `C:\inetpub\wwwroot\gateway\`)
2. Ensure AMO assemblies are available:
   - `Microsoft.AnalysisServices.dll`
   - `Microsoft.AnalysisServices.AdomdClient.dll`
   - Copy from NuGet package or install SSAS client tools
3. App pool must run under account with access to SSAS servers (Windows Auth)

## API

All requests: `POST https://ibmcognos-test.mhpost.ru/olap-gateway/olap-gateway.ashx`
Header: `X-Api-Key: <HMAC-SHA256 TOTP token>` (same secret as SQL gateway)

### Actions

| Action | Parameters | Description |
|---|---|---|
| `ping` | server? | Health check |
| `get_databases` | server | List SSAS databases |
| `get_cubes` | server, database | List cubes in database |
| `get_dimensions` | server, database, cube | Dimensions + attributes with key/name column bindings |
| `get_measures` | server, database, cube | Measure groups + measures with source bindings |
| `get_dsv` | server, database | List Data Source Views |
| `get_dsv_tables` | server, database, dsv | DSV tables/named queries + columns |
| `get_named_query_sql` | server, database, dsv, table | SQL of a named query |
| `get_full_structure` | server, database | Compact dump: cubes + dimensions + measures + DSV |
| `mdx` | server, database, query | Execute MDX query (max 10K rows) |
