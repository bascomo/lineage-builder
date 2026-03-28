# LineageBuilder

Column-level data lineage builder for SQL Server DWH ecosystem.

## Project Context

This project builds a **column-level lineage graph** covering the full data pipeline:
Sources -> mETL (staging) -> SSIS/StoredProcedures/Views (core/mart) -> SSAS Cubes -> Reports

### Predecessor: CubeMetaData (bascomo/CubeMetaData)
- .NET Framework 4.7.2 console app that already extracts metadata and parses SQL
- SQLParser uses `Microsoft.SqlServer.TransactSql.ScriptDom` (TSql110Parser) + Visitor pattern
- PackageParser extracts SSIS .dtsx structure
- CollectMetadata collects SSAS cube metadata
- Data stored in `DWH-VDI.MetaMart.lineage.*` tables

### Related: mETL (bascomo/Etl)
- .NET 8 ETL framework, copies data 1:1 from sources to staging
- Metadata in `MetaMart.Metadata.Objects/ObjectFields/Sources`

## Architecture

```
LineageBuilder.sln
├── LineageBuilder.Core/          — Models, interfaces, enums
├── LineageBuilder.SqlParser/     — T-SQL parsing via ScriptDom (TSql160Parser)
├── LineageBuilder.Extractors/    — SSIS, SSAS, SQL Agent, mETL extractors
├── LineageBuilder.Persistence/   — Repository, DB schema (lineage.Node/Edge/Run)
├── LineageBuilder.Api/           — ASP.NET Core Web API + Cytoscape.js UI
├── LineageBuilder.Worker/        — Orchestration console app
└── LineageBuilder.Tests/         — xUnit tests
```

## Tech Stack

- .NET 8, C# 12
- Microsoft.SqlServer.TransactSql.ScriptDom (TSql160Parser)
- Microsoft.Data.SqlClient, Dapper
- ASP.NET Core (API)
- Cytoscape.js + dagre layout (visualization)
- xUnit (tests)

## Database

- Server: DWH-VDI
- Database: MetaMart
- Schema: lineage
- Key tables: Object (2.4M rows), Link (194K rows), QueriesDataFlow (144K rows), AllTablesColumns (2M rows), CubesStructure (133K rows)
- 31 ObjectTypes covering SQL Server, SSAS, SSIS, mETL entities

## Development Strategy

Evolutionary migration from CubeMetaData, NOT rewrite from scratch.
Port existing Visitor.cs logic → refactor → add tests → add Web UI.

## Build & Run

```bash
dotnet build
dotnet test
dotnet run --project LineageBuilder.Worker
dotnet run --project LineageBuilder.Api
```

## Conventions

- Use `ILogger<T>` for logging
- Use `IOptions<T>` pattern for configuration
- Async/await where applicable
- All public API methods with XML-doc comments
