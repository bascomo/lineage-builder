namespace LineageBuilder.Core.Model;

/// <summary>
/// Well-known Node Type IDs matching lineage_v2.NodeType table.
/// Used in code for convenience; actual source of truth is the DB table.
/// </summary>
public static class WellKnownNodeTypes
{
    // SQL Server Platform
    public const int SqlServerPlatform = 1;
    public const int SqlServer = 2;
    public const int Database = 3;
    public const int Schema = 4;
    public const int Table = 5;
    public const int Column = 6;
    public const int View = 7;
    public const int StoredProcedure = 8;
    public const int TableFunction = 9;

    // SSAS Platform
    public const int SsasPlatform = 10;
    public const int SsasServer = 11;
    public const int SsasDatabase = 12;
    public const int Cube = 13;
    public const int Dsv = 14;
    public const int MeasureGroup = 15;
    public const int Measure = 16;
    public const int Dimension = 17;
    public const int DimensionAttribute = 18;
    public const int DsvTable = 19;
    public const int DsvNamedQuery = 20;
    public const int DsvTableField = 21;
    public const int DsvNqField = 22;

    // SSIS Platform
    public const int SsisPlatform = 23;
    public const int SsisProject = 24;
    public const int SsisPackage = 25;
    public const int SsisExecutable = 26;
    public const int SsisComponent = 27;
    public const int SsisComponentColumn = 28;

    // SQL Agent Platform
    public const int SqlAgentPlatform = 29;
    public const int SqlAgentJob = 30;
    public const int SqlAgentJobStep = 31;

    // mETL Platform
    public const int MetlPlatform = 32;
    public const int MetlMapping = 33;
}

/// <summary>
/// Well-known Edge Types as string constants.
/// </summary>
public static class EdgeTypes
{
    public const string DataFlow = "DataFlow";
    public const string Transform = "Transform";
    public const string Aggregation = "Aggregation";
    public const string Filter = "Filter";
    public const string Join = "Join";
    public const string DirectCopy = "DirectCopy";
    public const string ProcessExecution = "ProcessExecution";
    public const string Lookup = "Lookup";
}

/// <summary>
/// Well-known Layer Names.
/// </summary>
public static class Layers
{
    public const string Source = "Source";
    public const string Staging = "Staging";
    public const string Core = "Core";
    public const string DataMart = "DataMart";
    public const string Cube = "Cube";
    public const string Report = "Report";
}
