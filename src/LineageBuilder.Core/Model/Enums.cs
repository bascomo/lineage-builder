namespace LineageBuilder.Core.Model;

/// <summary>
/// Тип узла в графе lineage.
/// </summary>
public enum NodeType
{
    SqlServer,
    SsasServer,
    DatabaseRelational,
    DatabaseMultidimensional,
    Schema,
    Table,
    View,
    Column,
    StoredProcedure,
    TableFunction,
    SqlAgentJob,
    SqlAgentJobStep,
    SsisPackage,
    SsisExecutable,
    SsisComponent,
    SsisComponentColumn,
    SsasCube,
    SsasDsv,
    SsasDsvTable,
    SsasDsvTableField,
    SsasDsvNamedQuery,
    SsasDsvNamedQueryField,
    SsasDimension,
    SsasDimensionAttribute,
    SsasMeasureGroup,
    SsasMeasure,
    MetlMapping,
    ExternalSource,
    FinalPoint
}

/// <summary>
/// Тип связи (ребра) между узлами.
/// </summary>
public enum EdgeType
{
    /// <summary>Данные перетекают из source в target.</summary>
    DataFlow,
    /// <summary>Данные трансформируются (с выражением).</summary>
    Transform,
    /// <summary>Агрегация (SUM, COUNT и т.д.).</summary>
    Aggregation,
    /// <summary>Фильтрация (WHERE, HAVING).</summary>
    Filter,
    /// <summary>Участие в JOIN.</summary>
    Join,
    /// <summary>SSIS Lookup.</summary>
    Lookup,
    /// <summary>Job запускает пакет/процедуру.</summary>
    ProcessExecution,
    /// <summary>mETL 1:1 копирование.</summary>
    DirectCopy
}

/// <summary>
/// Механизм, осуществляющий связь.
/// </summary>
public enum MechanismType
{
    View,
    StoredProcedure,
    TableFunction,
    SsisDataFlow,
    SsisExecuteSql,
    SsasDsv,
    SqlAgentJob,
    MetlLoader
}

/// <summary>
/// Слой DWH, к которому принадлежит узел.
/// </summary>
public enum LayerName
{
    Source,
    Staging,
    Core,
    DataMart,
    Cube,
    Report
}
