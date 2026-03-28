-- ============================================================
-- LineageBuilder v2: Graph persistence schema
-- Target: DWH-VDI.MetaMart.lineage_v2
-- ============================================================

IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'lineage_v2')
    EXEC('CREATE SCHEMA lineage_v2');
GO

-- ============================================================
-- СПРАВОЧНИК ТИПОВ УЗЛОВ (иерархический)
-- ============================================================
DROP TABLE IF EXISTS lineage_v2.NodeHistory;
DROP TABLE IF EXISTS lineage_v2.Edge;
DROP TABLE IF EXISTS lineage_v2.Node;
DROP TABLE IF EXISTS lineage_v2.NodeType;
DROP TABLE IF EXISTS lineage_v2.Run;
GO

CREATE TABLE lineage_v2.NodeType (
    NodeTypeId      INT IDENTITY(1,1) PRIMARY KEY,
    Name            VARCHAR(100)    NOT NULL,
    ParentTypeId    INT             NULL REFERENCES lineage_v2.NodeType(NodeTypeId),
    Level           INT             NOT NULL DEFAULT 0,
    Description     NVARCHAR(500)   NULL,

    CONSTRAINT UQ_NodeType_Name UNIQUE (Name)
);
GO

-- Заполнение справочника
SET IDENTITY_INSERT lineage_v2.NodeType ON;

INSERT INTO lineage_v2.NodeType (NodeTypeId, Name, ParentTypeId, Level, Description) VALUES
-- SQL Server Platform
( 1, 'SQL Server Platform',      NULL, 0, 'Microsoft SQL Server ecosystem'),
( 2, 'SQL Server',                  1, 1, 'SQL Server instance'),
( 3, 'Database',                    2, 2, 'Relational database'),
( 4, 'Schema',                      3, 3, 'Database schema'),
( 5, 'Table',                       4, 4, 'Database table'),
( 6, 'Column',                      5, 5, 'Table/view column'),
( 7, 'View',                        4, 4, 'Database view'),
( 8, 'StoredProcedure',             4, 4, 'Stored procedure'),
( 9, 'TableFunction',               4, 4, 'Table-valued or scalar function'),

-- SSAS Platform
(10, 'SSAS Platform',            NULL, 0, 'SQL Server Analysis Services ecosystem'),
(11, 'SSAS Server',               10, 1, 'SSAS server instance'),
(12, 'SSAS Database',              11, 2, 'Multidimensional database'),
(13, 'Cube',                       12, 3, 'OLAP cube'),
(14, 'DSV',                        12, 3, 'Data Source View'),
(15, 'MeasureGroup',               13, 4, 'Measure group within cube'),
(16, 'Measure',                    15, 5, 'Cube measure'),
(17, 'Dimension',                  13, 4, 'Cube dimension'),
(18, 'DimensionAttribute',         17, 5, 'Dimension attribute'),
(19, 'DSV Table',                  14, 4, 'Physical table in DSV'),
(20, 'DSV NamedQuery',             14, 4, 'Named query (virtual table) in DSV'),
(21, 'DSV Table Field',            19, 5, 'Column from physical DSV table'),
(22, 'DSV NQ Field',               20, 5, 'Column from DSV named query'),

-- SSIS Platform
(23, 'SSIS Platform',            NULL, 0, 'SQL Server Integration Services ecosystem'),
(24, 'SSIS Project',               23, 1, 'SSIS project (SSISDB or file system)'),
(25, 'SSIS Package',               24, 2, 'SSIS package (.dtsx)'),
(26, 'SSIS Executable',            25, 3, 'Task or container within package'),
(27, 'SSIS Component',             26, 4, 'Data Flow component'),
(28, 'SSIS Component Column',      27, 5, 'Column within Data Flow component'),

-- SQL Agent Platform
(29, 'SQL Agent Platform',       NULL, 0, 'SQL Server Agent ecosystem'),
(30, 'SQL Agent Job',              29, 1, 'SQL Agent job'),
(31, 'SQL Agent Job Step',         30, 2, 'Job step'),

-- mETL Platform
(32, 'mETL Platform',            NULL, 0, 'Mercury ETL (mETL) ecosystem'),
(33, 'mETL Mapping',               32, 1, 'Source-to-staging mapping configuration');

SET IDENTITY_INSERT lineage_v2.NodeType OFF;
GO

-- ============================================================
-- ИСТОРИЯ ЗАПУСКОВ
-- ============================================================
CREATE TABLE lineage_v2.Run (
    RunId           INT IDENTITY(1,1) PRIMARY KEY,
    StartedAt       DATETIME2       NOT NULL DEFAULT SYSUTCDATETIME(),
    CompletedAt     DATETIME2       NULL,
    Status          VARCHAR(20)     NOT NULL DEFAULT 'Running',
    ExtractorName   VARCHAR(100)    NULL,
    NodesCreated    INT             DEFAULT 0,
    NodesUpdated    INT             DEFAULT 0,
    NodesDeleted    INT             DEFAULT 0,
    EdgesCreated    INT             DEFAULT 0,
    EdgesUpdated    INT             DEFAULT 0,
    EdgesDeleted    INT             DEFAULT 0,
    ErrorLog        NVARCHAR(MAX)   NULL
);
GO

-- ============================================================
-- ТАБЛИЦА УЗЛОВ ГРАФА
-- ============================================================
CREATE TABLE lineage_v2.Node (
    NodeId              INT IDENTITY(1,1) PRIMARY KEY,
    NodeTypeId          INT             NOT NULL REFERENCES lineage_v2.NodeType(NodeTypeId),
    FullyQualifiedName  NVARCHAR(1000)  NOT NULL,
    DisplayName         NVARCHAR(500)   NOT NULL,
    LayerName           VARCHAR(50)     NULL,   -- Source, Staging, Core, DataMart, Cube
    SourceLocation      NVARCHAR(1000)  NULL,   -- where the code/definition lives
    Description         NVARCHAR(MAX)   NULL,
    Metadata            NVARCHAR(MAX)   NULL,   -- JSON for extra attributes
    CreatedAt           DATETIME2       NOT NULL DEFAULT SYSUTCDATETIME(),
    UpdatedAt           DATETIME2       NOT NULL DEFAULT SYSUTCDATETIME(),
    CreatedByRunId      INT             NULL REFERENCES lineage_v2.Run(RunId),
    LastSeenRunId       INT             NULL REFERENCES lineage_v2.Run(RunId),
    IsDeleted           BIT             NOT NULL DEFAULT 0,
    DeletedAt           DATETIME2       NULL,

    CONSTRAINT UQ_Node_FQN UNIQUE (FullyQualifiedName)
);
GO

-- ============================================================
-- ТАБЛИЦА РЁБЕР ГРАФА
-- ============================================================
CREATE TABLE lineage_v2.Edge (
    EdgeId              INT IDENTITY(1,1) PRIMARY KEY,
    SourceNodeId        INT             NOT NULL REFERENCES lineage_v2.Node(NodeId),
    TargetNodeId        INT             NOT NULL REFERENCES lineage_v2.Node(NodeId),
    EdgeType            VARCHAR(50)     NOT NULL,
        -- DataFlow, Transform, Aggregation, Filter, Join,
        -- DirectCopy, ProcessExecution, Lookup
    MechanismNodeId     INT             NULL REFERENCES lineage_v2.Node(NodeId),
    TransformExpression NVARCHAR(MAX)   NULL,
    CreatedAt           DATETIME2       NOT NULL DEFAULT SYSUTCDATETIME(),
    UpdatedAt           DATETIME2       NOT NULL DEFAULT SYSUTCDATETIME(),
    CreatedByRunId      INT             NULL REFERENCES lineage_v2.Run(RunId),
    LastSeenRunId       INT             NULL REFERENCES lineage_v2.Run(RunId),
    IsDeleted           BIT             NOT NULL DEFAULT 0,
    DeletedAt           DATETIME2       NULL,

    CONSTRAINT UQ_Edge UNIQUE (SourceNodeId, TargetNodeId, EdgeType, MechanismNodeId)
);
GO

-- ============================================================
-- ИСТОРИЯ ИЗМЕНЕНИЙ УЗЛОВ
-- ============================================================
CREATE TABLE lineage_v2.NodeHistory (
    HistoryId       INT IDENTITY(1,1) PRIMARY KEY,
    NodeId          INT             NOT NULL REFERENCES lineage_v2.Node(NodeId),
    ChangeType      VARCHAR(20)     NOT NULL,  -- Created, Updated, Deleted, Restored
    ChangedAt       DATETIME2       NOT NULL DEFAULT SYSUTCDATETIME(),
    ChangedByRunId  INT             NULL REFERENCES lineage_v2.Run(RunId),
    OldValues       NVARCHAR(MAX)   NULL,   -- JSON
    NewValues       NVARCHAR(MAX)   NULL    -- JSON
);
GO

-- ============================================================
-- ИНДЕКСЫ
-- ============================================================
CREATE INDEX IX_Node_TypeId     ON lineage_v2.Node(NodeTypeId) INCLUDE (FullyQualifiedName, DisplayName);
CREATE INDEX IX_Node_Layer      ON lineage_v2.Node(LayerName) INCLUDE (FullyQualifiedName, NodeTypeId);
CREATE INDEX IX_Node_DisplayName ON lineage_v2.Node(DisplayName) INCLUDE (FullyQualifiedName, NodeTypeId);
CREATE INDEX IX_Node_NotDeleted ON lineage_v2.Node(IsDeleted) WHERE IsDeleted = 0;

CREATE INDEX IX_Edge_Source     ON lineage_v2.Edge(SourceNodeId) INCLUDE (TargetNodeId, EdgeType);
CREATE INDEX IX_Edge_Target     ON lineage_v2.Edge(TargetNodeId) INCLUDE (SourceNodeId, EdgeType);
CREATE INDEX IX_Edge_Mechanism  ON lineage_v2.Edge(MechanismNodeId) WHERE MechanismNodeId IS NOT NULL;
CREATE INDEX IX_Edge_NotDeleted ON lineage_v2.Edge(IsDeleted) WHERE IsDeleted = 0;

CREATE INDEX IX_NodeHistory_NodeId ON lineage_v2.NodeHistory(NodeId, ChangedAt DESC);
CREATE INDEX IX_NodeHistory_RunId  ON lineage_v2.NodeHistory(ChangedByRunId);
GO
