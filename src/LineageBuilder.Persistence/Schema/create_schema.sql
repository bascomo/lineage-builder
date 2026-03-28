-- ============================================================
-- LineageBuilder: Schema for graph persistence
-- Target: DWH-VDI.MetaMart.lineage2 (new schema, coexists with legacy lineage)
-- ============================================================

IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'lineage2')
    EXEC('CREATE SCHEMA lineage2');
GO

-- ============================================================
-- ТАБЛИЦА УЗЛОВ ГРАФА
-- ============================================================
DROP TABLE IF EXISTS lineage2.Edge;
DROP TABLE IF EXISTS lineage2.Node;
DROP TABLE IF EXISTS lineage2.Run;
GO

CREATE TABLE lineage2.Node (
    NodeId              INT IDENTITY PRIMARY KEY,
    NodeType            VARCHAR(50)     NOT NULL,
    FullyQualifiedName  NVARCHAR(1000)  NOT NULL,
    DisplayName         NVARCHAR(500)   NOT NULL,
    SourceLocation      NVARCHAR(1000)  NULL,
    LayerName           VARCHAR(100)    NULL,
    ParentNodeId        INT             NULL,
    Metadata            NVARCHAR(MAX)   NULL,  -- JSON
    CreatedAt           DATETIME2       DEFAULT SYSUTCDATETIME(),
    UpdatedAt           DATETIME2       DEFAULT SYSUTCDATETIME(),
    LastSeenRunId       INT             NULL,
    IsDeleted           BIT             DEFAULT 0,

    CONSTRAINT UQ_Node_FQN UNIQUE (FullyQualifiedName)
);
GO

-- ============================================================
-- ТАБЛИЦА РЁБЕР ГРАФА
-- ============================================================
CREATE TABLE lineage2.Edge (
    EdgeId              INT IDENTITY PRIMARY KEY,
    SourceNodeId        INT NOT NULL REFERENCES lineage2.Node(NodeId),
    TargetNodeId        INT NOT NULL REFERENCES lineage2.Node(NodeId),
    EdgeType            VARCHAR(50)     NOT NULL,
    MechanismType       VARCHAR(50)     NULL,
    MechanismLocation   NVARCHAR(1000)  NULL,
    TransformExpression NVARCHAR(MAX)   NULL,
    CreatedAt           DATETIME2       DEFAULT SYSUTCDATETIME(),
    LastSeenRunId       INT             NULL,
    IsDeleted           BIT             DEFAULT 0,

    CONSTRAINT UQ_Edge UNIQUE (SourceNodeId, TargetNodeId, EdgeType, MechanismLocation)
);
GO

-- ============================================================
-- ИСТОРИЯ ЗАПУСКОВ
-- ============================================================
CREATE TABLE lineage2.Run (
    RunId           INT IDENTITY PRIMARY KEY,
    StartedAt       DATETIME2       NOT NULL DEFAULT SYSUTCDATETIME(),
    CompletedAt     DATETIME2       NULL,
    Status          VARCHAR(20)     NOT NULL DEFAULT 'Running',
    NodesCreated    INT             DEFAULT 0,
    NodesUpdated    INT             DEFAULT 0,
    EdgesCreated    INT             DEFAULT 0,
    EdgesUpdated    INT             DEFAULT 0,
    ErrorLog        NVARCHAR(MAX)   NULL
);
GO

-- ============================================================
-- ИНДЕКСЫ
-- ============================================================
CREATE INDEX IX_Edge_Source ON lineage2.Edge(SourceNodeId) INCLUDE (TargetNodeId, EdgeType);
CREATE INDEX IX_Edge_Target ON lineage2.Edge(TargetNodeId) INCLUDE (SourceNodeId, EdgeType);
CREATE INDEX IX_Node_Type   ON lineage2.Node(NodeType) INCLUDE (FullyQualifiedName);
CREATE INDEX IX_Node_Layer  ON lineage2.Node(LayerName) INCLUDE (FullyQualifiedName);
CREATE INDEX IX_Node_Name   ON lineage2.Node(DisplayName) INCLUDE (FullyQualifiedName, NodeType);
GO
