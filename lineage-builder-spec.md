# Data Lineage Builder — Спецификация решения и постановка задачи для Claude Code

## 1. Контекст и цель

Имеется DWH-решение на стеке Microsoft: SQL Server, SSIS, SSAS Multidimensional, SQL Agent Jobs, плюс кастомный ETL-загрузчик mETL (C#). Необходимо построить **column-level lineage** — сквозной граф зависимостей на уровне отдельных полей, охватывающий всю цепочку от источников до кубов и отчётов.

### Уже сделано (входные данные доступны)
- Извлечены атрибуты кубов SSAS: меры, измерения, named queries из DSV
- Извлечён код всех views, stored procedures, functions из всех используемых баз
- Собраны все SSIS-пакеты (.dtsx)
- Собраны все SQL Agent Jobs (XML/T-SQL определения)
- Есть таблица настроек mETL (маппинг «источник → staging», 1:1, без трансформаций)

### Три ключевых сценария использования
1. **Backward lineage** (от отчёта/куба к источнику): «Из чего и откуда взялось это значение?»
2. **Forward lineage / Impact analysis** (от источника вниз): «На что повлияет изменение этого поля?»
3. **Cross-impact** (из середины в обе стороны): «Каких потребителей затронет изменение этого объекта, и от каких входных данных он зависит?»

---

## 2. Архитектура решения

### 2.1 Общая схема

```
┌─────────────────────────────────────────────────────────┐
│                    EXTRACTORS (C#)                       │
│                                                         │
│  ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌───────────┐  │
│  │ SQL Meta │ │ SSIS     │ │ SSAS     │ │ mETL      │  │
│  │ Extractor│ │ Parser   │ │ Parser   │ │ Config    │  │
│  └────┬─────┘ └────┬─────┘ └────┬─────┘ └─────┬─────┘  │
│       │            │            │              │        │
│       ▼            ▼            ▼              ▼        │
│  ┌─────────────────────────────────────────────────┐    │
│  │         Unified Lineage Model (in-memory)       │    │
│  │    Nodes: columns, tables, processes            │    │
│  │    Edges: dataflow, dependency                  │    │
│  └──────────────────┬──────────────────────────────┘    │
│                     │                                   │
│                     ▼                                   │
│  ┌─────────────────────────────────────────────────┐    │
│  │         Graph Persistence (SQL Server)           │    │
│  │    LineageNode / LineageEdge / LineageRun        │    │
│  └─────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────┘
                      │
                      ▼
         ┌────────────────────────┐
         │   Web UI (Cytoscape.js)│
         │   ASP.NET Core API     │
         └────────────────────────┘
```

### 2.2 SQL-парсер: Microsoft.SqlServer.TransactSql.ScriptDom

Это **критически важный** компонент. NuGet-пакет `Microsoft.SqlServer.TransactSql.ScriptDom` — официальный парсер T-SQL от Microsoft, который строит полное AST.

Ключевые возможности:
- Парсит SELECT, INSERT, UPDATE, MERGE, CTE, подзапросы, оконные функции
- Из AST извлекаются: исходные таблицы/колонки, целевые таблицы/колонки, трансформации (JOIN, WHERE, CASE, агрегации)
- Поддерживает все версии T-SQL диалекта

**Пример подхода к парсингу:**
```csharp
using Microsoft.SqlServer.TransactSql.ScriptDom;

var parser = new TSql160Parser(initialQuotedIdentifiers: false);
IList<ParseError> errors;
var fragment = parser.Parse(new StringReader(sqlText), out errors);

// Реализовать TSqlFragmentVisitor для обхода AST
// и извлечения column-level зависимостей
```

### 2.3 Модель данных графа (таблицы SQL Server)

```sql
-- ============================================================
-- ТАБЛИЦА УЗЛОВ ГРАФА
-- ============================================================
CREATE TABLE lineage.Node (
    NodeId          INT IDENTITY PRIMARY KEY,
    -- Тип узла
    NodeType        VARCHAR(50) NOT NULL,
        -- 'Column', 'Table', 'View', 'StoredProcedure',
        -- 'SsisPackage', 'SsisDataFlow', 'SsisTask',
        -- 'SsasNamedQuery', 'SsasMeasure', 'SsasDimAttribute',
        -- 'SqlAgentJob', 'SqlAgentJobStep',
        -- 'MetlMapping', 'ExternalSource'
    
    -- Полный квалифицированный путь (уникальный идентификатор)
    FullyQualifiedName NVARCHAR(1000) NOT NULL,
        -- Примеры:
        -- [ServerA].[DWH].[dbo].[FactSales].[Amount]
        -- [SSIS].[Package_LoadSales.dtsx].[DFT_LoadFact]
        -- [SSAS].[SalesCube].[DSV].[nq_FactSales].[Revenue]
        -- [mETL].[SourceDB].[dbo].[Orders] -> [Staging].[dbo].[Orders]
    
    -- Человекочитаемое имя
    DisplayName     NVARCHAR(500) NOT NULL,
    
    -- Где лежит исходный код / определение
    SourceLocation  NVARCHAR(1000) NULL,
        -- Путь к .dtsx, имя процедуры, имя view и т.д.
    
    -- К какой системе/слою принадлежит
    LayerName       VARCHAR(100) NULL,
        -- 'Source', 'Staging', 'Core', 'DataMart', 'Cube', 'Report'
    
    -- Дополнительные метаданные (JSON)
    Metadata        NVARCHAR(MAX) NULL,
        -- { "database": "DWH", "schema": "dbo", 
        --   "dataType": "decimal(18,2)", "ssisTaskType": "ExecuteSQL" }
    
    -- Техническое
    CreatedAt       DATETIME2 DEFAULT SYSUTCDATETIME(),
    UpdatedAt       DATETIME2 DEFAULT SYSUTCDATETIME(),
    LastSeenRunId   INT NULL,
    IsDeleted       BIT DEFAULT 0,
    
    CONSTRAINT UQ_Node_FQN UNIQUE (FullyQualifiedName)
);

-- ============================================================
-- ТАБЛИЦА РЁБЕР ГРАФА (ЗАВИСИМОСТЕЙ)
-- ============================================================
CREATE TABLE lineage.Edge (
    EdgeId          INT IDENTITY PRIMARY KEY,
    
    -- Откуда и куда
    SourceNodeId    INT NOT NULL REFERENCES lineage.Node(NodeId),
    TargetNodeId    INT NOT NULL REFERENCES lineage.Node(NodeId),
    
    -- Тип связи
    EdgeType        VARCHAR(50) NOT NULL,
        -- 'DataFlow'          — данные перетекают из source в target
        -- 'Transform'         — данные трансформируются (с описанием)
        -- 'Aggregation'       — агрегация (SUM, COUNT, etc.)
        -- 'Filter'            — фильтрация (WHERE, HAVING)
        -- 'Join'              — участие в JOIN
        -- 'Lookup'            — SSIS Lookup
        -- 'ProcessExecution'  — Job запускает пакет/процедуру
        -- 'DirectCopy'        — mETL 1:1 копирование
    
    -- Какой механизм осуществляет эту связь
    MechanismType   VARCHAR(50) NULL,
        -- 'View', 'StoredProcedure', 'SsisDataFlow',
        -- 'SsisExecuteSQL', 'SsasDsv', 'SqlAgentJob', 'mETL'
    
    -- Где именно лежит код, реализующий эту связь
    MechanismLocation NVARCHAR(1000) NULL,
        -- "[DWH].[dbo].[vw_FactSales]"
        -- "[SSIS].[Package_Load.dtsx].[DFT_Main]"
        -- "[SSAS].[Cube].[DSV].[NamedQuery1]"
    
    -- Описание трансформации (формула, выражение)
    TransformExpression NVARCHAR(MAX) NULL,
        -- "ISNULL(a.Amount, 0) * b.Rate"
        -- "SUM(Amount)"
        -- "1:1 copy"
    
    -- Техническое
    CreatedAt       DATETIME2 DEFAULT SYSUTCDATETIME(),
    LastSeenRunId   INT NULL,
    IsDeleted       BIT DEFAULT 0,
    
    CONSTRAINT UQ_Edge UNIQUE (SourceNodeId, TargetNodeId, MechanismLocation)
);

-- ============================================================
-- ИСТОРИЯ ЗАПУСКОВ
-- ============================================================
CREATE TABLE lineage.Run (
    RunId           INT IDENTITY PRIMARY KEY,
    StartedAt       DATETIME2 NOT NULL,
    CompletedAt     DATETIME2 NULL,
    Status          VARCHAR(20) NOT NULL, -- 'Running','Completed','Failed'
    NodesCreated    INT DEFAULT 0,
    NodesUpdated    INT DEFAULT 0,
    EdgesCreated    INT DEFAULT 0,
    EdgesUpdated    INT DEFAULT 0,
    ErrorLog        NVARCHAR(MAX) NULL
);

-- ============================================================
-- ИНДЕКСЫ ДЛЯ НАВИГАЦИИ ПО ГРАФУ
-- ============================================================
CREATE INDEX IX_Edge_Source ON lineage.Edge(SourceNodeId) INCLUDE (TargetNodeId, EdgeType);
CREATE INDEX IX_Edge_Target ON lineage.Edge(TargetNodeId) INCLUDE (SourceNodeId, EdgeType);
CREATE INDEX IX_Node_Type   ON lineage.Node(NodeType) INCLUDE (FullyQualifiedName);
CREATE INDEX IX_Node_Layer  ON lineage.Node(LayerName) INCLUDE (FullyQualifiedName);
```

---

## 3. Постановка задачи для Claude Code

### Задача: Разработка C# решения "LineageBuilder"

#### 3.1 Структура решения (solution)

```
LineageBuilder.sln
│
├── LineageBuilder.Core/              — модели, интерфейсы, граф в памяти
│   ├── Model/
│   │   ├── LineageGraph.cs           — in-memory граф (nodes + edges)
│   │   ├── LineageNode.cs            — узел графа
│   │   ├── LineageEdge.cs            — ребро графа
│   │   └── Enums.cs                  — NodeType, EdgeType, MechanismType, Layer
│   ├── Interfaces/
│   │   ├── IMetadataExtractor.cs     — интерфейс для всех экстракторов
│   │   └── ISqlLineageParser.cs      — интерфейс парсера SQL
│   └── Extensions/
│       └── GraphExtensions.cs        — Upstream/Downstream traversal
│
├── LineageBuilder.SqlParser/         — парсинг T-SQL через ScriptDom
│   ├── TsqlColumnLineageVisitor.cs   — AST visitor, извлекающий column-level lineage
│   ├── SelectStatementAnalyzer.cs    — анализ SELECT (включая CTE, подзапросы)
│   ├── InsertStatementAnalyzer.cs    — анализ INSERT...SELECT, INSERT...EXEC
│   ├── MergeStatementAnalyzer.cs     — анализ MERGE
│   ├── ProcedureAnalyzer.cs          — анализ тела stored procedure
│   └── ViewAnalyzer.cs               — анализ определения view
│
├── LineageBuilder.Extractors/        — экстракторы метаданных
│   ├── SqlServerExtractor.cs         — views, procedures, functions из sys.sql_modules
│   ├── SsisPackageExtractor.cs       — парсинг .dtsx (XML) файлов
│   ├── SsasExtractor.cs             — парсинг XMLA/DSV кубов SSAS
│   ├── SqlAgentJobExtractor.cs       — парсинг определений job + steps
│   └── MetlExtractor.cs             — чтение таблицы настроек mETL
│
├── LineageBuilder.Persistence/       — сохранение графа в SQL Server
│   ├── LineageRepository.cs          — CRUD для Node/Edge, merge-логика
│   └── Schema/
│       └── create_schema.sql         — DDL для lineage.Node, lineage.Edge, lineage.Run
│
├── LineageBuilder.Api/               — ASP.NET Core Web API для визуализации
│   ├── Controllers/
│   │   ├── LineageController.cs      — upstream/downstream/search endpoints
│   │   └── GraphController.cs        — граф для Cytoscape.js
│   └── wwwroot/                      — SPA с Cytoscape.js
│
├── LineageBuilder.Worker/            — Console app / Windows Service для запуска по расписанию
│   └── Program.cs                    — оркестрация: extract → parse → persist
│
└── LineageBuilder.Tests/
    ├── SqlParser/                    — unit-тесты парсера на реальных SQL-запросах
    └── Extractors/                   — тесты экстракторов
```

#### 3.2 Детальные задачи по компонентам

---

##### ЗАДАЧА 1: SQL Parser (самая сложная и важная часть)

**Цель:** По тексту T-SQL определить, какие колонки из каких таблиц читаются, и в какие колонки каких таблиц записываются, с учётом трансформаций.

**Входные данные:**
- Текст SQL: определение view, тело stored procedure, SQL-запрос из SSIS/SSAS
- Контекст: имя базы данных, схема по умолчанию

**Выходные данные:**
- Список рёбер: `(sourceTable.sourceColumn) → [transform_expression] → (targetTable.targetColumn)`

**Что должен обрабатывать парсер:**

```
Приоритет 1 (обязательно):
├── SELECT ... FROM ... JOIN ...
│   └── маппинг колонок select-list на исходные таблицы
├── INSERT INTO ... SELECT ...
│   └── маппинг target-колонок на source-колонки
├── CREATE/ALTER VIEW AS SELECT ...
│   └── то же что SELECT, но target = колонки view
├── Алиасы таблиц и колонок
├── Подзапросы в FROM (derived tables)
├── CTE (WITH ... AS (...) SELECT ...)
├── CASE WHEN ... THEN ... END
│   └── все ветки CASE → зависимости target-колонки
├── Встроенные функции: ISNULL, COALESCE, CAST, CONVERT, etc.
│   └── аргументы функций → зависимости
├── Агрегатные функции: SUM, COUNT, AVG, MAX, MIN
│   └── тип ребра = 'Aggregation'
├── WHERE / HAVING / JOIN ON
│   └── тип ребра = 'Filter' / 'Join' (влияет на rowset)
└── SELECT *
    └── раскрытие через метаданные таблиц (нужна схема БД)

Приоритет 2 (желательно):
├── MERGE ... USING ... ON ... WHEN MATCHED THEN UPDATE ...
├── UPDATE ... SET ... FROM ... JOIN ...
├── Оконные функции: ROW_NUMBER() OVER (PARTITION BY ... ORDER BY ...)
├── EXEC sp_name @param (вложенные вызовы процедур)
├── Dynamic SQL: EXEC(@sql) — пометить как «непарсируемый узел»
├── CROSS APPLY / OUTER APPLY
└── PIVOT / UNPIVOT

Приоритет 3 (можно отложить):
├── Табличные переменные и временные таблицы
├── Курсоры
├── OPENQUERY / linked servers
└── CLR-процедуры
```

**Подход к реализации:**

```csharp
// Главный класс — реализует TSqlFragmentVisitor
public class ColumnLineageVisitor : TSqlFragmentVisitor
{
    private readonly ISchemaProvider _schema; // Для раскрытия SELECT *
    private readonly LineageGraph _graph;
    private readonly string _contextDatabase;
    
    // Стек контекстов для обработки вложенных запросов
    private readonly Stack<QueryContext> _contextStack = new();
    
    // Точка входа
    public override void Visit(SelectStatement node) { ... }
    public override void Visit(InsertStatement node) { ... }
    public override void Visit(MergeStatement node) { ... }
    public override void Visit(CreateViewStatement node) { ... }
    public override void Visit(CreateProcedureStatement node) { ... }
    
    // Внутренние методы
    private List<ColumnRef> ResolveSelectElements(QuerySpec querySpec) { ... }
    private List<TableRef> ResolveFromClause(FromClause from) { ... }
    private ColumnOrigin TraceColumnOrigin(ColumnRef col, List<TableRef> tables) { ... }
}
```

**Критическое требование:** Парсер ДОЛЖЕН корректно разрешать алиасы таблиц и обрабатывать многоуровневые подзапросы. Это самая частая ошибка в самописных lineage-парсерах.

**Тестирование:** Для парсера нужен обширный набор unit-тестов с реальными SQL-запросами возрастающей сложности: от простого `SELECT a FROM t` до многоэтажных CTE с CASE и подзапросами.

---

##### ЗАДАЧА 2: SSIS Package Extractor

**Цель:** Из .dtsx файлов (XML) извлечь потоки данных: откуда читаются данные, какие трансформации применяются, куда записываются.

**Входные данные:** Директория с .dtsx файлами

**Что извлекать:**

```
Из Control Flow:
├── Execute SQL Task → текст SQL-запроса → передать в SQL Parser
├── Execute Package Task → связь между пакетами
├── For Each Loop / Sequence → группировка задач
└── Выражения в свойствах (Expressions) — могут содержать динамические имена

Из Data Flow:
├── Source компоненты:
│   ├── OLE DB Source → SQL-запрос или имя таблицы + маппинг output-колонок
│   ├── Flat File Source → имя файла + маппинг колонок
│   └── ADO.NET Source → аналогично OLE DB
├── Transformation компоненты:
│   ├── Derived Column → выражения трансформации
│   ├── Lookup → join-таблица + маппинг
│   ├── Conditional Split → условия фильтрации
│   ├── Aggregate → группировки и агрегации
│   ├── Data Conversion → конвертация типов
│   ├── Sort → сортировка (не влияет на column lineage, но важна для полноты)
│   └── Union All / Merge → объединение потоков
├── Destination компоненты:
│   ├── OLE DB Destination → target таблица + маппинг колонок
│   └── Flat File Destination → файл + маппинг колонок
└── Connection Managers → resolved connection strings → имена серверов/баз
```

**Подход:** DTSX — это XML. Парсить через `System.Xml.Linq` (XDocument). Пространство имён SSIS: `www.microsoft.com/SqlServer/Dts`. Data Flow хранится в свойстве `ObjectData` задачи типа `SSIS.Pipeline`.

**Подводные камни:**
- Column mapping в Data Flow хранится как `lineageId` — числовой ID, нужно разрешать в имена колонок через `<output>` → `<outputColumn>` элементы
- SQL-запросы в OLE DB Source могут быть параметризованы (переменные SSIS `?`)
- Expression на Connection Manager может менять имя базы/сервера — пометить как «variable»

---

##### ЗАДАЧА 3: SSAS Extractor

**Цель:** Из определения куба SSAS Multidimensional извлечь:
- Named Queries из DSV → тексты SQL → передать в SQL Parser
- Меры → привязка к fact-таблицам/колонкам + агрегация (Sum, Count, etc.)
- Атрибуты измерений → привязка к колонкам из named queries / таблиц DSV
- Calculated Members (MDX) → зависимости от других мер (парсить MDX сложно, на первом этапе можно извлечь ссылки на [Measures].[...] регулярками)

**Входные данные:** XMLA-файл куба или .dwproj (SSAS Project)

**Формат DSV (внутри XMLA):**
```xml
<DataSourceView>
  <Schema>
    <xs:schema>
      <xs:element name="NamedQuery1">
        <!-- SQL-запрос зашит сюда -->
      </xs:element>
    </xs:schema>
  </Schema>
</DataSourceView>
```

---

##### ЗАДАЧА 4: SQL Agent Job Extractor

**Цель:** Связать «кто запускает что» — job steps, которые вызывают SSIS-пакеты, хранимые процедуры, T-SQL скрипты.

**Входные данные:** `msdb.dbo.sysjobs` + `msdb.dbo.sysjobsteps` или XML-определения

**Что извлекать:**
- Job → Step → тип (SSIS, T-SQL, CmdExec) → target (пакет/процедура)
- Зависимости между steps внутри job (последовательность, on success/on failure)
- Расписание (для информации, не для lineage)

---

##### ЗАДАЧА 5: mETL Extractor

**Цель:** Из таблицы настроек mETL построить рёбра `Source.Table.Column → Staging.Table.Column` типа `DirectCopy`.

**Логика:** Поскольку mETL копирует 1:1 без переименования, для каждой строки настройки нужно:
1. Взять имя таблицы-источника
2. Прочитать список колонок из метаданных источника (sys.columns или из заранее извлечённых данных)
3. Создать рёбра 1:1 в staging

---

##### ЗАДАЧА 6: Persistence — merge-логика

**Цель:** При каждом запуске обновлять граф: добавлять новые узлы/рёбра, обновлять существующие, помечать удалённые.

**Алгоритм:**
```
1. Создать запись в lineage.Run (status = 'Running')
2. Для каждого экстрактора:
   a. Извлечь метаданные → in-memory граф
   b. Для каждого узла: MERGE INTO lineage.Node BY FullyQualifiedName
   c. Для каждого ребра: MERGE INTO lineage.Edge BY (Source, Target, MechanismLocation)
   d. Обновить LastSeenRunId
3. Пометить IsDeleted = 1 для узлов/рёбер, не увиденных в текущем run
4. Обновить lineage.Run (status = 'Completed', статистика)
```

---

##### ЗАДАЧА 7: Web API для навигации

**Endpoints:**

```
GET /api/lineage/search?q=FactSales.Amount
    → поиск узлов по имени (autocomplete)

GET /api/lineage/node/{nodeId}
    → метаданные узла

GET /api/lineage/upstream/{nodeId}?depth=10
    → все upstream-зависимости (рекурсивный обход по рёбрам назад)
    → возвращает subgraph в формате Cytoscape.js JSON

GET /api/lineage/downstream/{nodeId}?depth=10
    → все downstream-зависимости (рекурсивный обход вперёд)

GET /api/lineage/path/{sourceNodeId}/{targetNodeId}
    → кратчайший путь между двумя узлами

GET /api/lineage/impact/{nodeId}
    → upstream + downstream — полная картина влияния

GET /api/lineage/layer/{layerName}
    → все узлы определённого слоя (Source, Staging, Core, etc.)

GET /api/lineage/mechanisms
    → статистика: сколько рёбер по каждому типу механизма
```

**Формат ответа для Cytoscape.js:**
```json
{
  "elements": {
    "nodes": [
      { "data": { "id": "n1", "label": "FactSales.Amount", 
                   "nodeType": "Column", "layer": "Core",
                   "sourceLocation": "[DWH].[dbo].[FactSales]" } }
    ],
    "edges": [
      { "data": { "source": "n1", "target": "n2", 
                   "edgeType": "Transform",
                   "mechanism": "StoredProcedure",
                   "transform": "SUM(Amount)" } }
    ]
  }
}
```

---

## 4. Визуализация: Cytoscape.js

### Почему Cytoscape.js
- Open source, бесплатный, зрелый (500+ научных публикаций)
- Специализирован на визуализации графов (не generic chart library)
- Встроенные алгоритмы layout: dagre (иерархический, идеален для lineage), breadthfirst, klay
- Фильтрация, поиск, zoom, pan, подсветка путей — из коробки
- Расширения: `cytoscape-dagre`, `cytoscape-popper` (тултипы), `cytoscape-navigator` (minimap)
- Работает в браузере, не требует установки

### Ключевые фичи UI

```
1. Поиск: текстовое поле с autocomplete по именам узлов
2. Клик по узлу → подсветить upstream (синий) + downstream (оранжевый)
3. Панель деталей: при выборе узла показать метаданные, 
   source location (ссылка на код), тип механизма
4. Фильтры: по слою (Source/Staging/Core/Mart/Cube), 
   по типу механизма (SSIS/View/SP/mETL)
5. Группировка: узлы одной таблицы группируются в compound node
6. Уровень детализации: 
   - Table-level (свёрнуто) → клик → Column-level (развёрнуто)
7. Экспорт: PNG/SVG для документации
```

### Альтернативы визуализации

| Инструмент | Плюсы | Минусы |
|---|---|---|
| **Cytoscape.js** | Open source, специализирован на графах, dagre layout | Нужно писать UI |
| **Neo4j + Bloom** | Мощный графовый движок, красивый UI | Отдельная СУБД, лицензия Bloom |
| **D3.js** | Максимальная гибкость | Нужно писать ВСЁ с нуля |
| **vis.js** | Простой API | Хуже с большими графами |
| **yFiles** | Enterprise-grade, лучшие layouts | Дорогая коммерческая лицензия |
| **React Flow** | Современный, React-native | Больше для flow-editors, чем для lineage |

**Рекомендация:** Cytoscape.js + dagre layout — оптимальный баланс возможностей и трудозатрат.

---

## 5. Порядок разработки (roadmap)

### Этап 1: Ядро парсера (2-3 недели)
- [ ] Модель данных: LineageNode, LineageEdge, LineageGraph
- [ ] SQL Parser для views (самый чистый SQL, удобно тестировать)
- [ ] Unit-тесты парсера на 20+ реальных views из вашей БД
- [ ] Persistence: создание схемы, MERGE-логика

### Этап 2: Экстракторы (2-3 недели)
- [ ] SQL Server Extractor (procedures, functions)
- [ ] mETL Extractor (самый простой)
- [ ] SSIS Package Extractor (Data Flow маппинги)
- [ ] SQL Agent Job Extractor

### Этап 3: SSAS + визуализация (2 недели)
- [ ] SSAS Extractor (DSV named queries, меры, измерения)
- [ ] Web API (ASP.NET Core)
- [ ] UI на Cytoscape.js (базовый: поиск + upstream/downstream)

### Этап 4: Полировка (1-2 недели)
- [ ] Worker для запуска по расписанию
- [ ] Обработка edge cases парсера (dynamic SQL, EXEC, etc.)
- [ ] Compound nodes (группировка колонок в таблицы)
- [ ] Фильтры и экспорт в UI

---

## 6. Промпт для Claude Code

Ниже — готовый промпт, который можно дать Claude Code для начала работы над первым этапом.

---

```
Разработай C# решение LineageBuilder для построения column-level data lineage.

КОНТЕКСТ:
У меня есть DWH на SQL Server с ETL через SSIS, хранимые процедуры, 
views, кубы SSAS и кастомный загрузчик mETL. Нужно построить граф 
зависимостей на уровне колонок, сохранить его в таблицы SQL Server 
и визуализировать через веб-интерфейс.

ПЕРВЫЙ ЭТАП — SQL PARSER + МОДЕЛЬ + PERSISTENCE:

1. Создай .NET 8 solution со следующей структурой проектов:
   - LineageBuilder.Core (модели, интерфейсы)
   - LineageBuilder.SqlParser (парсинг T-SQL)
   - LineageBuilder.Persistence (сохранение в SQL Server)
   - LineageBuilder.Tests (xUnit)

2. В LineageBuilder.Core создай:
   - LineageNode (Id, NodeType enum, FullyQualifiedName, DisplayName, 
     SourceLocation, LayerName, Metadata dict)
   - LineageEdge (Id, SourceNode, TargetNode, EdgeType enum, 
     MechanismType enum, MechanismLocation, TransformExpression)
   - LineageGraph — in-memory граф с методами AddNode, AddEdge, 
     GetUpstream(nodeId, depth), GetDownstream(nodeId, depth), 
     FindPath(fromId, toId)
   - Enums: NodeType, EdgeType, MechanismType, LayerName

3. В LineageBuilder.SqlParser:
   - Используй NuGet-пакет Microsoft.SqlServer.TransactSql.ScriptDom
   - Реализуй ColumnLineageVisitor : TSqlFragmentVisitor, который:
     a) Обходит SelectStatement, извлекает source таблицы/колонки 
        из FROM/JOIN, маппит на target колонки через select list
     b) Обрабатывает InsertStatement (INSERT...SELECT), 
        маппит source-колонки на target-колонки INSERT
     c) Обрабатывает CreateViewStatement — target = колонки view
     d) Обрабатывает CreateProcedureStatement — анализирует тело
     e) Корректно разрешает алиасы таблиц (a.Column → RealTable.Column)
     f) Обрабатывает CTE (WITH ... AS)
     g) Обрабатывает подзапросы в FROM
     h) Для SELECT * использует ISchemaProvider для раскрытия в колонки
     i) Функции (ISNULL, COALESCE, CASE) → зависимость от всех аргументов
     j) Агрегатные функции → EdgeType.Aggregation
     k) WHERE/JOIN ON → EdgeType.Filter / EdgeType.Join

   - Реализуй ISchemaProvider с in-memory реализацией 
     (Dictionary<tableFQN, List<columnName>>), 
     загружаемой из sys.columns / INFORMATION_SCHEMA

4. В LineageBuilder.Persistence:
   - SQL DDL скрипт для создания lineage.Node, lineage.Edge, lineage.Run
   - LineageRepository с методами:
     - MergeNodes(IEnumerable<LineageNode>) — MERGE по FullyQualifiedName
     - MergeEdges(IEnumerable<LineageEdge>) — MERGE по (Source, Target, Mechanism)
     - MarkDeletedNodes(runId) — IsDeleted для не увиденных в текущем run
     - GetUpstreamGraph(nodeId, depth) — рекурсивный CTE по Edge
     - GetDownstreamGraph(nodeId, depth) — аналогично

5. В LineageBuilder.Tests:
   - Минимум 15 unit-тестов для SqlParser:
     a) Простой SELECT с JOIN
     b) SELECT с алиасами
     c) INSERT INTO ... SELECT
     d) View с CTE
     e) View с подзапросом в FROM
     f) SELECT * (с mock ISchemaProvider)
     g) CASE WHEN
     h) Агрегатные функции (SUM, COUNT)
     i) ISNULL, COALESCE
     j) Вложенные подзапросы (3 уровня)
     k) UNION ALL
     l) Stored procedure с INSERT...SELECT
     m) WHERE clause (filter dependency)
     n) JOIN ON (join dependency)
     o) Multiple CTEs referencing each other

ТЕХНИЧЕСКИЕ ТРЕБОВАНИЯ:
- .NET 8, C# 12
- NuGet: Microsoft.SqlServer.TransactSql.ScriptDom, 
  Microsoft.Data.SqlClient, Dapper, xUnit
- Все классы с XML-doc комментариями
- Async/await где применимо
- Логирование через ILogger<T>
- Конфигурация через appsettings.json + IOptions pattern
```

---

## 7. Рекомендации по эксплуатации

**Запуск:** Настроить SQL Agent Job или Windows Task Scheduler, который запускает LineageBuilder.Worker раз в сутки (или после каждого деплоя ETL-пакетов).

**Мониторинг:** Таблица lineage.Run — если status = 'Failed', смотреть ErrorLog. Если NodesCreated резко вырос или упал — что-то изменилось в структуре.

**Постепенное покрытие:** Начать с views (чистый SQL, легко парсить), потом добавить хранимые процедуры, потом SSIS, потом SSAS. Каждый этап даёт всё больше полноты графа.

**Валидация:** Взять 5-10 известных цепочек данных (от конкретного поля отчёта до источника) и проверить, что lineage builder построил их корректно. Это лучший способ найти баги парсера.
