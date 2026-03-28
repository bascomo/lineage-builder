using LineageBuilder.Core.Model;
using LineageBuilder.SqlParser;
using LineageBuilder.SqlParser.Schema;

namespace LineageBuilder.Tests.SqlParser;

public class TsqlLineageParserTests
{
    private readonly TsqlLineageParser _parser;
    private readonly InMemorySchemaProvider _schema;

    public TsqlLineageParserTests()
    {
        _schema = new InMemorySchemaProvider();
        _parser = new TsqlLineageParser(_schema);
    }

    // ==================== Test 1: Simple SELECT with JOIN ====================
    [Fact]
    public void SimpleSelectWithJoin_ExtractsColumnLineage()
    {
        var sql = @"
            SELECT a.OrderId, a.Amount, b.CustomerName
            FROM dbo.Orders a
            INNER JOIN dbo.Customers b ON a.CustomerId = b.CustomerId";

        var result = _parser.Parse(sql);

        Assert.False(result.HasErrors, string.Join("; ", result.Errors));
        Assert.Contains(result.Entries, e =>
            e.SourceTable == "dbo.Orders" && e.SourceColumn == "OrderId");
        Assert.Contains(result.Entries, e =>
            e.SourceTable == "dbo.Orders" && e.SourceColumn == "Amount");
        Assert.Contains(result.Entries, e =>
            e.SourceTable == "dbo.Customers" && e.SourceColumn == "CustomerName");
    }

    // ==================== Test 2: SELECT with column aliases ====================
    [Fact]
    public void SelectWithAliases_ResolvesCorrectly()
    {
        var sql = @"
            SELECT a.OrderId AS Id, a.Amount AS Total
            FROM dbo.Orders a";

        var result = _parser.Parse(sql);

        Assert.False(result.HasErrors);
        Assert.Contains(result.Entries, e =>
            e.TargetColumn == "Id" && e.SourceColumn == "OrderId");
        Assert.Contains(result.Entries, e =>
            e.TargetColumn == "Total" && e.SourceColumn == "Amount");
    }

    // ==================== Test 3: INSERT INTO ... SELECT ====================
    [Fact]
    public void InsertSelect_MapsColumnsToTarget()
    {
        var sql = @"
            INSERT INTO dbo.OrderArchive (OrderId, Amount, ArchiveDate)
            SELECT OrderId, Amount, GETDATE()
            FROM dbo.Orders";

        var result = _parser.Parse(sql);

        Assert.False(result.HasErrors);
        Assert.Contains(result.Entries, e =>
            e.TargetTable == "dbo.OrderArchive" && e.TargetColumn == "OrderId" &&
            e.SourceTable == "dbo.Orders" && e.SourceColumn == "OrderId");
        Assert.Contains(result.Entries, e =>
            e.TargetTable == "dbo.OrderArchive" && e.TargetColumn == "Amount" &&
            e.SourceColumn == "Amount");
    }

    // ==================== Test 4: CREATE VIEW ====================
    [Fact]
    public void CreateView_MapsColumnsToViewName()
    {
        var sql = @"
            CREATE VIEW dbo.vw_ActiveOrders AS
            SELECT o.OrderId, o.Amount, c.CustomerName
            FROM dbo.Orders o
            JOIN dbo.Customers c ON o.CustomerId = c.CustomerId
            WHERE o.IsActive = 1";

        var result = _parser.Parse(sql);

        Assert.False(result.HasErrors);
        Assert.Contains(result.Entries, e =>
            e.TargetTable == "dbo.vw_ActiveOrders" && e.TargetColumn == "OrderId");
        Assert.Contains(result.Entries, e =>
            e.TargetTable == "dbo.vw_ActiveOrders" && e.TargetColumn == "CustomerName" &&
            e.SourceTable == "dbo.Customers");
    }

    // ==================== Test 5: CTE ====================
    [Fact]
    public void CteQuery_TracesColumnsThroughCte()
    {
        var sql = @"
            ;WITH OrderTotals AS (
                SELECT CustomerId, SUM(Amount) AS TotalAmount
                FROM dbo.Orders
                GROUP BY CustomerId
            )
            SELECT c.CustomerName, ot.TotalAmount
            FROM dbo.Customers c
            JOIN OrderTotals ot ON c.CustomerId = ot.CustomerId";

        var result = _parser.Parse(sql);

        Assert.False(result.HasErrors);
        Assert.Contains(result.Entries, e =>
            e.SourceColumn == "CustomerName" && e.SourceTable == "dbo.Customers");
        // CTE TotalAmount traces back to dbo.Orders.Amount
        Assert.Contains(result.Entries, e =>
            e.TargetColumn == "TotalAmount" && e.SourceTable == "dbo.Orders" && e.SourceColumn == "Amount");
    }

    // ==================== Test 6: Subquery in FROM (derived table) ====================
    [Fact]
    public void DerivedTable_TracesColumnsThrough()
    {
        var sql = @"
            SELECT sub.OrderId, sub.Total
            FROM (
                SELECT OrderId, Amount AS Total
                FROM dbo.Orders
            ) sub";

        var result = _parser.Parse(sql);

        Assert.False(result.HasErrors);
        Assert.Contains(result.Entries, e =>
            e.TargetColumn == "OrderId" && e.SourceTable == "dbo.Orders" && e.SourceColumn == "OrderId");
        Assert.Contains(result.Entries, e =>
            e.TargetColumn == "Total" && e.SourceTable == "dbo.Orders" && e.SourceColumn == "Amount");
    }

    // ==================== Test 7: SELECT * with schema provider ====================
    [Fact]
    public void SelectStar_ExpandsViaSchemaProvider()
    {
        _schema.RegisterTable("", "dbo", "Orders",
            new[] { "OrderId", "CustomerId", "Amount", "OrderDate" });

        var sql = "SELECT * FROM dbo.Orders";

        var result = _parser.Parse(sql);

        Assert.False(result.HasErrors);
        Assert.Equal(4, result.Entries.Count);
        Assert.Contains(result.Entries, e => e.SourceColumn == "OrderId");
        Assert.Contains(result.Entries, e => e.SourceColumn == "Amount");
        Assert.Contains(result.Entries, e => e.SourceColumn == "OrderDate");
    }

    // ==================== Test 8: CASE WHEN ====================
    [Fact]
    public void CaseWhen_ExtractsDependenciesFromAllBranches()
    {
        var sql = @"
            SELECT OrderId,
                CASE
                    WHEN Status = 1 THEN Amount
                    WHEN Status = 2 THEN Amount * Discount
                    ELSE 0
                END AS FinalAmount
            FROM dbo.Orders";

        var result = _parser.Parse(sql);

        Assert.False(result.HasErrors);
        // FinalAmount depends on Status, Amount, and Discount
        var finalAmountSources = result.Entries
            .Where(e => e.TargetColumn == "FinalAmount")
            .Select(e => e.SourceColumn)
            .ToList();
        Assert.Contains("Status", finalAmountSources);
        Assert.Contains("Amount", finalAmountSources);
        Assert.Contains("Discount", finalAmountSources);
    }

    // ==================== Test 9: Aggregate functions ====================
    [Fact]
    public void AggregateFunctions_MarkedAsAggregation()
    {
        var sql = @"
            SELECT CustomerId, SUM(Amount) AS TotalAmount, COUNT(*) AS OrderCount
            FROM dbo.Orders
            GROUP BY CustomerId";

        var result = _parser.Parse(sql);

        Assert.False(result.HasErrors);
        Assert.Contains(result.Entries, e =>
            e.TargetColumn == "TotalAmount" && e.EdgeType == EdgeTypes.Aggregation);
    }

    // ==================== Test 10: ISNULL / COALESCE ====================
    [Fact]
    public void IsNullAndCoalesce_ExtractsAllArguments()
    {
        var sql = @"
            SELECT
                ISNULL(a.Amount, 0) AS SafeAmount,
                COALESCE(a.Discount, b.DefaultDiscount, 0) AS FinalDiscount
            FROM dbo.Orders a
            LEFT JOIN dbo.Settings b ON 1=1";

        var result = _parser.Parse(sql);

        Assert.False(result.HasErrors);
        Assert.Contains(result.Entries, e =>
            e.TargetColumn == "SafeAmount" && e.SourceColumn == "Amount");
        Assert.Contains(result.Entries, e =>
            e.TargetColumn == "FinalDiscount" && e.SourceColumn == "Discount");
        Assert.Contains(result.Entries, e =>
            e.TargetColumn == "FinalDiscount" && e.SourceColumn == "DefaultDiscount");
    }

    // ==================== Test 11: UNION ALL ====================
    [Fact]
    public void UnionAll_MergesSourcesFromBothBranches()
    {
        var sql = @"
            SELECT OrderId, Amount FROM dbo.Orders
            UNION ALL
            SELECT ReturnId, ReturnAmount FROM dbo.Returns";

        var result = _parser.Parse(sql);

        Assert.False(result.HasErrors);
        Assert.Contains(result.Entries, e =>
            e.SourceTable == "dbo.Orders" && e.SourceColumn == "OrderId");
        Assert.Contains(result.Entries, e =>
            e.SourceTable == "dbo.Returns" && e.SourceColumn == "ReturnId");
    }

    // ==================== Test 12: Multiple CTEs referencing each other ====================
    [Fact]
    public void MultipleCtes_ResolveReferencesCorrectly()
    {
        var sql = @"
            ;WITH cte1 AS (
                SELECT CustomerId, SUM(Amount) AS Total
                FROM dbo.Orders
                GROUP BY CustomerId
            ),
            cte2 AS (
                SELECT c.CustomerName, c1.Total
                FROM dbo.Customers c
                JOIN cte1 c1 ON c.CustomerId = c1.CustomerId
            )
            SELECT CustomerName, Total FROM cte2";

        var result = _parser.Parse(sql);

        Assert.False(result.HasErrors);
        // Total should trace back to dbo.Orders.Amount through cte1
        Assert.Contains(result.Entries, e =>
            e.TargetColumn == "Total" && e.SourceTable == "dbo.Orders" && e.SourceColumn == "Amount");
        Assert.Contains(result.Entries, e =>
            e.TargetColumn == "CustomerName" && e.SourceTable == "dbo.Customers");
    }

    // ==================== Test 13: CAST/CONVERT ====================
    [Fact]
    public void CastAndConvert_TracesThroughToSource()
    {
        var sql = @"
            SELECT
                CAST(Amount AS DECIMAL(18,2)) AS DecimalAmount,
                CONVERT(VARCHAR(10), OrderDate, 120) AS FormattedDate
            FROM dbo.Orders";

        var result = _parser.Parse(sql);

        Assert.False(result.HasErrors);
        Assert.Contains(result.Entries, e =>
            e.TargetColumn == "DecimalAmount" && e.SourceColumn == "Amount");
        Assert.Contains(result.Entries, e =>
            e.TargetColumn == "FormattedDate" && e.SourceColumn == "OrderDate");
    }

    // ==================== Test 14: Binary expression ====================
    [Fact]
    public void BinaryExpression_ExtractsBothOperands()
    {
        var sql = @"
            SELECT OrderId, Amount * Quantity AS LineTotal
            FROM dbo.OrderLines";

        var result = _parser.Parse(sql);

        Assert.False(result.HasErrors);
        var lineTotalSources = result.Entries
            .Where(e => e.TargetColumn == "LineTotal")
            .Select(e => e.SourceColumn)
            .ToList();
        Assert.Contains("Amount", lineTotalSources);
        Assert.Contains("Quantity", lineTotalSources);
    }

    // ==================== Test 15: Nested subquery (3 levels) ====================
    [Fact]
    public void NestedSubqueries_TracesToUltimateSource()
    {
        var sql = @"
            SELECT x.OrderId, x.Total
            FROM (
                SELECT y.OrderId, y.Total
                FROM (
                    SELECT OrderId, Amount AS Total
                    FROM dbo.Orders
                ) y
            ) x";

        var result = _parser.Parse(sql);

        Assert.False(result.HasErrors);
        Assert.Contains(result.Entries, e =>
            e.TargetColumn == "Total" && e.SourceTable == "dbo.Orders" && e.SourceColumn == "Amount");
    }

    // ==================== Test 16: UPDATE ... SET ... FROM ... JOIN ====================
    [Fact]
    public void UpdateFromJoin_ExtractsLineage()
    {
        var sql = @"
            UPDATE t
            SET t.Status = s.NewStatus,
                t.ModifiedDate = GETDATE()
            FROM dbo.Orders t
            INNER JOIN dbo.StatusUpdates s ON t.OrderId = s.OrderId";

        var result = _parser.Parse(sql);

        Assert.False(result.HasErrors);
        Assert.Contains(result.Entries, e =>
            e.TargetTable == "t" && e.TargetColumn == "Status" &&
            e.SourceTable == "dbo.StatusUpdates" && e.SourceColumn == "NewStatus");
    }

    // ==================== Test 17: MERGE statement ====================
    [Fact]
    public void MergeStatement_ExtractsLineageFromBothActions()
    {
        var sql = @"
            MERGE INTO dbo.TargetTable AS tgt
            USING dbo.SourceTable AS src ON tgt.Id = src.Id
            WHEN MATCHED THEN
                UPDATE SET tgt.Name = src.Name, tgt.Amount = src.Amount
            WHEN NOT MATCHED THEN
                INSERT (Id, Name, Amount) VALUES (src.Id, src.Name, src.Amount);";

        var result = _parser.Parse(sql);

        Assert.False(result.HasErrors);
        // UPDATE part
        Assert.Contains(result.Entries, e =>
            e.TargetTable == "dbo.TargetTable" && e.TargetColumn == "Name" &&
            e.SourceColumn == "Name");
        // INSERT part
        Assert.Contains(result.Entries, e =>
            e.TargetTable == "dbo.TargetTable" && e.TargetColumn == "Id" &&
            e.SourceColumn == "Id");
    }

    // ==================== Test 18: CREATE PROCEDURE with INSERT...SELECT ====================
    [Fact]
    public void CreateProcedure_ExtractsLineageFromBody()
    {
        var sql = @"
            CREATE PROCEDURE dbo.sp_ArchiveOrders
            AS
            BEGIN
                INSERT INTO dbo.OrderArchive (OrderId, Amount)
                SELECT OrderId, Amount
                FROM dbo.Orders
                WHERE OrderDate < '2020-01-01'
            END";

        var result = _parser.Parse(sql);

        Assert.False(result.HasErrors);
        Assert.Contains(result.Entries, e =>
            e.TargetTable == "dbo.OrderArchive" && e.TargetColumn == "OrderId" &&
            e.SourceTable == "dbo.Orders" && e.SourceColumn == "OrderId");
    }

    // ==================== Test 19: Procedure with IF branches ====================
    [Fact]
    public void ProcedureWithIf_ExtractsFromBothBranches()
    {
        var sql = @"
            CREATE PROCEDURE dbo.sp_Conditional
            AS
            BEGIN
                IF 1 = 1
                BEGIN
                    INSERT INTO dbo.Target1 (Col1) SELECT SourceCol1 FROM dbo.Source1
                END
                ELSE
                BEGIN
                    INSERT INTO dbo.Target2 (Col2) SELECT SourceCol2 FROM dbo.Source2
                END
            END";

        var result = _parser.Parse(sql);

        Assert.False(result.HasErrors);
        Assert.Contains(result.Entries, e =>
            e.TargetTable == "dbo.Target1" && e.SourceTable == "dbo.Source1");
        Assert.Contains(result.Entries, e =>
            e.TargetTable == "dbo.Target2" && e.SourceTable == "dbo.Source2");
    }

    // ==================== Test 20: Window function ROW_NUMBER ====================
    [Fact]
    public void WindowFunction_ExtractsPartitionAndOrderColumns()
    {
        var sql = @"
            SELECT OrderId,
                ROW_NUMBER() OVER (PARTITION BY CustomerId ORDER BY OrderDate DESC) AS RowNum
            FROM dbo.Orders";

        var result = _parser.Parse(sql);

        Assert.False(result.HasErrors);
        var rowNumSources = result.Entries
            .Where(e => e.TargetColumn == "RowNum")
            .Select(e => e.SourceColumn)
            .ToList();
        Assert.Contains("CustomerId", rowNumSources);
        Assert.Contains("OrderDate", rowNumSources);
    }

    // ==================== Test 21: View with CTE ====================
    [Fact]
    public void ViewWithCte_TracesCorrectly()
    {
        var sql = @"
            CREATE VIEW dbo.vw_TopCustomers AS
            WITH CustomerTotals AS (
                SELECT CustomerId, SUM(Amount) AS TotalSpent
                FROM dbo.Orders
                GROUP BY CustomerId
            )
            SELECT c.CustomerName, ct.TotalSpent
            FROM dbo.Customers c
            JOIN CustomerTotals ct ON c.CustomerId = ct.CustomerId
            WHERE ct.TotalSpent > 1000";

        var result = _parser.Parse(sql);

        Assert.False(result.HasErrors);
        Assert.Contains(result.Entries, e =>
            e.TargetTable == "dbo.vw_TopCustomers" && e.TargetColumn == "TotalSpent" &&
            e.SourceTable == "dbo.Orders" && e.SourceColumn == "Amount");
        Assert.Contains(result.Entries, e =>
            e.TargetTable == "dbo.vw_TopCustomers" && e.TargetColumn == "CustomerName" &&
            e.SourceTable == "dbo.Customers");
    }

    // ==================== Test 22: LEFT JOIN ====================
    [Fact]
    public void LeftJoin_ExtractsColumnsFromBothSides()
    {
        var sql = @"
            SELECT o.OrderId, o.Amount, ISNULL(d.DiscountPct, 0) AS Discount
            FROM dbo.Orders o
            LEFT JOIN dbo.Discounts d ON o.DiscountId = d.DiscountId";

        var result = _parser.Parse(sql);

        Assert.False(result.HasErrors);
        Assert.Contains(result.Entries, e =>
            e.TargetColumn == "Discount" && e.SourceColumn == "DiscountPct" &&
            e.SourceTable == "dbo.Discounts");
    }

    // ==================== Test 23: Multiple JOINs ====================
    [Fact]
    public void MultipleJoins_ResolvesAllTables()
    {
        var sql = @"
            SELECT o.OrderId, c.CustomerName, p.ProductName, ol.Quantity
            FROM dbo.Orders o
            JOIN dbo.Customers c ON o.CustomerId = c.CustomerId
            JOIN dbo.OrderLines ol ON o.OrderId = ol.OrderId
            JOIN dbo.Products p ON ol.ProductId = p.ProductId";

        var result = _parser.Parse(sql);

        Assert.False(result.HasErrors);
        Assert.Contains(result.Entries, e =>
            e.SourceTable == "dbo.Orders" && e.SourceColumn == "OrderId");
        Assert.Contains(result.Entries, e =>
            e.SourceTable == "dbo.Customers" && e.SourceColumn == "CustomerName");
        Assert.Contains(result.Entries, e =>
            e.SourceTable == "dbo.Products" && e.SourceColumn == "ProductName");
        Assert.Contains(result.Entries, e =>
            e.SourceTable == "dbo.OrderLines" && e.SourceColumn == "Quantity");
    }

    // ==================== Test 24: IIF function ====================
    [Fact]
    public void IifFunction_ExtractsAllBranches()
    {
        var sql = @"
            SELECT OrderId,
                IIF(Amount > 100, HighRate, LowRate) AS AppliedRate
            FROM dbo.Orders";

        var result = _parser.Parse(sql);

        Assert.False(result.HasErrors);
        var sources = result.Entries
            .Where(e => e.TargetColumn == "AppliedRate")
            .Select(e => e.SourceColumn)
            .ToList();
        Assert.Contains("Amount", sources);
        Assert.Contains("HighRate", sources);
        Assert.Contains("LowRate", sources);
    }

    // ==================== Test 25: Simple CASE ====================
    [Fact]
    public void SimpleCase_ExtractsInputAndBranches()
    {
        var sql = @"
            SELECT OrderId,
                CASE Status
                    WHEN 1 THEN Amount
                    WHEN 2 THEN Amount * 0.5
                    ELSE 0
                END AS AdjustedAmount
            FROM dbo.Orders";

        var result = _parser.Parse(sql);

        Assert.False(result.HasErrors);
        var sources = result.Entries
            .Where(e => e.TargetColumn == "AdjustedAmount")
            .Select(e => e.SourceColumn)
            .ToList();
        Assert.Contains("Status", sources);
        Assert.Contains("Amount", sources);
    }

    // ==================== Test 26: NullIf ====================
    [Fact]
    public void NullIf_ExtractsBothArgs()
    {
        var sql = @"
            SELECT OrderId, NULLIF(Amount, 0) AS NonZeroAmount
            FROM dbo.Orders";

        var result = _parser.Parse(sql);

        Assert.False(result.HasErrors);
        Assert.Contains(result.Entries, e =>
            e.TargetColumn == "NonZeroAmount" && e.SourceColumn == "Amount");
    }

    // ==================== Test 27: Scalar subquery in SELECT ====================
    [Fact]
    public void ScalarSubquery_ExtractsInnerSources()
    {
        var sql = @"
            SELECT o.OrderId,
                (SELECT MAX(p.Price) FROM dbo.Products p WHERE p.ProductId = o.ProductId) AS MaxPrice
            FROM dbo.Orders o";

        var result = _parser.Parse(sql);

        Assert.False(result.HasErrors);
        Assert.Contains(result.Entries, e =>
            e.TargetColumn == "MaxPrice" && e.SourceColumn == "Price" && e.SourceTable == "dbo.Products");
    }
}
