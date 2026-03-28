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
            e.TargetColumn == "TotalAmount" && e.EdgeType == EdgeType.Aggregation);
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
}
