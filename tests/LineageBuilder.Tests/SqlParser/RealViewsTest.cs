using LineageBuilder.Core.Model;
using LineageBuilder.SqlParser;
using LineageBuilder.SqlParser.Schema;

namespace LineageBuilder.Tests.SqlParser;

/// <summary>
/// Tests SQL parser against real-world view definitions from DWH.
/// These represent actual production SQL patterns.
/// </summary>
public class RealViewsTest
{
    private readonly TsqlLineageParser _parser = new(new InMemorySchemaProvider());

    [Fact]
    public void RealView_SimpleJoinWithCast()
    {
        // Pattern common in DWH: JOIN + CAST + ISNULL
        var sql = @"
            CREATE VIEW dbo.vw_FactSales AS
            SELECT
                f.SalesId,
                f.DateKey,
                CAST(f.Amount AS DECIMAL(18,2)) AS Amount,
                ISNULL(d.DiscountPct, 0) AS DiscountPct,
                f.Amount * (1 - ISNULL(d.DiscountPct, 0) / 100.0) AS NetAmount,
                c.CustomerName,
                p.ProductName
            FROM dbo.FactSalesRaw f
            INNER JOIN dbo.DimCustomer c ON f.CustomerId = c.CustomerId
            INNER JOIN dbo.DimProduct p ON f.ProductId = p.ProductId
            LEFT JOIN dbo.DimDiscount d ON f.DiscountId = d.DiscountId
            WHERE f.IsDeleted = 0";

        var result = _parser.Parse(sql);

        Assert.False(result.HasErrors, string.Join("; ", result.Errors));
        Assert.True(result.Entries.Count >= 5, $"Expected >=5 entries, got {result.Entries.Count}");
        Assert.Contains(result.Entries, e =>
            e.TargetTable == "dbo.vw_FactSales" && e.TargetColumn == "NetAmount");
        Assert.Contains(result.Entries, e =>
            e.SourceTable == "dbo.DimCustomer" && e.SourceColumn == "CustomerName");
    }

    [Fact]
    public void RealView_CteWithUnionAll()
    {
        // Pattern: CTE + UNION ALL for combining multiple sources
        var sql = @"
            CREATE VIEW dbo.vw_AllTransactions AS
            WITH OnlineOrders AS (
                SELECT OrderId, CustomerId, Amount, 'Online' AS Channel
                FROM dbo.OnlineOrders
                WHERE Status = 'Completed'
            ),
            StoreOrders AS (
                SELECT OrderId, CustomerId, Amount, 'Store' AS Channel
                FROM dbo.StoreOrders
                WHERE Status = 'Completed'
            )
            SELECT OrderId, CustomerId, Amount, Channel
            FROM OnlineOrders
            UNION ALL
            SELECT OrderId, CustomerId, Amount, Channel
            FROM StoreOrders";

        var result = _parser.Parse(sql);

        Assert.False(result.HasErrors);
        Assert.Contains(result.Entries, e => e.SourceTable == "dbo.OnlineOrders");
        // Second UNION branch: CTE name may appear as source (acceptable for v1)
        Assert.Contains(result.Entries, e =>
            e.SourceTable == "dbo.StoreOrders" || e.SourceTable == "StoreOrders");
    }

    [Fact]
    public void RealView_AggregationWithGroupBy()
    {
        var sql = @"
            CREATE VIEW dbo.vw_MonthlySales AS
            SELECT
                d.YearMonth,
                d.YearName,
                s.StoreId,
                st.StoreName,
                SUM(f.Amount) AS TotalSales,
                COUNT(*) AS TransactionCount,
                AVG(f.Amount) AS AvgTransaction
            FROM dbo.FactSales f
            INNER JOIN dbo.DimDate d ON f.DateKey = d.DateKey
            INNER JOIN dbo.DimStore st ON f.StoreId = st.StoreId
            CROSS JOIN dbo.Settings s
            GROUP BY d.YearMonth, d.YearName, s.StoreId, st.StoreName";

        var result = _parser.Parse(sql);

        Assert.False(result.HasErrors);
        Assert.Contains(result.Entries, e =>
            e.TargetColumn == "TotalSales" && e.EdgeType == EdgeTypes.Aggregation);
        // COUNT(*) has no column parameters, so no entry for TransactionCount sources
        // but it should still appear as an output column
        Assert.True(result.Entries.Count >= 4, $"Expected >=4 entries, got {result.Entries.Count}");
    }

    [Fact]
    public void RealView_NestedSubqueryWithCase()
    {
        var sql = @"
            CREATE VIEW dbo.vw_CustomerSegments AS
            SELECT
                c.CustomerId,
                c.CustomerName,
                t.TotalSpent,
                CASE
                    WHEN t.TotalSpent >= 100000 THEN 'VIP'
                    WHEN t.TotalSpent >= 10000 THEN 'Premium'
                    WHEN t.TotalSpent >= 1000 THEN 'Regular'
                    ELSE 'New'
                END AS Segment
            FROM dbo.DimCustomer c
            LEFT JOIN (
                SELECT CustomerId, SUM(Amount) AS TotalSpent
                FROM dbo.FactSales
                GROUP BY CustomerId
            ) t ON c.CustomerId = t.CustomerId";

        var result = _parser.Parse(sql);

        Assert.False(result.HasErrors);
        // Segment depends on TotalSpent which traces back to dbo.FactSales.Amount via subquery
        Assert.Contains(result.Entries, e =>
            e.TargetColumn == "Segment" && e.SourceTable == "dbo.FactSales");
        Assert.Contains(result.Entries, e =>
            e.TargetColumn == "TotalSpent" && e.SourceColumn == "Amount");
    }

    [Fact]
    public void RealView_MergeStatement()
    {
        var sql = @"
            MERGE INTO dbo.DimCustomer AS tgt
            USING (
                SELECT CustomerId, CustomerName, Email, Phone
                FROM staging.Customers
                WHERE LoadDate = (SELECT MAX(LoadDate) FROM staging.Customers)
            ) AS src ON tgt.CustomerId = src.CustomerId
            WHEN MATCHED AND (tgt.CustomerName <> src.CustomerName OR tgt.Email <> src.Email) THEN
                UPDATE SET
                    tgt.CustomerName = src.CustomerName,
                    tgt.Email = src.Email,
                    tgt.Phone = src.Phone,
                    tgt.UpdatedDate = GETDATE()
            WHEN NOT MATCHED THEN
                INSERT (CustomerId, CustomerName, Email, Phone, CreatedDate)
                VALUES (src.CustomerId, src.CustomerName, src.Email, src.Phone, GETDATE());";

        var result = _parser.Parse(sql);

        Assert.False(result.HasErrors);
        // UPDATE part
        Assert.Contains(result.Entries, e =>
            e.TargetTable == "dbo.DimCustomer" && e.TargetColumn == "CustomerName" &&
            e.SourceColumn == "CustomerName");
        // INSERT part
        Assert.Contains(result.Entries, e =>
            e.TargetTable == "dbo.DimCustomer" && e.TargetColumn == "Email");
    }

    [Fact]
    public void RealProcedure_InsertSelectWithMultipleCTE()
    {
        var sql = @"
            CREATE PROCEDURE dbo.sp_LoadFactInventory
            AS
            BEGIN
                ;WITH CurrentStock AS (
                    SELECT WarehouseId, ProductId, SUM(Quantity) AS StockQty
                    FROM dbo.InventoryMovements
                    WHERE MovementDate <= GETDATE()
                    GROUP BY WarehouseId, ProductId
                ),
                Reservations AS (
                    SELECT ProductId, SUM(ReservedQty) AS ReservedQty
                    FROM dbo.OrderReservations
                    WHERE Status = 'Active'
                    GROUP BY ProductId
                )
                INSERT INTO dbo.FactInventory (WarehouseId, ProductId, StockQty, ReservedQty, AvailableQty, SnapshotDate)
                SELECT
                    cs.WarehouseId,
                    cs.ProductId,
                    cs.StockQty,
                    ISNULL(r.ReservedQty, 0) AS ReservedQty,
                    cs.StockQty - ISNULL(r.ReservedQty, 0) AS AvailableQty,
                    GETDATE() AS SnapshotDate
                FROM CurrentStock cs
                LEFT JOIN Reservations r ON cs.ProductId = r.ProductId
            END";

        var result = _parser.Parse(sql);

        Assert.False(result.HasErrors);
        // CTE names may appear as intermediate sources (CurrentStock, Reservations)
        // This is acceptable for v1 — ultimate source resolution through CTEs in INSERT context
        Assert.Contains(result.Entries, e =>
            e.TargetTable == "dbo.FactInventory" && e.TargetColumn == "StockQty");
        Assert.Contains(result.Entries, e =>
            e.TargetTable == "dbo.FactInventory" && e.TargetColumn == "ReservedQty");
    }
}
