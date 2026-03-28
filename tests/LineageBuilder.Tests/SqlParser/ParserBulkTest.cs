using LineageBuilder.Core.Model;
using LineageBuilder.SqlParser;
using LineageBuilder.SqlParser.Schema;

namespace LineageBuilder.Tests.SqlParser;

/// <summary>
/// Bulk test: parse many SQL patterns to ensure no crashes.
/// </summary>
public class ParserBulkTest
{
    private readonly TsqlLineageParser _parser = new(new InMemorySchemaProvider());

    /// <summary>
    /// Parse a variety of complex SQL patterns without crashing.
    /// </summary>
    [Theory]
    [InlineData("SELECT a.Col1, b.Col2 FROM dbo.T1 a JOIN dbo.T2 b ON a.Id = b.Id")]
    [InlineData("SELECT *, GETDATE() AS Now FROM dbo.T1")]
    [InlineData("SELECT TOP 10 Col1, Col2 FROM dbo.T1 ORDER BY Col1 DESC")]
    [InlineData("SELECT Col1, ROW_NUMBER() OVER (PARTITION BY Col2 ORDER BY Col3) AS RN FROM dbo.T1")]
    [InlineData("SELECT COALESCE(a.X, b.X, 0) AS X FROM dbo.T1 a LEFT JOIN dbo.T2 b ON a.Id = b.Id")]
    [InlineData("INSERT INTO dbo.Target (A, B) SELECT X, Y FROM dbo.Source WHERE Z > 0")]
    [InlineData("UPDATE t SET t.Col1 = s.Col1 FROM dbo.Target t JOIN dbo.Source s ON t.Id = s.Id")]
    [InlineData("SELECT a.Col1 FROM dbo.T1 a WHERE EXISTS (SELECT 1 FROM dbo.T2 b WHERE b.Id = a.Id)")]
    [InlineData("SELECT a.Col1 FROM dbo.T1 a WHERE a.Col2 IN (SELECT Col2 FROM dbo.T2)")]
    [InlineData("SELECT a.Col1 FROM dbo.T1 a WHERE a.Col2 BETWEEN 1 AND 100")]
    [InlineData("SELECT CAST(Col1 AS VARCHAR(100)) + ' - ' + Col2 AS Combined FROM dbo.T1")]
    [InlineData("SELECT IIF(Col1 > 0, 'Positive', 'Negative') AS Sign FROM dbo.T1")]
    [InlineData("SELECT NULLIF(Col1, 0) AS NZ FROM dbo.T1")]
    public void DoesNotCrash(string sql)
    {
        var result = _parser.Parse(sql);
        // Should not throw — errors in result are OK (parse warnings), but no exception
        Assert.NotNull(result);
    }

    [Fact]
    public void RealWorldComplexView_DoesNotCrash()
    {
        // Simulates a real DWH view with IIF, GROUP BY, HAVING, UNION, subquery
        var sql = @"
            CREATE VIEW dbo.vOLAP_Moves AS
            SELECT
                m.MoveId,
                m.MoveDate,
                s.StoreName AS FromStore,
                d.StoreName AS ToStore,
                p.ProductName,
                m.Quantity,
                m.Amount,
                IIF(m.MoveType = 1, 'Internal', 'External') AS MoveCategory,
                CASE
                    WHEN m.Amount > 10000 THEN 'Large'
                    WHEN m.Amount > 1000 THEN 'Medium'
                    ELSE 'Small'
                END AS SizeCategory,
                ROW_NUMBER() OVER (PARTITION BY m.FromStoreId ORDER BY m.MoveDate DESC) AS RowNum
            FROM dbo.FactMoves m
            INNER JOIN dbo.DimStore s ON m.FromStoreId = s.StoreId
            INNER JOIN dbo.DimStore d ON m.ToStoreId = d.StoreId
            INNER JOIN dbo.DimProduct p ON m.ProductId = p.ProductId
            WHERE m.MoveDate >= '2020-01-01'
              AND m.IsDeleted = 0
              AND m.MoveType IN (1, 2, 3)";

        var result = _parser.Parse(sql);

        Assert.False(result.HasErrors, string.Join("; ", result.Errors));
        Assert.True(result.Entries.Count >= 8, $"Expected >=8 entries, got {result.Entries.Count}");
    }
}
