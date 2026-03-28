using Microsoft.SqlServer.TransactSql.ScriptDom;
using LineageBuilder.Core.Interfaces;
using LineageBuilder.Core.Model;

namespace LineageBuilder.SqlParser;

/// <summary>
/// Парсер T-SQL для извлечения column-level lineage.
/// Использует Microsoft.SqlServer.TransactSql.ScriptDom (TSql160Parser).
/// </summary>
public class TsqlLineageParser : ISqlLineageParser
{
    private readonly ISchemaProvider? _schemaProvider;

    public TsqlLineageParser(ISchemaProvider? schemaProvider = null)
    {
        _schemaProvider = schemaProvider;
    }

    public SqlLineageResult Parse(string sql, string? contextDatabase = null, string? contextSchema = null)
    {
        if (string.IsNullOrWhiteSpace(sql))
            return new SqlLineageResult { Errors = { "Empty SQL input" } };

        var parser = new TSql160Parser(initialQuotedIdentifiers: false);
        var fragment = parser.Parse(new StringReader(sql), out var parseErrors);

        var result = new SqlLineageResult();

        if (parseErrors.Any())
        {
            foreach (var error in parseErrors)
            {
                result.Errors.Add($"Line {error.Line}, Col {error.Column}: {error.Message}");
            }
            // Try to continue even with parse errors
        }

        if (fragment is TSqlScript script)
        {
            foreach (var batch in script.Batches)
            {
                foreach (var statement in batch.Statements)
                {
                    ProcessStatement(statement, result, contextDatabase ?? "", contextSchema ?? "dbo");
                }
            }
        }

        return result;
    }

    private void ProcessStatement(TSqlStatement statement, SqlLineageResult result,
        string contextDatabase, string contextSchema)
    {
        // Handle CTEs: they are on the SelectStatement wrapping the QueryExpression
        var cteDefinitions = new Dictionary<string, List<OutputColumn>>(StringComparer.OrdinalIgnoreCase);

        switch (statement)
        {
            case CreateViewStatement createView:
            {
                // Process CTEs from the select statement within the view
                if (createView.SelectStatement?.WithCtesAndXmlNamespaces?.CommonTableExpressions != null)
                {
                    ProcessCtes(createView.SelectStatement.WithCtesAndXmlNamespaces.CommonTableExpressions,
                        cteDefinitions, contextDatabase, contextSchema);
                }

                var visitor = new ColumnLineageVisitor(_schemaProvider, contextDatabase, contextSchema);
                // Inject CTEs
                InjectCtes(visitor, cteDefinitions);
                visitor.Visit(createView);
                MergeResult(result, visitor.Result);
                break;
            }
            case SelectStatement selectStmt:
            {
                if (selectStmt.WithCtesAndXmlNamespaces?.CommonTableExpressions != null)
                {
                    ProcessCtes(selectStmt.WithCtesAndXmlNamespaces.CommonTableExpressions,
                        cteDefinitions, contextDatabase, contextSchema);
                }

                if (selectStmt.QueryExpression != null)
                {
                    var visitor = new ColumnLineageVisitor(_schemaProvider, contextDatabase, contextSchema);
                    InjectCtes(visitor, cteDefinitions);

                    // Process as a standalone select — create a temporary context
                    var ctx = CreateContext(visitor, cteDefinitions);
                    ProcessQueryExpressionViaReflection(visitor, selectStmt.QueryExpression, ctx);

                    // Add entries from the context output columns
                    foreach (var col in ctx.OutputColumns)
                    {
                        foreach (var src in col.SourceColumns)
                        {
                            result.Entries.Add(new ColumnLineageEntry
                            {
                                SourceTable = src.TableName,
                                SourceColumn = src.ColumnName,
                                TargetTable = "",
                                TargetColumn = col.OutputName,
                                EdgeType = col.IsAggregation ? EdgeType.Aggregation : EdgeType.DataFlow,
                                TransformExpression = col.TransformExpression
                            });
                        }
                    }
                    result.Warnings.AddRange(visitor.Result.Warnings);
                }
                break;
            }
            case InsertStatement insertStmt:
            {
                // Check for CTEs
                if (insertStmt.WithCtesAndXmlNamespaces?.CommonTableExpressions != null)
                {
                    ProcessCtes(insertStmt.WithCtesAndXmlNamespaces.CommonTableExpressions,
                        cteDefinitions, contextDatabase, contextSchema);
                }

                var visitor = new ColumnLineageVisitor(_schemaProvider, contextDatabase, contextSchema);
                InjectCtes(visitor, cteDefinitions);
                visitor.Visit(insertStmt);
                MergeResult(result, visitor.Result);
                break;
            }
            case CreateProcedureStatement createProc:
            {
                // Process all statements in the procedure body
                if (createProc.StatementList?.Statements != null)
                {
                    foreach (var stmt in createProc.StatementList.Statements)
                    {
                        ProcessStatement(stmt, result, contextDatabase, contextSchema);
                    }
                }
                break;
            }
            case AlterViewStatement alterView:
            {
                if (alterView.SelectStatement?.WithCtesAndXmlNamespaces?.CommonTableExpressions != null)
                {
                    ProcessCtes(alterView.SelectStatement.WithCtesAndXmlNamespaces.CommonTableExpressions,
                        cteDefinitions, contextDatabase, contextSchema);
                }
                // Treat ALTER VIEW similar to CREATE VIEW
                var visitor = new ColumnLineageVisitor(_schemaProvider, contextDatabase, contextSchema);
                InjectCtes(visitor, cteDefinitions);

                // Process the select statement body
                if (alterView.SelectStatement?.QueryExpression != null)
                {
                    var viewName = string.Join(".", alterView.SchemaObjectName.Identifiers.Select(i => i.Value));
                    var ctx = CreateContext(visitor, cteDefinitions, viewName);
                    ProcessQueryExpressionViaReflection(visitor, alterView.SelectStatement.QueryExpression, ctx);

                    foreach (var col in ctx.OutputColumns)
                    {
                        foreach (var src in col.SourceColumns)
                        {
                            result.Entries.Add(new ColumnLineageEntry
                            {
                                SourceTable = src.TableName,
                                SourceColumn = src.ColumnName,
                                TargetTable = viewName,
                                TargetColumn = col.OutputName,
                                EdgeType = col.IsAggregation ? EdgeType.Aggregation : EdgeType.DataFlow,
                                TransformExpression = col.TransformExpression
                            });
                        }
                    }
                    result.Warnings.AddRange(visitor.Result.Warnings);
                }
                break;
            }
        }
    }

    private void ProcessCtes(IList<CommonTableExpression> ctes,
        Dictionary<string, List<OutputColumn>> cteDefinitions,
        string contextDatabase, string contextSchema)
    {
        foreach (var cte in ctes)
        {
            var cteName = cte.ExpressionName?.Value ?? "";
            var visitor = new ColumnLineageVisitor(_schemaProvider, contextDatabase, contextSchema);
            InjectCtes(visitor, cteDefinitions); // Previous CTEs visible

            var ctx = CreateContext(visitor, cteDefinitions);
            ProcessQueryExpressionViaReflection(visitor, cte.QueryExpression, ctx);

            // If CTE has explicit column names
            if (cte.Columns != null && cte.Columns.Count > 0)
            {
                for (int i = 0; i < Math.Min(cte.Columns.Count, ctx.OutputColumns.Count); i++)
                {
                    ctx.OutputColumns[i].OutputName = cte.Columns[i].Value;
                }
            }

            cteDefinitions[cteName] = ctx.OutputColumns;
        }
    }

    private static QueryContext CreateContext(ColumnLineageVisitor visitor,
        Dictionary<string, List<OutputColumn>> cteDefinitions, string targetObject = "")
    {
        var ctx = new QueryContext { TargetObject = targetObject };
        foreach (var cte in cteDefinitions)
            ctx.CteDefinitions[cte.Key] = cte.Value;
        return ctx;
    }

    private static void InjectCtes(ColumnLineageVisitor visitor,
        Dictionary<string, List<OutputColumn>> cteDefinitions)
    {
        // CTEs are passed via QueryContext, not directly into the visitor
    }

    private static void ProcessQueryExpressionViaReflection(ColumnLineageVisitor visitor,
        QueryExpression queryExpression, QueryContext ctx)
    {
        visitor.ProcessQueryExpression(queryExpression, ctx);
    }

    private static void MergeResult(SqlLineageResult target, SqlLineageResult source)
    {
        target.Entries.AddRange(source.Entries);
        target.Errors.AddRange(source.Errors);
        target.Warnings.AddRange(source.Warnings);
    }
}
