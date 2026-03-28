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

                if (createView.SelectStatement?.QueryExpression != null)
                {
                    var viewName = string.Join(".", createView.SchemaObjectName.Identifiers.Select(i => i.Value));
                    var visitor = new ColumnLineageVisitor(_schemaProvider, contextDatabase, contextSchema);
                    var ctx = CreateContext(visitor, cteDefinitions, viewName);
                    ProcessQueryExpressionViaReflection(visitor, createView.SelectStatement.QueryExpression, ctx);
                    EmitEntries(result, ctx, viewName);
                    result.Warnings.AddRange(visitor.Result.Warnings);
                }
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
                var visitor = new ColumnLineageVisitor(_schemaProvider, contextDatabase, contextSchema);
                InjectCtes(visitor, cteDefinitions);

                if (alterView.SelectStatement?.QueryExpression != null)
                {
                    var viewName = string.Join(".", alterView.SchemaObjectName.Identifiers.Select(i => i.Value));
                    var ctx = CreateContext(visitor, cteDefinitions, viewName);
                    ProcessQueryExpressionViaReflection(visitor, alterView.SelectStatement.QueryExpression, ctx);
                    EmitEntries(result, ctx, viewName);
                    result.Warnings.AddRange(visitor.Result.Warnings);
                }
                break;
            }
            case AlterProcedureStatement alterProc:
            {
                if (alterProc.StatementList?.Statements != null)
                {
                    foreach (var stmt in alterProc.StatementList.Statements)
                        ProcessStatement(stmt, result, contextDatabase, contextSchema);
                }
                break;
            }
            case MergeStatement mergeStmt:
            {
                ProcessMergeStatement(mergeStmt, result, cteDefinitions, contextDatabase, contextSchema);
                break;
            }
            case UpdateStatement updateStmt:
            {
                ProcessUpdateStatement(updateStmt, result, cteDefinitions, contextDatabase, contextSchema);
                break;
            }
            // Statements we skip silently (no lineage info)
            case DeclareVariableStatement:
            case SetVariableStatement:
            case PredicateSetStatement:
            case BeginEndBlockStatement block:
                if (statement is BeginEndBlockStatement beginEnd)
                {
                    if (beginEnd.StatementList?.Statements != null)
                        foreach (var stmt in beginEnd.StatementList.Statements)
                            ProcessStatement(stmt, result, contextDatabase, contextSchema);
                }
                break;
            case IfStatement ifStmt:
            {
                // Process both branches
                if (ifStmt.ThenStatement != null)
                    ProcessNestedStatement(ifStmt.ThenStatement, result, contextDatabase, contextSchema);
                if (ifStmt.ElseStatement != null)
                    ProcessNestedStatement(ifStmt.ElseStatement, result, contextDatabase, contextSchema);
                break;
            }
            case WhileStatement whileStmt:
            {
                if (whileStmt.Statement != null)
                    ProcessNestedStatement(whileStmt.Statement, result, contextDatabase, contextSchema);
                break;
            }
            case TryCatchStatement tryCatch:
            {
                if (tryCatch.TryStatements?.Statements != null)
                    foreach (var stmt in tryCatch.TryStatements.Statements)
                        ProcessStatement(stmt, result, contextDatabase, contextSchema);
                break;
            }
        }
    }

    private void ProcessNestedStatement(TSqlStatement statement, SqlLineageResult result,
        string contextDatabase, string contextSchema)
    {
        if (statement is BeginEndBlockStatement block && block.StatementList?.Statements != null)
        {
            foreach (var stmt in block.StatementList.Statements)
                ProcessStatement(stmt, result, contextDatabase, contextSchema);
        }
        else
        {
            ProcessStatement(statement, result, contextDatabase, contextSchema);
        }
    }

    private void ProcessMergeStatement(MergeStatement mergeStmt, SqlLineageResult result,
        Dictionary<string, List<OutputColumn>> cteDefinitions, string contextDatabase, string contextSchema)
    {
        var spec = mergeStmt.MergeSpecification;
        if (spec == null) return;

        // Get target table name
        var targetName = spec.Target switch
        {
            NamedTableReference named => string.Join(".", named.SchemaObject.Identifiers.Select(i => i.Value)),
            _ => ""
        };
        if (string.IsNullOrEmpty(targetName)) return;

        var targetAlias = (spec.Target as NamedTableReference)?.Alias?.Value;

        // Process USING source as a table reference to get available columns
        var visitor = new ColumnLineageVisitor(_schemaProvider, contextDatabase, contextSchema);
        var ctx = CreateContext(visitor, cteDefinitions, targetName);

        // Register target table
        if (!string.IsNullOrEmpty(targetAlias))
        {
            ctx.TableAliases[targetAlias] = new TableInfo
            {
                FullName = targetName,
                Alias = targetAlias
            };
        }
        ctx.TableAliases[targetName] = new TableInfo { FullName = targetName, Alias = targetName };

        // Process USING clause as a table/subquery
        if (spec.TableReference != null)
            visitor.ProcessTableReference(spec.TableReference, ctx);

        // Process each action clause
        foreach (var action in spec.ActionClauses)
        {
            if (action.Action is UpdateMergeAction updateAction)
            {
                foreach (var setClause in updateAction.SetClauses)
                {
                    if (setClause is AssignmentSetClause assignment)
                    {
                        var targetCol = assignment.Column?.MultiPartIdentifier?.Identifiers.Last()?.Value ?? "";
                        var sources = visitor.ExtractColumnRefsPublic(assignment.NewValue, ctx);
                        foreach (var src in sources)
                        {
                            result.Entries.Add(new ColumnLineageEntry
                            {
                                SourceTable = src.TableName,
                                SourceColumn = src.ColumnName,
                                TargetTable = targetName,
                                TargetColumn = targetCol,
                                EdgeType = EdgeType.DataFlow
                            });
                        }
                    }
                }
            }
            else if (action.Action is InsertMergeAction insertAction)
            {
                var insertCols = insertAction.Columns?
                    .Select(c => c.MultiPartIdentifier?.Identifiers.Last()?.Value ?? "")
                    .Where(c => !string.IsNullOrEmpty(c))
                    .ToList() ?? new List<string>();

                if (insertAction.Source is ValuesInsertSource valuesSource)
                {
                    foreach (var row in valuesSource.RowValues)
                    {
                        for (int i = 0; i < Math.Min(insertCols.Count, row.ColumnValues.Count); i++)
                        {
                            var sources = visitor.ExtractColumnRefsPublic(row.ColumnValues[i], ctx);
                            foreach (var src in sources)
                            {
                                result.Entries.Add(new ColumnLineageEntry
                                {
                                    SourceTable = src.TableName,
                                    SourceColumn = src.ColumnName,
                                    TargetTable = targetName,
                                    TargetColumn = insertCols[i],
                                    EdgeType = EdgeType.DataFlow
                                });
                            }
                        }
                    }
                }
            }
        }
    }

    private void ProcessUpdateStatement(UpdateStatement updateStmt, SqlLineageResult result,
        Dictionary<string, List<OutputColumn>> cteDefinitions, string contextDatabase, string contextSchema)
    {
        var spec = updateStmt.UpdateSpecification;
        if (spec == null) return;

        // Handle CTEs on the update
        if (updateStmt.WithCtesAndXmlNamespaces?.CommonTableExpressions != null)
        {
            ProcessCtes(updateStmt.WithCtesAndXmlNamespaces.CommonTableExpressions,
                cteDefinitions, contextDatabase, contextSchema);
        }

        // Get target table name
        var targetName = spec.Target switch
        {
            NamedTableReference named => string.Join(".", named.SchemaObject.Identifiers.Select(i => i.Value)),
            _ => ""
        };
        if (string.IsNullOrEmpty(targetName)) return;

        var targetAlias = (spec.Target as NamedTableReference)?.Alias?.Value;

        var visitor = new ColumnLineageVisitor(_schemaProvider, contextDatabase, contextSchema);
        var ctx = CreateContext(visitor, cteDefinitions, targetName);

        // Register target
        if (!string.IsNullOrEmpty(targetAlias))
            ctx.TableAliases[targetAlias] = new TableInfo { FullName = targetName, Alias = targetAlias };

        // Process FROM clause (UPDATE ... FROM ... JOIN ...)
        if (spec.FromClause != null)
        {
            foreach (var tableRef in spec.FromClause.TableReferences)
                visitor.ProcessTableReference(tableRef, ctx);
        }

        // Process SET clauses
        foreach (var setClause in spec.SetClauses)
        {
            if (setClause is AssignmentSetClause assignment)
            {
                var targetCol = assignment.Column?.MultiPartIdentifier?.Identifiers.Last()?.Value ?? "";
                var sources = visitor.ExtractColumnRefsPublic(assignment.NewValue, ctx);
                foreach (var src in sources)
                {
                    result.Entries.Add(new ColumnLineageEntry
                    {
                        SourceTable = src.TableName,
                        SourceColumn = src.ColumnName,
                        TargetTable = targetName,
                        TargetColumn = targetCol,
                        EdgeType = EdgeType.DataFlow
                    });
                }
            }
        }
    }

    /// <summary>
    /// Helper to emit lineage entries from a processed QueryContext.
    /// </summary>
    private static void EmitEntries(SqlLineageResult result, QueryContext ctx, string targetTable)
    {
        foreach (var col in ctx.OutputColumns)
        {
            foreach (var src in col.SourceColumns)
            {
                result.Entries.Add(new ColumnLineageEntry
                {
                    SourceTable = src.TableName,
                    SourceColumn = src.ColumnName,
                    TargetTable = targetTable,
                    TargetColumn = col.OutputName,
                    EdgeType = col.IsAggregation ? EdgeType.Aggregation : EdgeType.DataFlow,
                    TransformExpression = col.TransformExpression
                });
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
