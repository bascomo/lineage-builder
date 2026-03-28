using Microsoft.SqlServer.TransactSql.ScriptDom;
using LineageBuilder.Core.Interfaces;
using LineageBuilder.Core.Model;

namespace LineageBuilder.SqlParser;

/// <summary>
/// Главный визитор для извлечения column-level lineage из T-SQL AST.
/// Поддерживает SELECT, INSERT...SELECT, CREATE VIEW, CTE, подзапросы, алиасы.
/// </summary>
public class ColumnLineageVisitor : TSqlFragmentVisitor
{
    private readonly ISchemaProvider? _schemaProvider;
    private readonly string _contextDatabase;
    private readonly string _contextSchema;
    private readonly SqlLineageResult _result = new();

    // Стек контекстов для вложенных запросов (CTE, подзапросы)
    private readonly Stack<QueryContext> _contextStack = new();

    public ColumnLineageVisitor(ISchemaProvider? schemaProvider = null,
        string contextDatabase = "", string contextSchema = "dbo")
    {
        _schemaProvider = schemaProvider;
        _contextDatabase = contextDatabase;
        _contextSchema = contextSchema;
    }

    public SqlLineageResult Result => _result;

    // ==================== Statement visitors ====================

    public override void Visit(CreateViewStatement node)
    {
        var viewName = GetObjectName(node.SchemaObjectName);
        var ctx = new QueryContext { TargetObject = viewName };
        _contextStack.Push(ctx);

        if (node.SelectStatement?.QueryExpression != null)
            ProcessQueryExpression(node.SelectStatement.QueryExpression, ctx);

        // Map select list columns to view columns
        foreach (var col in ctx.OutputColumns)
        {
            foreach (var src in col.SourceColumns)
            {
                _result.Entries.Add(new ColumnLineageEntry
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

        _contextStack.Pop();
    }

    public override void Visit(InsertStatement node)
    {
        var spec = node.InsertSpecification;
        if (spec == null) return;
        if (spec.InsertSource is not SelectInsertSource selectSource) return;
        var target = spec.Target;
        if (target == null) return;

        var targetName = GetTableReferenceName(target);
        if (string.IsNullOrEmpty(targetName)) return;

        // Get target column names from column list
        var targetColumns = spec.Columns?
            .Select(c => c.MultiPartIdentifier?.Identifiers.Last()?.Value ?? "")
            .Where(c => !string.IsNullOrEmpty(c))
            .ToList() ?? new List<string>();

        var ctx = new QueryContext { TargetObject = targetName };
        _contextStack.Push(ctx);

        ProcessQueryExpression(selectSource.Select, ctx);

        // Match output columns to target columns by position
        for (int i = 0; i < ctx.OutputColumns.Count; i++)
        {
            var col = ctx.OutputColumns[i];
            var targetCol = i < targetColumns.Count ? targetColumns[i] : col.OutputName;

            foreach (var src in col.SourceColumns)
            {
                _result.Entries.Add(new ColumnLineageEntry
                {
                    SourceTable = src.TableName,
                    SourceColumn = src.ColumnName,
                    TargetTable = targetName,
                    TargetColumn = targetCol,
                    EdgeType = col.IsAggregation ? EdgeType.Aggregation : EdgeType.DataFlow,
                    TransformExpression = col.TransformExpression
                });
            }
        }

        _contextStack.Pop();
    }

    // ==================== Query Expression Processing ====================

    internal void ProcessQueryExpression(QueryExpression? expr, QueryContext ctx)
    {
        switch (expr)
        {
            case QuerySpecification querySpec:
                ProcessQuerySpecification(querySpec, ctx);
                break;
            case BinaryQueryExpression binaryExpr: // UNION, EXCEPT, INTERSECT
                ProcessQueryExpression(binaryExpr.FirstQueryExpression, ctx);
                // Second query adds sources but doesn't change output columns
                var ctx2 = new QueryContext { TargetObject = ctx.TargetObject };
                ProcessQueryExpression(binaryExpr.SecondQueryExpression, ctx2);
                // Merge source columns from second branch
                for (int i = 0; i < Math.Min(ctx.OutputColumns.Count, ctx2.OutputColumns.Count); i++)
                {
                    ctx.OutputColumns[i].SourceColumns.AddRange(ctx2.OutputColumns[i].SourceColumns);
                }
                break;
            case QueryParenthesisExpression paren:
                ProcessQueryExpression(paren.QueryExpression, ctx);
                break;
        }
    }

    internal void ProcessQuerySpecification(QuerySpecification querySpec, QueryContext ctx)
    {
        // 1. Process WITH (CTEs) if present in parent select statement
        // CTEs are handled by ProcessCte before this method is called

        // 2. Process FROM clause to resolve table references
        if (querySpec.FromClause != null)
        {
            foreach (var tableRef in querySpec.FromClause.TableReferences)
            {
                ProcessTableReference(tableRef, ctx);
            }
        }

        // 3. Process SELECT list
        foreach (var element in querySpec.SelectElements)
        {
            switch (element)
            {
                case SelectScalarExpression scalar:
                    ProcessSelectScalar(scalar, ctx);
                    break;
                case SelectStarExpression star:
                    ProcessSelectStar(star, ctx);
                    break;
            }
        }

        // 4. Process WHERE clause (filter dependencies)
        if (querySpec.WhereClause?.SearchCondition != null)
        {
            var filterSources = ExtractColumnRefs(querySpec.WhereClause.SearchCondition, ctx);
            foreach (var src in filterSources)
            {
                ctx.FilterColumns.Add(src);
            }
        }

        // 5. Process JOIN ON conditions
        // Already handled in ProcessTableReference for QualifiedJoin
    }

    // ==================== Table Reference Processing ====================

    private void ProcessTableReference(TableReference tableRef, QueryContext ctx)
    {
        switch (tableRef)
        {
            case NamedTableReference namedTable:
            {
                var tableName = GetObjectName(namedTable.SchemaObject);
                var alias = namedTable.Alias?.Value ?? tableName;

                // Check if this is a CTE reference
                if (ctx.CteDefinitions.TryGetValue(tableName, out var cteCols))
                {
                    ctx.TableAliases[alias] = new TableInfo
                    {
                        FullName = tableName,
                        Alias = alias,
                        IsCte = true,
                        Columns = cteCols.Select(c => c.OutputName).ToList()
                    };
                }
                else
                {
                    ctx.TableAliases[alias] = new TableInfo
                    {
                        FullName = tableName,
                        Alias = alias
                    };
                }
                break;
            }
            case QualifiedJoin qualifiedJoin:
            {
                ProcessTableReference(qualifiedJoin.FirstTableReference, ctx);
                ProcessTableReference(qualifiedJoin.SecondTableReference, ctx);
                // Extract join condition columns
                if (qualifiedJoin.SearchCondition != null)
                {
                    var joinSources = ExtractColumnRefs(qualifiedJoin.SearchCondition, ctx);
                    foreach (var src in joinSources)
                    {
                        ctx.JoinColumns.Add(src);
                    }
                }
                break;
            }
            case UnqualifiedJoin unqualifiedJoin:
            {
                ProcessTableReference(unqualifiedJoin.FirstTableReference, ctx);
                ProcessTableReference(unqualifiedJoin.SecondTableReference, ctx);
                break;
            }
            case QueryDerivedTable derivedTable:
            {
                var alias = derivedTable.Alias?.Value ?? "";
                var subCtx = new QueryContext { TargetObject = alias };
                // Inherit CTE definitions
                foreach (var cte in ctx.CteDefinitions)
                    subCtx.CteDefinitions[cte.Key] = cte.Value;

                if (derivedTable.QueryExpression != null)
                    ProcessQueryExpression(derivedTable.QueryExpression, subCtx);

                ctx.TableAliases[alias] = new TableInfo
                {
                    FullName = alias,
                    Alias = alias,
                    IsSubquery = true,
                    SubqueryColumns = subCtx.OutputColumns
                };
                break;
            }
            case JoinParenthesisTableReference joinParen:
                ProcessTableReference(joinParen.Join, ctx);
                break;
        }
    }

    // ==================== SELECT Element Processing ====================

    private void ProcessSelectScalar(SelectScalarExpression scalar, QueryContext ctx)
    {
        var outputName = scalar.ColumnName?.Value;
        var sources = ExtractColumnRefs(scalar.Expression, ctx);

        // If no alias, try to infer from expression
        if (string.IsNullOrEmpty(outputName))
        {
            if (scalar.Expression is ColumnReferenceExpression colRef)
                outputName = colRef.MultiPartIdentifier?.Identifiers.Last()?.Value ?? "?";
            else
                outputName = "expr_" + (ctx.OutputColumns.Count + 1);
        }

        var isAggregation = ContainsAggregation(scalar.Expression);
        string? transformExpr = null;
        if (IsTransformExpression(scalar.Expression))
            transformExpr = FragmentToString(scalar.Expression);

        ctx.OutputColumns.Add(new OutputColumn
        {
            OutputName = outputName,
            SourceColumns = sources,
            IsAggregation = isAggregation,
            TransformExpression = transformExpr
        });
    }

    private void ProcessSelectStar(SelectStarExpression star, QueryContext ctx)
    {
        var tableAlias = star.Qualifier?.Identifiers.Last()?.Value;

        if (tableAlias != null)
        {
            // SELECT t.* — expand for one table
            ExpandStar(tableAlias, ctx);
        }
        else
        {
            // SELECT * — expand all tables
            foreach (var alias in ctx.TableAliases.Keys.ToList())
            {
                ExpandStar(alias, ctx);
            }
        }
    }

    private void ExpandStar(string alias, QueryContext ctx)
    {
        if (!ctx.TableAliases.TryGetValue(alias, out var tableInfo)) return;

        if (tableInfo.IsSubquery && tableInfo.SubqueryColumns != null)
        {
            // Expand from subquery output columns
            foreach (var subCol in tableInfo.SubqueryColumns)
            {
                ctx.OutputColumns.Add(new OutputColumn
                {
                    OutputName = subCol.OutputName,
                    SourceColumns = new List<ColumnRef>(subCol.SourceColumns)
                });
            }
        }
        else if (tableInfo.IsCte && ctx.CteDefinitions.TryGetValue(tableInfo.FullName, out var cteCols))
        {
            foreach (var cteCol in cteCols)
            {
                ctx.OutputColumns.Add(new OutputColumn
                {
                    OutputName = cteCol.OutputName,
                    SourceColumns = new List<ColumnRef>(cteCol.SourceColumns)
                });
            }
        }
        else if (_schemaProvider != null)
        {
            // Use schema provider to expand
            var parts = tableInfo.FullName.Split('.');
            var (server, db, schema, obj) = parts.Length switch
            {
                4 => (parts[0], parts[1], parts[2], parts[3]),
                3 => ("", parts[0], parts[1], parts[2]),
                2 => ("", _contextDatabase, parts[0], parts[1]),
                1 => ("", _contextDatabase, _contextSchema, parts[0]),
                _ => ("", "", "", "")
            };

            var columns = _schemaProvider.GetColumns(server, db, schema, obj);
            foreach (var col in columns)
            {
                ctx.OutputColumns.Add(new OutputColumn
                {
                    OutputName = col,
                    SourceColumns = new List<ColumnRef>
                    {
                        new() { TableName = tableInfo.FullName, ColumnName = col }
                    }
                });
            }
        }
        else
        {
            _result.Warnings.Add($"SELECT * from {tableInfo.FullName}: cannot expand without schema provider");
            ctx.OutputColumns.Add(new OutputColumn
            {
                OutputName = "*",
                SourceColumns = new List<ColumnRef>
                {
                    new() { TableName = tableInfo.FullName, ColumnName = "*" }
                }
            });
        }
    }

    // ==================== Expression Analysis ====================

    private List<ColumnRef> ExtractColumnRefs(ScalarExpression? expr, QueryContext ctx)
    {
        var refs = new List<ColumnRef>();
        if (expr == null) return refs;

        switch (expr)
        {
            case ColumnReferenceExpression colRef:
            {
                var identifiers = colRef.MultiPartIdentifier?.Identifiers;
                if (identifiers == null || identifiers.Count == 0) break;

                string? tableAlias = null;
                string columnName;

                if (identifiers.Count >= 2)
                {
                    tableAlias = identifiers[identifiers.Count - 2].Value;
                    columnName = identifiers[identifiers.Count - 1].Value;
                }
                else
                {
                    columnName = identifiers[0].Value;
                }

                var resolved = ResolveColumn(tableAlias, columnName, ctx);
                refs.Add(resolved);
                break;
            }
            case FunctionCall funcCall:
            {
                // Process all function parameters
                foreach (var param in funcCall.Parameters)
                {
                    refs.AddRange(ExtractColumnRefs(param, ctx));
                }
                // OVER clause (window functions)
                if (funcCall.OverClause != null)
                {
                    if (funcCall.OverClause.Partitions != null)
                        foreach (var p in funcCall.OverClause.Partitions)
                            refs.AddRange(ExtractColumnRefs(p, ctx));
                    if (funcCall.OverClause.OrderByClause != null)
                        foreach (var o in funcCall.OverClause.OrderByClause.OrderByElements)
                            refs.AddRange(ExtractColumnRefs(o.Expression, ctx));
                }
                break;
            }
            case BinaryExpression binary:
                refs.AddRange(ExtractColumnRefs(binary.FirstExpression, ctx));
                refs.AddRange(ExtractColumnRefs(binary.SecondExpression, ctx));
                break;
            case UnaryExpression unary:
                refs.AddRange(ExtractColumnRefs(unary.Expression, ctx));
                break;
            case ParenthesisExpression paren:
                refs.AddRange(ExtractColumnRefs(paren.Expression, ctx));
                break;
            case CastCall cast:
                refs.AddRange(ExtractColumnRefs(cast.Parameter, ctx));
                break;
            case ConvertCall convert:
                refs.AddRange(ExtractColumnRefs(convert.Parameter, ctx));
                break;
            case SearchedCaseExpression searchedCase:
                foreach (var when in searchedCase.WhenClauses)
                {
                    refs.AddRange(ExtractColumnRefs(when.WhenExpression, ctx));
                    refs.AddRange(ExtractColumnRefs(when.ThenExpression, ctx));
                }
                if (searchedCase.ElseExpression != null)
                    refs.AddRange(ExtractColumnRefs(searchedCase.ElseExpression, ctx));
                break;
            case SimpleCaseExpression simpleCase:
                refs.AddRange(ExtractColumnRefs(simpleCase.InputExpression, ctx));
                foreach (var when in simpleCase.WhenClauses)
                {
                    refs.AddRange(ExtractColumnRefs(when.WhenExpression, ctx));
                    refs.AddRange(ExtractColumnRefs(when.ThenExpression, ctx));
                }
                if (simpleCase.ElseExpression != null)
                    refs.AddRange(ExtractColumnRefs(simpleCase.ElseExpression, ctx));
                break;
            case CoalesceExpression coalesce:
                foreach (var e in coalesce.Expressions)
                    refs.AddRange(ExtractColumnRefs(e, ctx));
                break;
            case NullIfExpression nullIf:
                refs.AddRange(ExtractColumnRefs(nullIf.FirstExpression, ctx));
                refs.AddRange(ExtractColumnRefs(nullIf.SecondExpression, ctx));
                break;
            case IIfCall iif:
                refs.AddRange(ExtractColumnRefs(iif.Predicate, ctx));
                refs.AddRange(ExtractColumnRefs(iif.ThenExpression, ctx));
                refs.AddRange(ExtractColumnRefs(iif.ElseExpression, ctx));
                break;
            case ScalarSubquery scalarSub:
            {
                var subCtx = new QueryContext();
                foreach (var cte in ctx.CteDefinitions)
                    subCtx.CteDefinitions[cte.Key] = cte.Value;
                ProcessQueryExpression(scalarSub.QueryExpression, subCtx);
                foreach (var col in subCtx.OutputColumns)
                    refs.AddRange(col.SourceColumns);
                break;
            }
            // Literals, variables — no column refs
            case IntegerLiteral:
            case StringLiteral:
            case NumericLiteral:
            case NullLiteral:
            case RealLiteral:
            case MoneyLiteral:
            case VariableReference:
            case GlobalVariableExpression:
                break;
        }

        return refs;
    }

    private List<ColumnRef> ExtractColumnRefs(BooleanExpression? expr, QueryContext ctx)
    {
        var refs = new List<ColumnRef>();
        if (expr == null) return refs;

        switch (expr)
        {
            case BooleanComparisonExpression cmp:
                refs.AddRange(ExtractColumnRefs(cmp.FirstExpression, ctx));
                refs.AddRange(ExtractColumnRefs(cmp.SecondExpression, ctx));
                break;
            case BooleanBinaryExpression bin:
                refs.AddRange(ExtractColumnRefs(bin.FirstExpression, ctx));
                refs.AddRange(ExtractColumnRefs(bin.SecondExpression, ctx));
                break;
            case BooleanNotExpression not:
                refs.AddRange(ExtractColumnRefs(not.Expression, ctx));
                break;
            case BooleanParenthesisExpression paren:
                refs.AddRange(ExtractColumnRefs(paren.Expression, ctx));
                break;
            case BooleanIsNullExpression isNull:
                refs.AddRange(ExtractColumnRefs(isNull.Expression, ctx));
                break;
            case InPredicate inPred:
                refs.AddRange(ExtractColumnRefs(inPred.Expression, ctx));
                foreach (var val in inPred.Values)
                    refs.AddRange(ExtractColumnRefs(val, ctx));
                break;
            case LikePredicate like:
                refs.AddRange(ExtractColumnRefs(like.FirstExpression, ctx));
                refs.AddRange(ExtractColumnRefs(like.SecondExpression, ctx));
                break;
            case BooleanTernaryExpression ternary:
                refs.AddRange(ExtractColumnRefs(ternary.FirstExpression, ctx));
                refs.AddRange(ExtractColumnRefs(ternary.SecondExpression, ctx));
                refs.AddRange(ExtractColumnRefs(ternary.ThirdExpression, ctx));
                break;
            case ExistsPredicate exists:
                // Don't trace into EXISTS subquery for lineage
                break;
        }

        return refs;
    }

    // Helper: extract column refs from generic ScalarExpression that might be BooleanExpression
    private List<ColumnRef> ExtractColumnRefs(BooleanExpression? boolExpr, ScalarExpression? scalarExpr, QueryContext ctx)
    {
        var refs = new List<ColumnRef>();
        if (boolExpr != null) refs.AddRange(ExtractColumnRefs(boolExpr, ctx));
        if (scalarExpr != null) refs.AddRange(ExtractColumnRefs(scalarExpr, ctx));
        return refs;
    }

    // ==================== Column Resolution ====================

    private ColumnRef ResolveColumn(string? tableAlias, string columnName, QueryContext ctx)
    {
        if (tableAlias != null && ctx.TableAliases.TryGetValue(tableAlias, out var tableInfo))
        {
            // Resolve through subquery
            if (tableInfo.IsSubquery && tableInfo.SubqueryColumns != null)
            {
                var subCol = tableInfo.SubqueryColumns
                    .FirstOrDefault(c => c.OutputName.Equals(columnName, StringComparison.OrdinalIgnoreCase));
                if (subCol != null && subCol.SourceColumns.Count > 0)
                    return subCol.SourceColumns[0]; // Return the ultimate source
            }

            // Resolve through CTE
            if (tableInfo.IsCte && ctx.CteDefinitions.TryGetValue(tableInfo.FullName, out var cteCols))
            {
                var cteCol = cteCols
                    .FirstOrDefault(c => c.OutputName.Equals(columnName, StringComparison.OrdinalIgnoreCase));
                if (cteCol != null && cteCol.SourceColumns.Count > 0)
                    return cteCol.SourceColumns[0];
            }

            return new ColumnRef { TableName = tableInfo.FullName, ColumnName = columnName };
        }

        // No table alias — try to find the column in any available table
        if (tableAlias == null)
        {
            // Try subqueries and CTEs first
            foreach (var ti in ctx.TableAliases.Values)
            {
                if (ti.IsSubquery && ti.SubqueryColumns != null)
                {
                    var subCol = ti.SubqueryColumns
                        .FirstOrDefault(c => c.OutputName.Equals(columnName, StringComparison.OrdinalIgnoreCase));
                    if (subCol != null && subCol.SourceColumns.Count > 0)
                        return subCol.SourceColumns[0];
                }
                if (ti.IsCte && ctx.CteDefinitions.TryGetValue(ti.FullName, out var cteColsNoAlias))
                {
                    var cteCol = cteColsNoAlias
                        .FirstOrDefault(c => c.OutputName.Equals(columnName, StringComparison.OrdinalIgnoreCase));
                    if (cteCol != null && cteCol.SourceColumns.Count > 0)
                        return cteCol.SourceColumns[0];
                }
            }
        }

        // Fallback: return with alias or unknown table
        return new ColumnRef
        {
            TableName = tableAlias ?? ctx.TableAliases.Values.FirstOrDefault()?.FullName ?? "?",
            ColumnName = columnName
        };
    }

    // ==================== Helpers ====================

    private static bool ContainsAggregation(ScalarExpression? expr)
    {
        if (expr is FunctionCall func)
        {
            var name = func.FunctionName?.Value?.ToUpperInvariant();
            if (name is "SUM" or "COUNT" or "AVG" or "MIN" or "MAX" or "COUNT_BIG"
                or "STDEV" or "STDEVP" or "VAR" or "VARP")
                return true;
        }
        return false;
    }

    private static bool IsTransformExpression(ScalarExpression? expr) =>
        expr is not ColumnReferenceExpression;

    private string GetObjectName(SchemaObjectName? name)
    {
        if (name == null) return "";
        var parts = name.Identifiers.Select(i => i.Value).ToList();
        return string.Join(".", parts);
    }

    private string GetTableReferenceName(TableReference tableRef) => tableRef switch
    {
        NamedTableReference named => GetObjectName(named.SchemaObject),
        _ => ""
    };

    private static string FragmentToString(TSqlFragment fragment)
    {
        var sb = new System.Text.StringBuilder();
        for (int i = fragment.FirstTokenIndex; i <= fragment.LastTokenIndex; i++)
        {
            sb.Append(fragment.ScriptTokenStream[i].Text);
        }
        return sb.ToString().Trim();
    }
}

// ==================== Internal Models ====================

internal class QueryContext
{
    public string TargetObject { get; set; } = "";
    public Dictionary<string, TableInfo> TableAliases { get; } = new(StringComparer.OrdinalIgnoreCase);
    public List<OutputColumn> OutputColumns { get; } = new();
    public List<ColumnRef> FilterColumns { get; } = new();
    public List<ColumnRef> JoinColumns { get; } = new();
    public Dictionary<string, List<OutputColumn>> CteDefinitions { get; } = new(StringComparer.OrdinalIgnoreCase);
}

internal class TableInfo
{
    public string FullName { get; set; } = "";
    public string Alias { get; set; } = "";
    public bool IsCte { get; set; }
    public bool IsSubquery { get; set; }
    public List<string>? Columns { get; set; }
    public List<OutputColumn>? SubqueryColumns { get; set; }
}

internal class OutputColumn
{
    public string OutputName { get; set; } = "";
    public List<ColumnRef> SourceColumns { get; set; } = new();
    public bool IsAggregation { get; set; }
    public string? TransformExpression { get; set; }
}

public class ColumnRef
{
    public string TableName { get; set; } = "";
    public string ColumnName { get; set; } = "";
    public override string ToString() => $"{TableName}.{ColumnName}";
}
