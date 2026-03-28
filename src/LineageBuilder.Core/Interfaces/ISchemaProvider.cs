namespace LineageBuilder.Core.Interfaces;

/// <summary>
/// Поставщик схемы БД — для разрешения SELECT * и проверки существования колонок.
/// </summary>
public interface ISchemaProvider
{
    /// <summary>
    /// Получить список колонок таблицы/view по полному имени [server].[db].[schema].[table].
    /// </summary>
    IReadOnlyList<string> GetColumns(string serverName, string databaseName, string schemaName, string objectName);

    /// <summary>
    /// Проверить существование колонки.
    /// </summary>
    bool ColumnExists(string serverName, string databaseName, string schemaName, string objectName, string columnName);
}
