using LineageBuilder.Core.Model;

namespace LineageBuilder.Core.Interfaces;

/// <summary>
/// Интерфейс для всех экстракторов метаданных.
/// </summary>
public interface IMetadataExtractor
{
    /// <summary>Имя экстрактора для логирования.</summary>
    string Name { get; }

    /// <summary>
    /// Извлечь метаданные и добавить узлы/рёбра в граф.
    /// </summary>
    Task ExtractAsync(LineageGraph graph, CancellationToken cancellationToken = default);
}
