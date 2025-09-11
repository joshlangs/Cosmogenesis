using Microsoft.Azure.Cosmos;

namespace Cosmogenesis.Core;

public abstract class ChangeFeedProcessorBase
{
    public static readonly TimeSpan DefaultPollInterval = TimeSpan.FromSeconds(5);
    public static readonly TimeSpan MinPollInterval = TimeSpan.FromMilliseconds(500);
    public static readonly TimeSpan MaxPollInterval = TimeSpan.FromMinutes(5);

    public const int DefaultMaxItemsPerBatch = 100;
    public const int MaxMaxItemsPerBatch = 10000;

    ChangeFeedProcessor? changeFeedProcessor;
    protected virtual ChangeFeedProcessor ChangeFeedProcessor => changeFeedProcessor ??= CreateChangeFeedProcessor();

    protected virtual Container DatabaseContainer { get; } = default!;
    protected virtual Container LeaseContainer { get; } = default!;
    protected virtual string ProcessorName { get; } = default!;
    protected virtual TimeSpan PollInterval { get; } = default!;
    protected virtual int MaxItemsPerBatch { get; } = default!;
    protected virtual DateTime StartTime { get; } = default!;

    protected virtual BatchProcessor BatchProcessor { get; } = default!;
    protected virtual DbSerializerBase Serializer { get; } = default!;

    /// <summary>Mocking constructor</summary>
    protected ChangeFeedProcessorBase() { }

    protected ChangeFeedProcessorBase(
        BatchProcessor batchProcessor,
        DbSerializerBase serializer,
        Container databaseContainer,
        Container leaseContainer,
        string processorName,
        int maxItemsPerBatch = DefaultMaxItemsPerBatch,
        TimeSpan? pollInterval = null,
        DateTime? startTime = null)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(processorName);
        ArgumentOutOfRangeException.ThrowIfLessThan(maxItemsPerBatch, 1);
        ArgumentNullException.ThrowIfNull(serializer);
        ArgumentNullException.ThrowIfNull(batchProcessor);
        ArgumentNullException.ThrowIfNull(databaseContainer);
        ArgumentNullException.ThrowIfNull(leaseContainer);

        if (maxItemsPerBatch > MaxMaxItemsPerBatch)
        {
            maxItemsPerBatch = MaxMaxItemsPerBatch;
        }
        MaxItemsPerBatch = maxItemsPerBatch;
        Serializer = serializer;

        PollInterval = pollInterval ?? DefaultPollInterval;
        if (PollInterval < TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(nameof(pollInterval));
        }
        if (PollInterval < MinPollInterval)
        {
            PollInterval = MinPollInterval;
        }
        else if (PollInterval > MaxPollInterval)
        {
            PollInterval = MaxPollInterval;
        }
        StartTime = startTime ?? IsoDateCheater.MinValue;
        BatchProcessor = batchProcessor;
        ProcessorName = processorName;
        DatabaseContainer = databaseContainer;
        LeaseContainer = leaseContainer;
    }

    protected virtual ChangeFeedProcessor CreateChangeFeedProcessor() => DatabaseContainer
        .GetChangeFeedProcessorBuilder(
            processorName: ProcessorName,
            onChangesDelegate: HandleChangeFeedStream
        )
        .WithInstanceName($"{DateTime.UtcNow:O}-{Guid.NewGuid()}")
        .WithLeaseContainer(LeaseContainer)
        .WithPollInterval(PollInterval)
        .WithMaxItems(MaxItemsPerBatch)
        .WithStartTime(StartTime)
        .Build();

    async Task HandleChangeFeedStream(ChangeFeedProcessorContext context, Stream changes, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var items = Serializer.DeserializeDocumentList<DbDoc>(changes);
        await BatchProcessor.HandleAsync(items, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Start processing the new and changed documents in the database.
    /// Processing begins from where it left off if you use the same processor name.
    /// </summary>
    public virtual Task StartAsync() => ChangeFeedProcessor.StartAsync();

    /// <summary>
    /// Stop processing the new and changed documents in the database.
    /// Processing will resume from where it left off if you use the same processor name.
    /// </summary>
    public virtual Task StopAsync() => ChangeFeedProcessor.StopAsync();
}
