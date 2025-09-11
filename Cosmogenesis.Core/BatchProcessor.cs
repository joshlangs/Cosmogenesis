namespace Cosmogenesis.Core;
public class BatchProcessor
{
    public const BatchProcessingMode DefaultProcessingMode = BatchProcessingMode.SequentialByPartition;

    protected virtual BatchHandlersBase BatchHandlers { get; } = default!;
    public virtual BatchProcessingMode ProcessingMode { get; set; } = DefaultProcessingMode;

    protected BatchProcessor() { }
    public BatchProcessor(BatchHandlersBase batchHandlers)
    {
        ArgumentNullException.ThrowIfNull(batchHandlers);
        BatchHandlers = batchHandlers;
    }

    public virtual async Task HandleAsync(IReadOnlyCollection<DbDoc> changes, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(changes);
        if (changes.Count == 0)
        {
            return;
        }

        cancellationToken.ThrowIfCancellationRequested();

        if (BatchHandlers.NewChangeFeedBatch?.Invoke(cancellationToken) is Task newChangeFeedBatch)
        {
            await newChangeFeedBatch.ConfigureAwait(false);
        }

        await (ProcessingMode switch
        {
            BatchProcessingMode.AllAtOnce => HandleAllAtOnce(changes: changes, cancellationToken: cancellationToken),
            BatchProcessingMode.SequentialByPartition => HandleSequentialByPartition(changes: changes, cancellationToken: cancellationToken),
            BatchProcessingMode.Sequential => HandleSequential(changes: changes, cancellationToken: cancellationToken),
            _ => throw new NotImplementedException()
        }).ConfigureAwait(false);

        if (BatchHandlers.FinishingBatch?.Invoke(cancellationToken) is Task finishingBatch)
        {
            await finishingBatch.ConfigureAwait(false);
        }
    }

    protected virtual Task HandleAllAtOnce(IReadOnlyCollection<DbDoc> changes, CancellationToken cancellationToken) => Task.WhenAll(changes.Select(x => BatchHandlers.GetHandlerTask(x, cancellationToken)).ExcludeNull());

    protected virtual Task HandleSequentialByPartition(IReadOnlyCollection<DbDoc> changes, CancellationToken cancellationToken)
    {
        var plan = changes
            .Select((doc, index) => new
            {
                doc,
                index
            })
            .GroupBy(x => x.doc.pk)
            .Select(x => x.OrderBy(x => x.index).ToList())
            .OrderBy(x => x[0].index)            
            .ToList();

        return Task.WhenAll(plan.Select(x => HandleSequential(x.Select(a => a.doc), cancellationToken)));
    }

    protected virtual async Task HandleSequential(IEnumerable<DbDoc> changes, CancellationToken cancellationToken)
    {
        foreach (var doc in changes)
        {
            cancellationToken.ThrowIfCancellationRequested();

            var t = BatchHandlers.GetHandlerTask(doc, cancellationToken);
            if (t is not null)
            {
                await t.ConfigureAwait(false);
            }
        }
    }
}
