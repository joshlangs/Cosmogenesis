using Moq;

namespace Test.Cosmogenesis.Core;

public class BatchProcessorTests
{
#pragma warning disable IDE0060 // Remove unused parameter
    public class TestChangeFeed(BatchHandlersBase batchHandlers) : BatchProcessor(batchHandlers)
    {
        public sealed override Task HandleAsync(IReadOnlyCollection<DbDoc> changes, CancellationToken cancellationToken) => MockHandleAsync(changes, cancellationToken);
        protected sealed override Task HandleAllAtOnce(IReadOnlyCollection<DbDoc> changes, CancellationToken cancellationToken) => MockHandleAllAtOnce(changes, cancellationToken);
        protected sealed override Task HandleSequential(IEnumerable<DbDoc> changes, CancellationToken cancellationToken) => MockHandleSequential(changes, cancellationToken);
        protected sealed override Task HandleSequentialByPartition(IReadOnlyCollection<DbDoc> changes, CancellationToken cancellationToken) => MockHandleSequentialByPartition(changes, cancellationToken);

        public virtual Task MockHandleAsync(IReadOnlyCollection<DbDoc> changes, CancellationToken cancellationToken) => base.HandleAsync(changes, cancellationToken);
        public virtual Task MockHandleAllAtOnce(IReadOnlyCollection<DbDoc> changes, CancellationToken cancellationToken) => base.HandleAllAtOnce(changes, cancellationToken);
        public virtual Task MockHandleSequential(IEnumerable<DbDoc> changes, CancellationToken cancellationToken) => base.HandleSequential(changes, cancellationToken);
        public virtual Task MockHandleSequentialByPartition(IReadOnlyCollection<DbDoc> changes, CancellationToken cancellationToken) => base.HandleSequentialByPartition(changes, cancellationToken);

        protected sealed override BatchHandlersBase BatchHandlers => base.BatchHandlers;
        public virtual BatchHandlersBase MockBatchHandlers => base.BatchHandlers;
    }

    readonly Func<CancellationToken, Task> Cancel = _ => Task.CompletedTask;

    [Test]
    public async Task HandleAsync_EmptyChanges_ReturnsSynchronously()
    {
        var feed = new Mock<TestChangeFeed>(MockBehavior.Strict, new FeedHandlers(Cancel, Cancel));
        feed.Setup(x => x.MockHandleAsync(Array.Empty<DbDoc>(), It.IsAny<CancellationToken>())).CallBase();
        var result = feed.Object.MockHandleAsync([], default);

        await Assert.That(result.IsCompletedSuccessfully).IsTrue();
    }

    bool HandleNewChangeFeedBatchCalled;
    bool HandleFinishingBatchCalled;
    bool HandleCalled;

    async Task HandleNew(CancellationToken cancellationToken)
    {
        await Assert.That(HandleNewChangeFeedBatchCalled).IsFalse();
        await Assert.That(HandleFinishingBatchCalled).IsFalse();
        await Assert.That(HandleCalled).IsFalse();
        HandleNewChangeFeedBatchCalled = true;
    }
    async Task HandleFinish(CancellationToken cancellationToken)
    {
        await Assert.That(HandleNewChangeFeedBatchCalled).IsTrue();
        await Assert.That(HandleFinishingBatchCalled).IsFalse();
        await Assert.That(HandleCalled).IsTrue();
        HandleFinishingBatchCalled = true;
    }
    async Task HandleDocs(IReadOnlyCollection<DbDoc> changes, CancellationToken cancellationToken)
    {
        await Assert.That(HandleNewChangeFeedBatchCalled).IsTrue();
        await Assert.That(HandleFinishingBatchCalled).IsFalse();
        await Assert.That(HandleCalled).IsFalse();
        HandleCalled = true;
    }
    public class FeedHandlers : BatchHandlersBase
    {
        public FeedHandlers(Func<CancellationToken, Task> handleNew, Func<CancellationToken, Task> handleFinish)
        {
            FinishingBatch = handleFinish;
            NewChangeFeedBatch = handleNew;
        }
        public override Task? GetHandlerTask(DbDoc change, CancellationToken cancellationToken = default) => throw new NotImplementedException();
    }
    FeedHandlers Handlers => new(HandleNew, HandleFinish);

    readonly TestDoc[] ChangedDocs =
    [
        new TestDoc{ pk = "a"},
        new TestDoc{ pk = "b"},
        new TestDoc{ pk = "a"},
    ];

    [Test]
    public async Task HandleAsync_Changes_AllAtOnce()
    {
        var feed = new Mock<TestChangeFeed>(MockBehavior.Loose, Handlers);
        feed.Setup(x => x.MockHandleAsync(ChangedDocs, It.IsAny<CancellationToken>())).CallBase();
        feed.Setup(x => x.ProcessingMode).Returns(BatchProcessingMode.AllAtOnce).Verifiable();
        feed
            .Setup(x => x.MockHandleAllAtOnce(ChangedDocs, It.IsAny<CancellationToken>()))
            .Returns((IReadOnlyCollection<DbDoc> changes, CancellationToken cancellationToken) => HandleDocs(changes, cancellationToken))
            .Verifiable();

        await feed.Object.MockHandleAsync(ChangedDocs, default);

        feed.Verify();
        await Assert.That(HandleNewChangeFeedBatchCalled).IsTrue();
        await Assert.That(HandleFinishingBatchCalled).IsTrue();
        await Assert.That(HandleCalled).IsTrue();
    }

    [Test]
    public async Task HandleAsync_Changes_SequentialByPartition()
    {
        var feed = new Mock<TestChangeFeed>(MockBehavior.Strict, Handlers);
        feed.Setup(x => x.MockHandleAsync(ChangedDocs, It.IsAny<CancellationToken>())).CallBase();
        feed.Setup(x => x.ProcessingMode).Returns(BatchProcessingMode.SequentialByPartition).Verifiable();
        feed
            .Setup(x => x.MockHandleSequentialByPartition(ChangedDocs, It.IsAny<CancellationToken>()))
            .Returns((IReadOnlyCollection<DbDoc> changes, CancellationToken cancellationToken) => HandleDocs(changes, cancellationToken))
            .Verifiable();

        await feed.Object.MockHandleAsync(ChangedDocs, default);

        feed.Verify();
        await Assert.That(HandleNewChangeFeedBatchCalled).IsTrue();
        await Assert.That(HandleFinishingBatchCalled).IsTrue();
        await Assert.That(HandleCalled).IsTrue();
    }

    [Test]
    public async Task HandleAsync_Changes_Sequential()
    {
        var feed = new Mock<TestChangeFeed>(MockBehavior.Strict, Handlers);
        feed.Setup(x => x.MockHandleAsync(ChangedDocs, It.IsAny<CancellationToken>())).CallBase();
        feed.Setup(x => x.ProcessingMode).Returns(BatchProcessingMode.Sequential).Verifiable();
        feed
            .Setup(x => x.MockHandleSequential(ChangedDocs, It.IsAny<CancellationToken>()))
            .Returns((IReadOnlyCollection<DbDoc> changes, CancellationToken cancellationToken) => HandleDocs(changes, cancellationToken))
            .Verifiable();

        await feed.Object.MockHandleAsync(ChangedDocs, default);

        feed.Verify();
        await Assert.That(HandleNewChangeFeedBatchCalled).IsTrue();
        await Assert.That(HandleFinishingBatchCalled).IsTrue();
        await Assert.That(HandleCalled).IsTrue();
    }

    readonly TaskCompletionSource<bool> TaskCompletionSource = new();

    [Test]
    public async Task HandleAllAtOnce()
    {
        var called = new List<DbDoc>();
        var explode = Task.Delay(30000).ContinueWith(_ => { if (!TaskCompletionSource.Task.IsCompleted) { throw new Exception(); } });
        var handlers = new Mock<FeedHandlers>(MockBehavior.Loose, (object)HandleNew, (object)HandleFinish);
        handlers
            .Setup(x => x.GetHandlerTask(It.IsAny<DbDoc>(), It.IsAny<CancellationToken>()))
            .Returns((DbDoc doc, CancellationToken c) =>
            {
                lock (called)
                {
                    called.Add(doc);
                    if (called.Count == ChangedDocs.Length)
                    {
                        TaskCompletionSource.SetResult(true);
                    }
                }
                return Task.WhenAny(explode, TaskCompletionSource.Task);
            })
            .Verifiable();
        var feed = new Mock<TestChangeFeed>(MockBehavior.Strict, handlers.Object);
        feed.Setup(x => x.MockHandleAllAtOnce(ChangedDocs, It.IsAny<CancellationToken>())).CallBase();

        await feed.Object.MockHandleAllAtOnce(ChangedDocs, default);

        feed.Verify();
        handlers.Verify();
    }

    [Test]
    public async Task HandleSequential()
    {
        var explode = Task.Delay(30000).ContinueWith(_ => { if (!TaskCompletionSource.Task.IsCompleted) { throw new Exception(); } });
        var handlers = new Mock<FeedHandlers>(MockBehavior.Loose, (object)HandleNew, (object)HandleFinish);
        var previousTask = Task.CompletedTask;
        var fail = false;
        handlers
            .Setup(x => x.GetHandlerTask(It.IsAny<DbDoc>(), It.IsAny<CancellationToken>()))
            .Returns((DbDoc doc, CancellationToken c) =>
            {
                if (!previousTask.IsCompletedSuccessfully) { fail = true; }
                return previousTask = Task.Delay(10, default);
            })
            .Verifiable();
        var feed = new Mock<TestChangeFeed>(MockBehavior.Strict, handlers.Object);
        feed.Setup(x => x.MockHandleSequential(ChangedDocs, It.IsAny<CancellationToken>())).CallBase();

        await Assert.That(fail).IsFalse();

        await feed.Object.MockHandleSequential(ChangedDocs, default);

        feed.Verify();
        handlers.Verify();
    }

    [Test]
    public async Task HandleSequentialByPartition()
    {
        var handlers = new Mock<FeedHandlers>(MockBehavior.Loose, (object)HandleNew, (object)HandleFinish);
        var explode = Task.Delay(30000).ContinueWith(_ => { if (!TaskCompletionSource.Task.IsCompleted) { throw new Exception(); } });
        var previousTasksByPk = new Dictionary<string, List<Task>>();
        var fail = false;
        handlers
            .Setup(x => x.GetHandlerTask(It.IsAny<DbDoc>(), It.IsAny<CancellationToken>()))
            .Returns((DbDoc doc, CancellationToken c) =>
            {
                lock (previousTasksByPk)
                {
                    var prev = Task.Delay(10, default);
                    if (previousTasksByPk.TryGetValue(doc.pk, out var previousTasks))
                    {
                        foreach (var previousTask in previousTasks)
                        {
                            if (!previousTask.IsCompletedSuccessfully) { fail = true; }
                        }
                        previousTasks.Add(prev);
                    }
                    else
                    {
                        previousTasksByPk[doc.pk] = [prev];
                    }
                    return prev;
                }
            })
            .Verifiable();
        var feed = new Mock<TestChangeFeed>(MockBehavior.Strict, handlers.Object);
        feed.Setup(x => x.MockHandleSequentialByPartition(ChangedDocs, It.IsAny<CancellationToken>())).CallBase();
        feed.Setup(x => x.MockHandleSequential(It.IsAny<IEnumerable<DbDoc>>(), It.IsAny<CancellationToken>())).CallBase();

        await Assert.That(fail).IsFalse();

        await feed.Object.MockHandleSequentialByPartition(ChangedDocs, default);

        feed.Verify();
        handlers.Verify();
    }
#pragma warning restore IDE0060 // Remove unused parameter
}
