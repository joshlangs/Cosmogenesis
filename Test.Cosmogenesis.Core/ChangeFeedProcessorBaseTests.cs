using Microsoft.Azure.Cosmos;
using Moq;

namespace Test.Cosmogenesis.Core;

public class ChangeFeedProcessorBaseTests
{
    public class TestChangeFeed(DbSerializerBase serializer, Container databaseContainer, Container leaseContainer, string processorName, int maxItemsPerBatch, TimeSpan? pollInterval, DateTime? startTime, BatchProcessor batchProcessor) : ChangeFeedProcessorBase(batchProcessor, serializer, databaseContainer, leaseContainer, processorName, maxItemsPerBatch, pollInterval, startTime)
    {
        protected sealed override ChangeFeedProcessor CreateChangeFeedProcessor() => MockCreateChangeFeedProcessor();
        public virtual ChangeFeedProcessor MockCreateChangeFeedProcessor() => throw new NotImplementedException();

        public virtual BatchProcessor MockBatchProcessor => base.BatchProcessor;

        protected sealed override TimeSpan PollInterval => base.PollInterval;
        protected sealed override BatchProcessor BatchProcessor => base.BatchProcessor;
        protected sealed override ChangeFeedProcessor ChangeFeedProcessor => base.ChangeFeedProcessor;
        protected sealed override int MaxItemsPerBatch => base.MaxItemsPerBatch;
        protected sealed override DateTime StartTime => base.StartTime;
    }

    readonly Mock<Container> MockDatabaseContainer = new(MockBehavior.Strict);
    readonly Mock<Container> MockLeaseContainer = new(MockBehavior.Strict);
    readonly Mock<ChangeFeedProcessor> MockProcessor = new(MockBehavior.Strict);
    readonly Mock<BatchProcessor> MockHandlers = new();
    readonly Mock<DbSerializerBase> MockSerializer = new();

    [Test]
    public async Task DefaultPollInterval_BetweenMinAndMax() => await Assert.That(ChangeFeedProcessorBase.DefaultPollInterval >= ChangeFeedProcessorBase.MinPollInterval && ChangeFeedProcessorBase.DefaultPollInterval <= ChangeFeedProcessorBase.MaxPollInterval).IsTrue();

    [Test]
    public async Task MinPollInterval_NotNegative() => await Assert.That(ChangeFeedProcessorBase.MinPollInterval >= TimeSpan.Zero).IsTrue();

    [Test]
    public async Task Ctor_NullDatabaseContainer_Throws() => Assert.Throws<ArgumentNullException>(() => new TestChangeFeed(MockSerializer.Object, null!, MockLeaseContainer.Object, "asdf", 5, null, null, MockHandlers.Object));

    [Test]
    public async Task Ctor_NullLeaseContainer_Throws() => Assert.Throws<ArgumentNullException>(() => new TestChangeFeed(MockSerializer.Object, MockDatabaseContainer.Object, null!, "asdf", 5, null, null, MockHandlers.Object));

    [Test]
    public async Task Ctor_NullProcessorName_Throws() => Assert.Throws<ArgumentNullException>(() => new TestChangeFeed(MockSerializer.Object, MockDatabaseContainer.Object, MockLeaseContainer.Object, null!, 5, null, null, MockHandlers.Object));

    [Test]
    public async Task Ctor_ZeroMaxItems_Throws() => Assert.Throws<ArgumentOutOfRangeException>(() => new TestChangeFeed(MockSerializer.Object, MockDatabaseContainer.Object, MockLeaseContainer.Object, "asdf", 0, null, null, MockHandlers.Object));

    [Test]
    public async Task Ctor_NegativePollInterval_Throws() => Assert.Throws<ArgumentOutOfRangeException>(() => new TestChangeFeed(MockSerializer.Object, MockDatabaseContainer.Object, MockLeaseContainer.Object, "asdf", 5, TimeSpan.FromSeconds(-1), null, MockHandlers.Object));

    readonly Func<CancellationToken, Task> Cancel = _ => Task.CompletedTask;
    public class FeedHandlers : BatchHandlersBase
    {
        public FeedHandlers(Func<CancellationToken, Task> handleNew, Func<CancellationToken, Task> handleFinish)
        {
            FinishingBatch = handleFinish;
            NewChangeFeedBatch = handleNew;
        }
        public override Task? GetHandlerTask(DbDoc change, CancellationToken cancellationToken = default) => throw new NotImplementedException();
    }

    [Test]
    public async Task StartAsync_CallsStartAsync()
    {
        MockProcessor.Setup(x => x.StartAsync()).Returns(Task.CompletedTask).Verifiable();
        var feed = new Mock<TestChangeFeed>(MockBehavior.Strict, MockSerializer.Object, MockDatabaseContainer.Object, MockLeaseContainer.Object, "asdf", 5, null!, null!, new BatchProcessor(new FeedHandlers(Cancel, Cancel)));
        feed.Setup(x => x.MockCreateChangeFeedProcessor()).Returns(MockProcessor.Object).Verifiable();
        feed.Setup(x => x.StartAsync()).CallBase();

        await Assert.That(feed.Object.StartAsync()).IsEqualTo(Task.CompletedTask);

        MockProcessor.Verify();
    }

    [Test]
    public async Task StopAsync_CallsStopAsync()
    {
        MockProcessor.Setup(x => x.StopAsync()).Returns(Task.CompletedTask).Verifiable();
        var feed = new Mock<TestChangeFeed>(MockBehavior.Strict, MockSerializer.Object, MockDatabaseContainer.Object, MockLeaseContainer.Object, "asdf", 5, null!, null!, new BatchProcessor(new FeedHandlers(Cancel, Cancel)));
        feed.Setup(x => x.MockCreateChangeFeedProcessor()).Returns(MockProcessor.Object).Verifiable();
        feed.Setup(x => x.StopAsync()).CallBase();

        await Assert.That(feed.Object.StopAsync()).IsEqualTo(Task.CompletedTask);

        MockProcessor.Verify();
    }
}
