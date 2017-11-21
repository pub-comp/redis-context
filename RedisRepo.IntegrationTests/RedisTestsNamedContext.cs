using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Payoneer.Infra.Repo.IntegrationTests
{
    [TestClass]
    public class RedisTestsNamedContext : RedisTests
    {
        [TestInitialize]
        public void TestInitialize()
        {
            redisContext = RedisTestContext.Retry(()
                => new RedisTestContext(nameof(RedisTestsNamedContext), db: 1), 5);
            ClearDb(redisContext, TestContext);

            redisContext.Delete(TestContext.TestName);
        }

        [TestCleanup]
        public void TestCleanup()
        {
            redisContext?.Connection?.Dispose();
        }
    }
}
