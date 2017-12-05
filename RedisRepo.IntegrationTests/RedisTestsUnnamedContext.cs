using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Payoneer.Infra.RedisRepo.IntegrationTests
{
    [TestClass]
    public class RedisTestsUnnamedContext : RedisTests
    {
        [TestInitialize]
        public void TestInitialize()
        {
            redisContext = RedisTestContext.Retry(()
                => new RedisTestContext(null, db: 2), 5);
            ClearDb(redisContext, TestContext);

            redisContext.Delete(TestContext.TestName);
        }

        [TestCleanup]
        public void TestCleanup()
        {
            (redisContext as RedisTestContext)?.Connection?.Dispose();
        }
    }
}
