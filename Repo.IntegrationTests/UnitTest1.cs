using System;
using System.Linq;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Payoneer.Infra.Repo.IntegrationTests
{
    [TestClass]
    public class UnitTest1
    {
        private RedisContext redisContext;

        [TestInitialize]
        public void TestInitialize()
        {
            redisContext = new RedisContext(db: 1);
            var keys = redisContext.GetKeys();
            redisContext.Delete(keys.ToArray());
        }

        [TestMethod]
        public void TestMethod1()
        {
            redisContext.Set("key", "valU");

            var result = redisContext.TryGet("key", out string value);

            Assert.IsTrue(result);
            Assert.AreEqual("valU", value);
        }

        [TestMethod]
        public void TestMethod2()
        {
            redisContext.Set("key2", "valU", TimeSpan.FromSeconds(20.0));

            var result = redisContext.TryGet("key2", out string value);

            Assert.IsTrue(result);
            Assert.AreEqual("valU", value);
        }
    }
}
