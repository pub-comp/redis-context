using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Payoneer.Infra.Repo.IntegrationTests
{
    [TestClass]
    public class RedisTests
    {
        private RedisContext redisContext;

        #region Initialization

        [TestInitialize]
        public void TestInitialize()
        {
            redisContext = new RedisContext(db: 1);
            ClearDb(redisContext);
        }

        public TestContext TestContext { get; set; }

        private static void ClearDb(RedisContext redisContext)
        {
            IEnumerable<string> keys = null;

            const int maxAttempts = 3;
            for (int attempts = 0; attempts < maxAttempts; attempts++)
            {
                try
                {
                    keys = redisContext.GetKeys();
                    break;
                }
                catch (StackExchange.Redis.RedisTimeoutException)
                {
                    if (attempts < maxAttempts - 1)
                        Thread.Sleep(100);
                    else
                        throw;
                }
            }
            redisContext.Delete(keys.ToArray());
        }

        #endregion

        #region Test Cases

        [TestMethod]
        public void SetTryGetString()
        {
            SetTryGetTest("valU");
        }

        [TestMethod]
        public void SetTryGetStringTtl()
        {
            SetTryGetTest("valU", TimeSpan.FromSeconds(1.0));
        }

        [TestMethod]
        public void SetTryGetInt()
        {
            SetTryGetTest(5);
        }

        [TestMethod]
        public void SetTryGetNullableInt()
        {
            SetTryGetTest((int?)4);
        }

        [TestMethod]
        public void SetTryGetNullableIntNull()
        {
            SetTryGetTest((int?)null);
        }

        [TestMethod]
        public void SetTryGetLong()
        {
            SetTryGetTest(-3L);
        }

        [TestMethod]
        public void SetTryGetNullableLong()
        {
            SetTryGetTest((long?)-7L);
        }

        [TestMethod]
        public void SetTryGetNullableLongNull()
        {
            SetTryGetTest((long?)null);
        }

        [TestMethod]
        public void SetTryGetDouble()
        {
            SetTryGetTest(1.4);
        }

        [TestMethod]
        public void SetTryGetNullableDouble()
        {
            SetTryGetTest((double?)0.6);
        }

        [TestMethod]
        public void SetTryGetNullableDoubleNull()
        {
            SetTryGetTest((double?)null);
        }

        [TestMethod]
        public void SetTryGetBoolTrue()
        {
            SetTryGetTest(true);
        }

        [TestMethod]
        public void SetTryGetNullableBoolTrue()
        {
            SetTryGetTest((bool?)true);
        }

        [TestMethod]
        public void SetTryGetBoolFalse()
        {
            SetTryGetTest(false);
        }

        [TestMethod]
        public void SetTryGetNullableBoolFalse()
        {
            SetTryGetTest((bool?)false);
        }

        [TestMethod]
        public void SetTryGetNullableBoolNull()
        {
            SetTryGetTest((bool?)null);
        }

        #endregion

        private void SetTryGetTest<TData>(TData value, TimeSpan? ttl = null)
        {
            var key = TestContext.TestName;
            bool result = false;
            object resultValueData = null;

            if (typeof(TData) == typeof(string))
            {
                redisContext.Set(key, value as string, ttl);
                TtlSleep(ttl);
                result = redisContext.TryGet(key, out string resultValue);
                resultValueData = resultValue;
            }
            else if (typeof(TData) == typeof(int?))
            {
                redisContext.Set(key, value as int?, ttl);
                TtlSleep(ttl);
                result = redisContext.TryGet(key, out int? resultValue);
                resultValueData = resultValue;
            }
            else if (typeof(TData) == typeof(int))
            {
                redisContext.Set(key, Convert.ToInt32(value), ttl);
                TtlSleep(ttl);
                result = redisContext.TryGet(key, out int resultValue);
                resultValueData = resultValue;
            }
            else if (typeof(TData) == typeof(long?))
            {
                redisContext.Set(key, value as long?, ttl);
                TtlSleep(ttl);
                result = redisContext.TryGet(key, out long? resultValue);
                resultValueData = resultValue;
            }
            else if (typeof(TData) == typeof(long))
            {
                redisContext.Set(key, Convert.ToInt64(value), ttl);
                TtlSleep(ttl);
                result = redisContext.TryGet(key, out long resultValue);
                resultValueData = resultValue;
            }
            else if (typeof(TData) == typeof(double?))
            {
                redisContext.Set(key, value as double?, ttl);
                TtlSleep(ttl);
                result = redisContext.TryGet(key, out double? resultValue);
                resultValueData = resultValue;
            }
            else if (typeof(TData) == typeof(double))
            {
                redisContext.Set(key, Convert.ToDouble(value), ttl);
                TtlSleep(ttl);
                result = redisContext.TryGet(key, out double resultValue);
                resultValueData = resultValue;
            }
            else if (typeof(TData) == typeof(bool?))
            {
                redisContext.Set(key, value as bool?, ttl);
                TtlSleep(ttl);
                result = redisContext.TryGet(key, out bool? resultValue);
                resultValueData = resultValue;
            }
            else if (typeof(TData) == typeof(bool))
            {
                redisContext.Set(key, Convert.ToBoolean(value), ttl);
                TtlSleep(ttl);
                result = redisContext.TryGet(key, out bool resultValue);
                resultValueData = resultValue;
            }

            if (ttl == null)
            {
                Assert.IsTrue(result);
                Assert.AreEqual(value, resultValueData);
            }
            else
            {
                Assert.IsFalse(result);
            }
        }

        private void TtlSleep(TimeSpan? ttl)
        {
            if (ttl.HasValue)
                Thread.Sleep((int)Math.Ceiling(ttl.Value.TotalMilliseconds) + 501);
        }
    }
}
