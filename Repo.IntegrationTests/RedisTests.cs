using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using StackExchange.Redis;

namespace Payoneer.Infra.Repo.IntegrationTests
{
    [TestClass]
    public class RedisTests
    {
        private RedisTestContext redisContext;

        #region Initialization

        [TestInitialize]
        public void TestInitialize()
        {
            redisContext = RedisTestContext.Retry(() => new RedisTestContext(db: 1), 5);
            ClearDb(redisContext, TestContext);
        }

        [TestCleanup]
        public void TestCleanup()
        {
            redisContext?.Connection?.Dispose();
        }

        public TestContext TestContext { get; set; }

        private static void ClearDb(RedisContext redisContext, TestContext testContext)
        {
            var keys = RedisTestContext.Retry(() => redisContext.GetKeys(testContext.TestName + '*'), 5);
            redisContext.Delete(keys.ToArray());
        }

        public class RedisTestContext : RedisContext
        {
            public RedisTestContext(int db) : base(db: db)
            {
            }

            public new IConnectionMultiplexer Connection => base.Connection;

            public new IDatabase Database => base.Database;

            public new static TResult Retry<TResult>(Func<TResult> func, int maxAttempts)
            {
                return RedisContext.Retry(func, maxAttempts);
            }

            public new static void Retry(Action action, int maxAttempts)
            {
                RedisContext.Retry(action, maxAttempts);
            }
        }

        #endregion

        #region Test Cases

        #region SetTryGet

        [TestMethod]
        public void SetTryGetString()
        {
            SetTryGetTest("valU");
        }

        [TestMethod]
        public void SetTryGetStringDelete()
        {
            SetTryGetTest("valU", doDelete: true);
        }

        [TestMethod]
        public void SetTryGetStringTtl()
        {
            SetTryGetTest("valU", TimeSpan.FromSeconds(1.0));
        }

        [TestMethod]
        public void SetTryGetStringWithSeparateTtlCommand()
        {
            SetTryGetTest("valU", TimeSpan.FromSeconds(1.0), doSetTtlInSeparateCommand: true);
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

        #region IncDec

        [TestMethod]
        public void IncDec_InitIncLong()
        {
            IncDecTest(doSetInitialValue: true, initialValue: 2L, incrementBy: 3L,
                doInc: true, doDec: false, expectedIncResult: 5L, expectedFinalResult: 5L);
        }

        [TestMethod]
        public void IncDec_InitIncDouble()
        {
            IncDecTest(doSetInitialValue: true, initialValue: 3.7, incrementBy: 1.1,
                doInc: true, doDec: false, expectedIncResult: 4.8, expectedFinalResult: 4.8);
        }

        [TestMethod]
        public void IncDec_IncLong()
        {
            IncDecTest(doSetInitialValue: false, incrementBy: 3L,
                doInc: true, doDec: false, expectedIncResult: 3L, expectedFinalResult: 3L);
        }

        [TestMethod]
        public void IncDec_IncDouble()
        {
            IncDecTest(doSetInitialValue: false, incrementBy: 1.1,
                doInc: true, doDec: false, expectedIncResult: 1.1, expectedFinalResult: 1.1);
        }

        [TestMethod]
        public void IncDec_InitDecLong()
        {
            IncDecTest(doSetInitialValue: true, initialValue: 5L, decrementBy: 3L,
                doInc: false, doDec: true, expectedDecResult: 2L, expectedFinalResult: 2L);
        }

        [TestMethod]
        public void IncDec_InitDecDouble()
        {
            IncDecTest(doSetInitialValue: true, initialValue: 3.7, decrementBy: 1.1,
                doInc: false, doDec: true, expectedDecResult: 2.6, expectedFinalResult: 2.6);
        }

        [TestMethod]
        public void IncDec_DecLong()
        {
            IncDecTest(doSetInitialValue: false, decrementBy: 3L,
                doInc: false, doDec: true, expectedDecResult: -3L, expectedFinalResult: -3L);
        }

        [TestMethod]
        public void IncDec_DecDouble()
        {
            IncDecTest(doSetInitialValue: false, decrementBy: 1.1,
                doInc: false, doDec: true, expectedDecResult: -1.1, expectedFinalResult: -1.1);
        }

        #endregion

        #endregion

        private void Set<TData>(string key, TData value, TimeSpan? ttl = null)
        {
            if (typeof(TData) == typeof(string))
            {
                redisContext.Set(key, value as string, ttl);
            }
            else if (typeof(TData) == typeof(int?))
            {
                redisContext.Set(key, value as int?, ttl);
            }
            else if (typeof(TData) == typeof(int))
            {
                redisContext.Set(key, Convert.ToInt32(value), ttl);
            }
            else if (typeof(TData) == typeof(long?))
            {
                redisContext.Set(key, value as long?, ttl);
            }
            else if (typeof(TData) == typeof(long))
            {
                redisContext.Set(key, Convert.ToInt64(value), ttl);
            }
            else if (typeof(TData) == typeof(double?))
            {
                redisContext.Set(key, value as double?, ttl);
            }
            else if (typeof(TData) == typeof(double))
            {
                redisContext.Set(key, Convert.ToDouble(value), ttl);
            }
            else if (typeof(TData) == typeof(bool?))
            {
                redisContext.Set(key, value as bool?, ttl);
            }
            else if (typeof(TData) == typeof(bool))
            {
                redisContext.Set(key, Convert.ToBoolean(value), ttl);
            }
            else throw new NotSupportedException($"{nameof(TData)} is not supported");
        }

        private bool TryGet<TData>(string key, out TData value)
        {
            bool result = false;
            object resultValueData = null;

            if (typeof(TData) == typeof(string))
            {
                result = redisContext.TryGet(key, out string resultValue);
                resultValueData = resultValue;
            }
            else if (typeof(TData) == typeof(int?))
            {
                result = redisContext.TryGet(key, out int? resultValue);
                resultValueData = resultValue;
            }
            else if (typeof(TData) == typeof(int))
            {
                result = redisContext.TryGet(key, out int resultValue);
                resultValueData = resultValue;
            }
            else if (typeof(TData) == typeof(long?))
            {
                result = redisContext.TryGet(key, out long? resultValue);
                resultValueData = resultValue;
            }
            else if (typeof(TData) == typeof(long))
            {
                result = redisContext.TryGet(key, out long resultValue);
                resultValueData = resultValue;
            }
            else if (typeof(TData) == typeof(double?))
            {
                result = redisContext.TryGet(key, out double? resultValue);
                resultValueData = resultValue;
            }
            else if (typeof(TData) == typeof(double))
            {
                result = redisContext.TryGet(key, out double resultValue);
                resultValueData = resultValue;
            }
            else if (typeof(TData) == typeof(bool?))
            {
                result = redisContext.TryGet(key, out bool? resultValue);
                resultValueData = resultValue;
            }
            else if (typeof(TData) == typeof(bool))
            {
                result = redisContext.TryGet(key, out bool resultValue);
                resultValueData = resultValue;
            }
            else throw new NotSupportedException($"{nameof(TData)} is not supported");

            value = (TData)resultValueData;
            return result;
        }

        private void SetTryGetTest<TData>(TData value, TimeSpan? ttl = null,
            bool doSetTtlInSeparateCommand = false, bool doDelete = false)
        {
            var key = TestContext.TestName;

            bool doOverrideTtl = ttl.HasValue && doSetTtlInSeparateCommand;
            var ttl1 = doOverrideTtl ? null : ttl;

            Set(key, value, ttl1);

            if (doOverrideTtl)
                redisContext.SetTimeToLive(key, ttl);

            if (doDelete)
                redisContext.Delete(key);

            TtlSleep(ttl);
            var result = TryGet<TData>(key, out TData resultValue);

            if (ttl == null && !doDelete)
            {
                Assert.IsTrue(result);
                Assert.AreEqual(value, resultValue);
            }
            else
            {
                Assert.IsFalse(result);
            }
        }

        private void IncDecTest<TData>(
            bool doSetInitialValue, TData initialValue = default(TData),
            TData incrementBy = default(TData), TData decrementBy = default(TData),
            bool doInc = false, bool doDec = false,
            TData expectedIncResult = default(TData),
            TData expectedDecResult = default(TData),
            TData expectedFinalResult = default(TData))
        {
            var key = TestContext.TestName;

            if (doSetInitialValue)
                Set(key, initialValue);

            object incResult = null;
            object decResult = null;

            if (typeof(TData) == typeof(long))
            {
                if (doInc)
                    incResult = redisContext.Increment(key, Convert.ToInt64(incrementBy));

                if (doDec)
                    decResult = redisContext.Decrement(key, Convert.ToInt64(decrementBy));

                var getResult = TryGet(key, out TData resultValue);

                if (doInc)
                    Assert.AreEqual(expectedIncResult, (TData)incResult);

                if (doDec)
                    Assert.AreEqual(expectedDecResult, (TData)decResult);

                if (doSetInitialValue || doInc || doDec)
                {
                    Assert.IsTrue(getResult);
                }
                else
                {
                    Assert.IsFalse(getResult);
                }

                Assert.AreEqual(expectedFinalResult, resultValue);
            }
            else if (typeof(TData) == typeof(double))
            {
                if (doInc)
                    incResult = redisContext.Increment(key, Convert.ToDouble(incrementBy));

                if (doDec)
                    decResult = redisContext.Decrement(key, Convert.ToDouble(decrementBy));

                var getResult = TryGet(key, out TData resultValue);

                if (doInc)
                    AssertAreSimilar(Convert.ToDouble(expectedIncResult), Convert.ToDouble(incResult));

                if (doDec)
                    AssertAreSimilar(Convert.ToDouble(expectedDecResult), Convert.ToDouble(decResult));

                if (doSetInitialValue || doInc || doDec)
                {
                    Assert.IsTrue(getResult);
                }
                else
                {
                    Assert.IsFalse(getResult);
                }

                AssertAreSimilar(Convert.ToDouble(expectedFinalResult), Convert.ToDouble(resultValue));
            }
            else throw new NotSupportedException($"{nameof(TData)} is not supported");
        }

        private void AssertAreSimilar(double expected, double actual)
        {
            if (Math.Abs(expected - actual) >= 1e-3)
                Assert.Fail($"Expected: {expected}, Actual: {actual}");
        }

        private void TtlSleep(TimeSpan? ttl)
        {
            if (ttl.HasValue)
                Thread.Sleep((int)Math.Ceiling(ttl.Value.TotalMilliseconds) + 501);
        }
    }
}
