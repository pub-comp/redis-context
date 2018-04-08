using System;
using System.Linq;
using System.Threading;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using StackExchange.Redis;

namespace Payoneer.Infra.RedisRepo.IntegrationTests
{
    public abstract class RedisTests
    {
        // ReSharper disable once InconsistentNaming
        protected IRedisContext redisContext;

        #region Initialization

        // ReSharper disable once UnusedAutoPropertyAccessor.Global
        public TestContext TestContext { get; set; }

        protected static void ClearDb(IRedisContext redisContext, TestContext testContext)
        {
            var keys = RedisTestContext.Retry(() => redisContext.GetKeys(testContext.TestName + '*'), 5);
            redisContext.Delete(keys.ToArray());
        }

        public class RedisTestContext : RedisContext
        {
            public RedisTestContext(string name, int db) : base(name, db: db)
            {
            }

            public new IConnectionMultiplexer Connection => base.Connection;

            public new IDatabase Database => base.Database;

            public static TResult Retry<TResult>(Func<TResult> func, int maxAttempts)
            {
                return RetryUtil.Retry(func, maxAttempts);
            }

            public static void Retry(Action action, int maxAttempts)
            {
                RetryUtil.Retry(action, maxAttempts);
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
        public void SetTryGetStringNull()
        {
            SetTryGetTest((string)null);
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

        #region MyRegion
        
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
        public void SetGetTTl()
        {
            Set(TestContext.TestName, "123", TimeSpan.FromSeconds(5.0));
            var ttl = redisContext.GetTimeToLive(TestContext.TestName);
            Assert.IsTrue(ttl.HasValue);
            Assert.IsTrue(ttl.Value <= TimeSpan.FromSeconds(5.0));
            Assert.IsTrue(ttl.Value >= TimeSpan.FromSeconds(1.0));
        }

        [TestMethod]
        public void SetGetNoTTl()
        {
            Set(TestContext.TestName, "123");
            var ttl = redisContext.GetTimeToLive(TestContext.TestName);
            Assert.IsFalse(ttl.HasValue);
        }

        #endregion

        #region Delete

        [TestMethod]
        public void SetTryGetStringDelete()
        {
            SetTryGetTest("valU", doDelete: true);
        }

        [TestMethod]
        public void SetDeleteMany()
        {
            Set(TestContext.TestName + ".1", 1);
            Set(TestContext.TestName + ".2", "two");
            Set(TestContext.TestName + ".3", 3.0);
            
            var key1There = TryGet(TestContext.TestName + ".1", out int _);
            var key2There = TryGet(TestContext.TestName + ".2", out string _);
            var key3There = TryGet(TestContext.TestName + ".3", out double _);

            Assert.IsTrue(key1There);
            Assert.IsTrue(key2There);
            Assert.IsTrue(key3There);

            redisContext.Delete(
                TestContext.TestName + ".1",
                TestContext.TestName + ".3");

            var key1StillThere = TryGet(TestContext.TestName + ".1", out int _);
            var key2StillThere = TryGet(TestContext.TestName + ".2", out string _);
            var key3StillThere = TryGet(TestContext.TestName + ".3", out double _);

            Assert.IsFalse(key1StillThere);
            Assert.IsTrue(key2StillThere);
            Assert.IsFalse(key3StillThere);
        }

        #endregion

        #region SetExchangeTryGet

        [TestMethod]
        public void SetExchangeTryGetString()
        {
            SetTryGetTest("valU", doExchange: true, newValue: "newV");
        }
        
        [TestMethod]
        public void SetExchangeTryGetStringNullThenNot()
        {
            SetTryGetTest(null, doExchange: true, newValue: "newV");
        }

        [TestMethod][ExpectedException(typeof(ArgumentException))]
        public void SetExchangeTryGetStringNotThenNull()
        {
            SetTryGetTest("valU", doExchange: true, newValue: null);
        }

        [TestMethod]
        public void SetExchangeTryGetInt()
        {
            SetTryGetTest(5, doExchange: true, newValue: 7);
        }

        [TestMethod]
        public void SetExchangeTryGetNullableInt()
        {
            SetTryGetTest((int?)4, doExchange: true, newValue: (int?)2);
        }

        [TestMethod]
        public void SetExchangeTryGetNullableIntNullThenNot()
        {
            SetTryGetTest((int?)null, doExchange: true, newValue: (int?)9);
        }

        [TestMethod][ExpectedException(typeof(ArgumentException))]
        public void SetExchangeTryGetNullableIntNotThenNull()
        {
            SetTryGetTest((int?)8, doExchange: true, newValue: (int?)null);
        }

        [TestMethod]
        public void SetExchangeTryGetLong()
        {
            SetTryGetTest(-3L, doExchange: true, newValue: -4L);
        }

        [TestMethod]
        public void SetExchangeTryGetNullableLong()
        {
            SetTryGetTest((long?)-7L, doExchange: true, newValue: 2L);
        }

        [TestMethod][ExpectedException(typeof(ArgumentException))]
        public void SetExchangeTryGetNullableLongNull()
        {
            SetTryGetTest((long?)1L, doExchange: true, newValue: (long?)null);
        }

        [TestMethod]
        public void SetExchangeTryGetDouble()
        {
            SetTryGetTest(1.4, doExchange: true, newValue: 4.5);
        }

        [TestMethod]
        public void SetExchangeTryGetNullableDouble()
        {
            SetTryGetTest((double?)0.6, doExchange: true, newValue: (double?)-0.3);
        }

        [TestMethod][ExpectedException(typeof(ArgumentException))]
        public void SetExchangeTryGetNullableDoubleNull()
        {
            SetTryGetTest((double?)0.4, doExchange: true, newValue: (double?)null);
        }

        [TestMethod]
        public void SetExchangeTryGetBoolTrue()
        {
            SetTryGetTest(true, doExchange: true, newValue: false);
        }

        [TestMethod]
        public void SetExchangeTryGetBoolFalse()
        {
            SetTryGetTest(false, doExchange: true, newValue: true);
        }

        [TestMethod]
        public void SetExchangeTryGetNullableBoolTrue()
        {
            SetTryGetTest((bool?)true, doExchange: true, newValue: (bool?)false);
        }

        [TestMethod]
        public void SetExchangeTryGetNullableBoolFalse()
        {
            SetTryGetTest((bool?)false, doExchange: true, newValue: (bool?)true);
        }

        [TestMethod]
        public void SetExchangeTryGetNullableBoolNullThenNot()
        {
            SetTryGetTest((bool?)null, doExchange: true, newValue: (bool?)true);
        }

        [TestMethod][ExpectedException(typeof(ArgumentException))]
        public void SetExchangeTryGetNullableBoolNotThenNull()
        {
            SetTryGetTest((bool?)false, doExchange: true, newValue: (bool?)null);
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

        #region Append

        [TestMethod]
        public void Append_NoInit()
        {
            AppendTest(doSetInitialValue: false, appendSuffix: "-a",
                doAppend: true, expectedFinalResult: "-a");
        }

        [TestMethod]
        public void Append_Init()
        {
            AppendTest(initialValue: "1", doSetInitialValue: true, appendSuffix: "-a",
                doAppend: true, expectedFinalResult: "1-a");
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

        private TData AtomicExchange<TData>(string key, TData value)
        {
            if (typeof(TData) == typeof(string))
            {
                object result = redisContext.AtomicExchange(key, value as string);
                return (TData)result;
            }
            else if (typeof(TData) == typeof(int?))
            {
                object result = redisContext.AtomicExchange(key, value as int?);
                return (TData)result;
            }
            else if (typeof(TData) == typeof(int))
            {
                object result = redisContext.AtomicExchange(key, Convert.ToInt32(value));
                return (TData)result;
            }
            else if (typeof(TData) == typeof(long?))
            {
                object result = redisContext.AtomicExchange(key, value as long?);
                return (TData)result;
            }
            else if (typeof(TData) == typeof(long))
            {
                object result = redisContext.AtomicExchange(key, Convert.ToInt64(value));
                return (TData)result;
            }
            else if (typeof(TData) == typeof(double?))
            {
                object result = redisContext.AtomicExchange(key, value as double?);
                return (TData)result;
            }
            else if (typeof(TData) == typeof(double))
            {
                object result = redisContext.AtomicExchange(key, Convert.ToDouble(value));
                return (TData)result;
            }
            else if (typeof(TData) == typeof(bool?))
            {
                object result = redisContext.AtomicExchange(key, value as bool?);
                return (TData)result;
            }
            else if (typeof(TData) == typeof(bool))
            {
                object result = redisContext.AtomicExchange(key, Convert.ToBoolean(value));
                return (TData)result;
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

        private void SetTryGetTest<TData>(
            TData value, TimeSpan? ttl = null,
            bool doSetTtlInSeparateCommand = false, bool doDelete = false,
            bool doExchange = false, TData newValue = default(TData))
        {
            var retryAttempts = (doExchange) ? 5 : 1;
            RedisTestContext.Retry(() =>
                SetTryGetTestInner(
                    value, ttl, doSetTtlInSeparateCommand, doDelete, doExchange, newValue
                ), retryAttempts);
        }

        private void SetTryGetTestInner<TData>(
            TData value, TimeSpan? ttl,
            bool doSetTtlInSeparateCommand, bool doDelete,
            bool doExchange, TData newValue)
        {
            var key = TestContext.TestName;

            bool doOverrideTtl = ttl.HasValue && doSetTtlInSeparateCommand;
            var ttl1 = doOverrideTtl ? null : ttl;

            Set(key, value, ttl1);

            if (doOverrideTtl)
                redisContext.SetTimeToLive(key, ttl);

            TData exchangeResult = default(TData);
            if (doExchange)
                exchangeResult = AtomicExchange(key, newValue);

            if (doDelete)
                redisContext.Delete(key);

            TtlSleep(ttl);
            var result = TryGet<TData>(key, out TData resultValue);

            if (ttl == null && !doDelete
                && (!doExchange && value != null || doExchange && newValue != null))
            {
                Assert.IsTrue(result);

                if (!doExchange)
                {
                    Assert.AreEqual(value, resultValue);
                }
                else
                {
                    Assert.AreEqual(value, exchangeResult);
                    Assert.AreEqual(newValue, resultValue);
                }
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
            var retryAttempts = (doInc || doDec) ? 5 : 1;
            RedisTestContext.Retry(() =>
                IncDecTestInner(
                    doSetInitialValue, initialValue,
                    incrementBy, decrementBy,
                    doInc, doDec,
                    expectedIncResult,
                    expectedDecResult,
                    expectedFinalResult
                ), retryAttempts);
        }

        private void IncDecTestInner<TData>(
            bool doSetInitialValue, TData initialValue,
            TData incrementBy, TData decrementBy,
            bool doInc, bool doDec,
            TData expectedIncResult,
            TData expectedDecResult,
            TData expectedFinalResult)
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

        private void AppendTest(
            bool doSetInitialValue, string initialValue = default(string),
            string appendSuffix = default(string), bool doAppend = true,
            string expectedFinalResult = default(string))
        {
            var retryAttempts = (doAppend) ? 5 : 1;
            RedisTestContext.Retry(() =>
                AppendTestInner(
                    doSetInitialValue, initialValue, appendSuffix, doAppend, expectedFinalResult
                    ), retryAttempts);
        }

        private void AppendTestInner(
            bool doSetInitialValue, string initialValue,
            string appendSuffix, bool doAppend,
            string expectedFinalResult)
        {
            var key = TestContext.TestName;

            if (doSetInitialValue)
                Set(key, initialValue);

            if (doAppend)
                redisContext.SetOrAppend(key, appendSuffix);

            var getResult = TryGet(key, out string resultValue);

            if (doSetInitialValue || doAppend)
            {
                Assert.IsTrue(getResult);
            }
            else
            {
                Assert.IsFalse(getResult);
            }

            Assert.AreEqual(expectedFinalResult, resultValue);
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
