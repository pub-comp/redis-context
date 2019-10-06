using FluentAssertions;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using StackExchange.Redis;
using System;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Threading;

namespace PubComp.RedisRepo.IntegrationTests
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
            var concreteContext = (RedisTestContext)redisContext;
            Assert.IsTrue(concreteContext.IsLocal);
            var keys = RedisTestContext.Retry(() => redisContext.GetKeys("*"), 5);
            redisContext.Delete(keys.ToArray());
        }

        public class RedisTestContext : RedisContext
        {
            public RedisTestContext(string name, int db) : base(name, db: db)
            {
            }

            public new IConnectionMultiplexer Connection => base.Connection;

            public bool IsLocal
            {
                get
                {
                    var endpoints = base.Connection.GetEndPoints();
                    Assert.IsNotNull(endpoints);
                    Assert.AreEqual(1, endpoints.Length);

                    switch (endpoints[0])
                    {
                        case DnsEndPoint dns:
                            return dns.Host.ToLower() == "localhost";
                        case IPEndPoint ip:
                            {
                                var ipBytes = ip.Address.GetAddressBytes();
                                return ipBytes[0] == 127 && ipBytes[1] == 0 && ipBytes[2] == 0 && ipBytes[3] == 1;
                            }
                        default:
                            return false;
                    }
                }
            }

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

        #region TTL

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
        public void SetOperations_Double()
        {
            var key1 = TestContext.TestName + ".1";
            var key2 = TestContext.TestName + ".2";
            var key3 = TestContext.TestName + ".3";

            redisContext.Delete(key1);
            redisContext.Delete(key2);
            redisContext.Delete(key3);

            redisContext.SetAdd<double>(key1, new[] { 5.0, 2.0, 1.5 });
            redisContext.SetAdd<double>(key1, 3.5);

            CollectionAssert.AreEquivalent(
                new[] { 1.5, 2.0, 3.5, 5.0 },
                redisContext.SetGetItems(key1, RedisValueConverter.ToDouble)
                .OrderBy(x => x).ToList());

            redisContext.SetAdd<double>(key2, new[] { 7.0, 4.0, 1.5 });
            redisContext.SetAdd<double>(key3, new[] { 1.5, 7.0, 3.5, 8.5 });

            var actualIntersect123 = redisContext.SetsIntersect(
                new[] { key1, key2, key3 }, RedisValueConverter.ToDouble);
            CollectionAssert.AreEquivalent(
                new[] { 1.5 },
                actualIntersect123.OrderBy(x => x).ToList());

            var actualUnion123 = redisContext.SetsUnion(
                new[] { key1, key2, key3 }, RedisValueConverter.ToDouble);
            CollectionAssert.AreEquivalent(
                new[] { 1.5, 2.0, 3.5, 4.0, 5.0, 7.0, 8.5 },
                actualUnion123.OrderBy(x => x).ToList());

            var actualMinus123 = redisContext.SetsDiff(
                new[] { key1, key2, key3 }, RedisValueConverter.ToDouble);
            CollectionAssert.AreEquivalent(
                new[] { 2.0, 5.0 },
                actualMinus123.OrderBy(x => x).ToList());

            Assert.AreEqual(4, redisContext.SetLength(key1));
            Assert.AreEqual(3, redisContext.SetLength(key2));
            Assert.AreEqual(4, redisContext.SetLength(key3));

            redisContext.SetRemove<double>(key1, 2.0);
            Assert.AreEqual(3, redisContext.SetLength(key1));
            CollectionAssert.AreEquivalent(
                new[] { 1.5, 3.5, 5.0 },
                redisContext.SetGetItems(key1, RedisValueConverter.ToDouble)
                .OrderBy(x => x).ToList());

            redisContext.SetRemove<double>(key3, new[] { 2.0, 8.5, 7.0 });
            Assert.AreEqual(2, redisContext.SetLength(key3));
            CollectionAssert.AreEquivalent(
                new[] { 1.5, 3.5 },
                redisContext.SetGetItems(key3, RedisValueConverter.ToDouble)
                .OrderBy(x => x).ToList());

            redisContext.Delete(key2);
            redisContext.SetAdd<double>(key2, 9.0);
            Assert.AreEqual(1, redisContext.SetLength(key2));
            CollectionAssert.AreEquivalent(
                new[] { 9.0 },
                redisContext.SetGetItems(key2, RedisValueConverter.ToDouble)
                .OrderBy(x => x).ToList());
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

        [TestMethod]
        [ExpectedException(typeof(ArgumentException))]
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

        [TestMethod]
        [ExpectedException(typeof(ArgumentException))]
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

        [TestMethod]
        [ExpectedException(typeof(ArgumentException))]
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

        [TestMethod]
        [ExpectedException(typeof(ArgumentException))]
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

        [TestMethod]
        [ExpectedException(typeof(ArgumentException))]
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

        #region distributed lock

        [TestMethod]
        public void TestDistributedLockSuccess()
        {
            var res = redisContext.TryGetDistributedLock("object1", "myName", TimeSpan.FromSeconds(10));
            Assert.IsTrue(res);

            // same locker should be able to gain the lock again
            res = redisContext.TryGetDistributedLock("object1", "myName", TimeSpan.FromSeconds(10));
            Assert.IsTrue(res);
        }

        [TestMethod]
        public void TestDistributedLockFail()
        {
            var res = redisContext.TryGetDistributedLock("object1", "otherLocker", TimeSpan.FromSeconds(10));
            Assert.IsTrue(res);

            // other locker should not be able to gain the lock
            res = redisContext.TryGetDistributedLock("object1", "myName", TimeSpan.FromSeconds(10));
            Assert.IsFalse(res);
        }

        [TestMethod]
        public void TestDistributedLockSuccessAfterLockTimePasses()
        {
            var res = redisContext.TryGetDistributedLock("object1", "otherLocker", TimeSpan.FromSeconds(2));
            Assert.IsTrue(res);

            Thread.Sleep(TimeSpan.FromSeconds(3));

            // other locker should be able to gain the lock after the lock expires
            res = redisContext.TryGetDistributedLock("object1", "myName", TimeSpan.FromSeconds(10));
            Assert.IsTrue(res);
        }
        #endregion

        #region Redis Lists

        [TestMethod]
        public void TestAddToRedisList()
        {
            const string key = "TestAddToList";
            var values = new[] { "bar", "bar", "a", "b", "c" };

            for (var i = 0; i < values.Length; i++)
            {
                var length = redisContext.AddToList(key, values[i]);
                Assert.AreEqual(i + 1, length);
            }

            ValidateListResults(key, values);
        }

        [TestMethod]
        public void TestAddRangeToRedisList()
        {
            const string key = "TestAddRangeToList";
            var values = new[] { "bar", "bar", "a", "b", "c" };

            var length = redisContext.AddRangeToList(key, values);

            Assert.AreEqual(values.Length, length);
            ValidateListResults(key, values);
        }

        [TestMethod]
        public void TestGetRedisList()
        {
            const string key = "TestAddRangeToList";
            var values = new[] { "bar", "bar", "a", "b", "c" };

            redisContext.AddRangeToList(key, values);

            ValidateListResults(key, values);
            ValidateSubListResults(key, -100, 100, values);

            var valuesSubArray = values.Skip(1).Take(3).ToArray();
            ValidateSubListResults(key, 1, 3, valuesSubArray);
            ValidateSubListResults(key, -4, -2, valuesSubArray);
        }

        private void ValidateListResults(string key, string[] expected)
        {
            var valuesFromRedis = redisContext.GetList(key);
            CollectionAssert.AreEqual(expected, valuesFromRedis);
        }

        private void ValidateSubListResults(string key, long start, long end, string[] expected)
        {
            var valuesFromRedis = redisContext.GetList(key, start, end);
            CollectionAssert.AreEqual(expected, valuesFromRedis);
        }

        #endregion

        #region Redis Sets

        [TestMethod]
        public void TestAddToRedisSet()
        {
            const string key = "k1";
            var values = new[] { "bar", "bar", "a", "b", "c" };

            redisContext.AddToSet(key, values);

            ValidateSetResults(key, new[] { "a", "b", "c", "bar" });
        }

        [TestMethod]
        public void TestSetContainsTrue()
        {
            TestSetContains("a", "a", true);
        }

        [TestMethod]
        public void TestSetContainsFalse()
        {
            TestSetContains("a", "b", false);
        }


        [TestMethod]
        public void TestCountSetMembers()
        {
            const string key = "k2";
            var values = new[] { "bar", "bar", "a", "b", "c", "a", "b" };

            redisContext.AddToSet(key, values);

            Assert.AreEqual(4, redisContext.CountSetMembers(key));
        }

        [TestMethod]
        public void TestSetsDiff()
        {
            const string key1 = "testSetDiff1";
            var values1 = new[] { "a", "b", "c", "d", "e" };

            const string key2 = "testSetDiff2";
            var values2 = new[] { "a", "b", };

            redisContext.AddToSet(key1, values1);
            redisContext.AddToSet(key2, values2);

            var results = redisContext.GetSetsDifference(new[] { key1, key2 });
            CollectionAssert.AreEquivalent(new[] { "c", "d", "e" }, results);
        }

        [TestMethod]
        public void TestSetsUnion()
        {
            const string key1 = "TestSetsUnion1";
            var values1 = new[] { "a", "c" };

            const string key2 = "TestSetsUnion2";
            var values2 = new[] { "a", "b" };

            redisContext.AddToSet(key1, values1);
            redisContext.AddToSet(key2, values2);

            var results = redisContext.UnionSets(new[] { key1, key2 });
            CollectionAssert.AreEquivalent(new[] { "a", "b", "c" }, results);
        }

        [TestMethod]
        public void TestSetsIntersect()
        {
            const string key1 = "TestSetsIntersect1";
            var values1 = new[] { "a", "c" };

            const string key2 = "TestSetsIntersect2";
            var values2 = new[] { "a", "b" };

            redisContext.AddToSet(key1, values1);
            redisContext.AddToSet(key2, values2);

            var results = redisContext.IntersectSets(new[] { key1, key2 });
            CollectionAssert.AreEquivalent(new[] { "a" }, results);
        }

        private void ValidateSetResults(string key, string[] expected)
        {
            var valuesFromRedis = redisContext.GetSetMembers(key);
            CollectionAssert.AreEquivalent(expected, valuesFromRedis);
        }

        public void TestSetContains(string valueToAdd, string searchForValue, bool expected)
        {
            const string key = "testSetContains";
            var values = new[] { "foo", "bar" };

            redisContext.AddToSet(key, values);
            redisContext.AddToSet(key, new[] { valueToAdd });

            var setContains = redisContext.SetContains(key, searchForValue);
            Assert.AreEqual(expected, setContains);
        }

        #endregion

        #region Redis Hashes

        [TestMethod]
        public void Hashes_SetHashEntryWithSingleField_Added()
        {
            //Arrange
            const string key = "testHashesSet";
            var hashEntryToAdd = new HashEntry[]
            {
                new HashEntry("Test", "Test2")
            };

            //Act
            redisContext.HashesSet(key, hashEntryToAdd);

            //Assert
            var hashesContains = redisContext.HashesGetAll(key);
            hashesContains.Should().Contain(hashEntryToAdd);
        }

        [TestMethod]
        public void Hashes_SetHashEntryWithMultipleFields_Added()
        {
            //Arrange
            const string key = "testHashesSet";
            var hashEntryToAdd = new HashEntry[]
            {
                new HashEntry("Test", "Test2"),
                new HashEntry(false, true),
                new HashEntry(int.MaxValue, "TestValue2"),
                new HashEntry("TestField3", 1.0),
                new HashEntry(double.MinValue, long.MinValue)
            };

            //Act
            redisContext.HashesSet(key, hashEntryToAdd);

            //Assert
            var hashesContains = redisContext.HashesGetAll(key);
            hashesContains.Should().Contain(hashEntryToAdd);
        }

        [TestMethod]
        public void Hashes_SetWithStringFieldAndValue_Added()
        {
            //Arrange
            const string key = "testHashesSet";
            var field = "TestField";
            var value = "TestValue";

            //Act
            redisContext.HashesSet(key, field, value);

            //Assert
            var hashesContains = redisContext.HashesGetAll(key);
            hashesContains.Should().Contain(new HashEntry(field, value));
        }

        [TestMethod]
        public void Hashes_SetWithIntFieldAndStringValue_Added()
        {
            //Arrange
            const string key = "testHashesSet";
            var field = int.MaxValue;
            var value = "TestValue2";

            //Act
            redisContext.HashesSet(key, field, value);

            //Assert
            var hashesContains = redisContext.HashesGetAll(key);
            hashesContains.Should().Contain(new HashEntry(field, value));
        }

        [TestMethod]
        public void Hashes_SetWithStringFieldAndDoubleValue_Added()
        {
            //Arrange
            const string key = "testHashesSet";
            var field = "TestField3";
            var value = 1.0;

            //Act
            redisContext.HashesSet(key, field, value);

            //Assert
            var hashesContains = redisContext.HashesGetAll(key);
            hashesContains.Should().Contain(new HashEntry(field, value));
        }

        [TestMethod]
        public void Hashes_SetWithDoubleFieldAndLongValue_Added()
        {
            //Arrange
            const string key = "testHashesSet";
            var field = double.MinValue;
            var value = long.MaxValue;

            //Act
            redisContext.HashesSet(key, field, value);

            //Assert
            var hashesContains = redisContext.HashesGetAll(key);
            hashesContains.Should().Contain(new HashEntry(field, value));
        }

        [TestMethod]
        public void Hashes_SetWithBooleanFieldAndValue_Added()
        {
            //Arrange
            const string key = "testHashesSet";
            var field = true;
            var value = false;

            //Act
            redisContext.HashesSet(key, field, value);

            //Assert
            var hashesContains = redisContext.HashesGetAll(key);
            hashesContains.Should().Contain(new HashEntry(field, value));
        }

        [TestMethod]
        [ExpectedException(typeof(NotSupportedException))]
        public void Hashes_SetWithInvalidFieldAndValidValue_NotAdded()
        {
            //Arrange
            const string key = "testHashesSet";
            var field = char.MinValue;
            var value = "TestValue";

            //Act
            redisContext.HashesSet(key, field, value);

            //Assert
            var hashesContains = redisContext.HashesGetAll(key);
            hashesContains.Should().Contain(new HashEntry(field, value));
        }

        [TestMethod]
        [ExpectedException(typeof(NotSupportedException))]
        public void Hashes_SetWithValidFieldAndInvalidValue_NotAdded()
        {
            //Arrange
            const string key = "testHashesSet";
            var field = "TestField";
            var value = char.MaxValue;

            //Act
            redisContext.HashesSet(key, field, value);

            //Assert
            var hashesContains = redisContext.HashesGetAll(key);
            hashesContains.Should().Contain(new HashEntry(field, value));
        }

        [TestMethod]
        public void Hashes_TryGetFieldString_ValueReturned()
        {
            //Arrange
            const string key = "testHashesSet";
            var field = "TestField";
            var value = "TestValue";
            var hashEntryToAdd = new HashEntry[]
            {
                new HashEntry(field, value)
            };

            redisContext.HashesSet(key, hashEntryToAdd);

            //Act
            redisContext.HashesTryGetField<string, string>(key, field, out var returnedValue);

            //Assert
            Assert.AreEqual(value, returnedValue);
            var hashesContains = redisContext.HashesGetAll(key);
            hashesContains.Should().Contain(hashEntryToAdd);
        }

        [TestMethod]
        public void Hashes_TryGetFieldInt_ValueReturned()
        {
            //Arrange
            const string key = "testHashesSet";
            var field = "TestField";
            var value = 123;
            var hashEntryToAdd = new HashEntry[]
            {
                new HashEntry(field, value)
            };

            redisContext.HashesSet(key, hashEntryToAdd);

            //Act
            redisContext.HashesTryGetField<string, int>(key, field, out var returnedValue);

            //Assert
            Assert.AreEqual(value, returnedValue);
            var hashesContains = redisContext.HashesGetAll(key);
            hashesContains.Should().Contain(hashEntryToAdd);
        }

        [TestMethod]
        public void Hashes_TryGetFieldDouble_ValueReturned()
        {
            //Arrange
            const string key = "testHashesSet";
            var field = "TestField";
            var value = 123D;
            var hashEntryToAdd = new HashEntry[]
            {
                new HashEntry(field, value)
            };

            redisContext.HashesSet(key, hashEntryToAdd);

            //Act
            redisContext.HashesTryGetField<string, double>(key, field, out var returnedValue);

            //Assert
            Assert.AreEqual(value, returnedValue);
            var hashesContains = redisContext.HashesGetAll(key);
            hashesContains.Should().Contain(hashEntryToAdd);
        }

        [TestMethod]
        public void Hashes_TryGetFieldLong_ValueReturned()
        {
            //Arrange
            const string key = "testHashesSet";
            var field = "TestField";
            var value = 123L;
            var hashEntryToAdd = new HashEntry[]
            {
                new HashEntry(field, value)
            };

            redisContext.HashesSet(key, hashEntryToAdd);

            //Act
            redisContext.HashesTryGetField<string, long>(key, field, out var returnedValue);

            //Assert
            Assert.AreEqual(value, returnedValue);
            var hashesContains = redisContext.HashesGetAll(key);
            hashesContains.Should().Contain(hashEntryToAdd);
        }

        [TestMethod]
        public void Hashes_TryGetFieldBool_ValueReturned()
        {
            //Arrange
            const string key = "testHashesSet";
            var field = "TestField";
            var value = true;
            var hashEntryToAdd = new HashEntry[]
            {
                new HashEntry(field, value)
            };

            redisContext.HashesSet(key, hashEntryToAdd);

            //Act
            redisContext.HashesTryGetField<string, bool>(key, field, out var returnedValue);

            //Assert
            Assert.AreEqual(value, returnedValue);
            var hashesContains = redisContext.HashesGetAll(key);
            hashesContains.Should().Contain(hashEntryToAdd);
        }

        [TestMethod]
        public void Hashes_TryGetFieldInvalidType_TryGetFalse()
        {
            //Arrange
            const string key = "testHashesSet";
            var field = "TestField";
            var value = "TestValue";
            var hashEntryToAdd = new HashEntry[]
            {
                new HashEntry(field, value)
            };

            redisContext.HashesSet(key, hashEntryToAdd);

            //Act
            var result = redisContext.HashesTryGetField<string, char>(key, field, out var returnedValue);

            //Assert
            Assert.AreEqual(default(char), returnedValue);
            Assert.IsFalse(result);
        }

        [TestMethod]
        public void Hashes_DeleteFieldString_Deleted()
        {
            //Arrange
            const string key = "testHashesSet";
            var field = "TestField";
            var value = "TestValue";
            var hashEntryToAdd = new HashEntry[]
            {
                new HashEntry(field, value)
            };

            redisContext.HashesSet(key, hashEntryToAdd);

            //Act
            var result = redisContext.HashesDeleteField(key, field);

            //Assert
            Assert.IsTrue(result);
            var hashesContains = redisContext.HashesGetAll(key);
            hashesContains.Should().NotContain(hashEntryToAdd);
        }

        [TestMethod]
        public void Hashes_DeleteFieldInt_Deleted()
        {
            //Arrange
            const string key = "testHashesSet";
            var field = 123;
            var value = "TestValue";
            var hashEntryToAdd = new HashEntry[]
            {
                new HashEntry(field, value)
            };

            redisContext.HashesSet(key, hashEntryToAdd);

            //Act
            var result = redisContext.HashesDeleteField(key, field);

            //Assert
            Assert.IsTrue(result);
            var hashesContains = redisContext.HashesGetAll(key);
            hashesContains.Should().NotContain(hashEntryToAdd);
        }

        [TestMethod]
        public void Hashes_DeleteFieldDouble_Deleted()
        {
            //Arrange
            const string key = "testHashesSet";
            var field = 123D;
            var value = "TestValue";
            var hashEntryToAdd = new HashEntry[]
            {
                new HashEntry(field, value)
            };

            redisContext.HashesSet(key, hashEntryToAdd);

            //Act
            var result = redisContext.HashesDeleteField(key, field);

            //Assert
            Assert.IsTrue(result);
            var hashesContains = redisContext.HashesGetAll(key);
            hashesContains.Should().NotContain(hashEntryToAdd);
        }

        [TestMethod]
        public void Hashes_DeleteFieldLong_Deleted()
        {
            //Arrange
            const string key = "testHashesSet";
            var field = 123L;
            var value = "TestValue";
            var hashEntryToAdd = new HashEntry[]
            {
                new HashEntry(field, value)
            };

            redisContext.HashesSet(key, hashEntryToAdd);

            //Act
            var result = redisContext.HashesDeleteField(key, field);

            //Assert
            Assert.IsTrue(result);
            var hashesContains = redisContext.HashesGetAll(key);
            hashesContains.Should().NotContain(hashEntryToAdd);
        }

        [TestMethod]
        public void Hashes_DeleteFieldBool_Deleted()
        {
            //Arrange
            const string key = "testHashesSet";
            var field = true;
            var value = "TestValue";
            var hashEntryToAdd = new HashEntry[]
            {
                new HashEntry(field, value)
            };

            redisContext.HashesSet(key, hashEntryToAdd);

            //Act
            var result = redisContext.HashesDeleteField(key, field);

            //Assert
            Assert.IsTrue(result);
            var hashesContains = redisContext.HashesGetAll(key);
            hashesContains.Should().NotContain(hashEntryToAdd);
        }

        [TestMethod]
        public void Hashes_LengthOfTwoFields_LengthReturned()
        {
            //Arrange
            const string key = "testHashesLength";
            var hashEntryToAdd = new HashEntry[]
            {
                new HashEntry("TestField1", "TestValue1"),
                new HashEntry("TestField2", "TestValue2")
            };

            redisContext.HashesSet(key, hashEntryToAdd);

            //Act
            var length = redisContext.HashesLength(key);

            //Assert
            Assert.AreEqual(hashEntryToAdd.Length, length);
        }

        [TestMethod]
        public void Hashes_LengthOfZeroFields_LengthReturned()
        {
            //Arrange
            const string key = "testHashesLength";
            var hashEntryToAdd = new HashEntry[] { };

            redisContext.HashesSet(key, hashEntryToAdd);

            //Act
            var length = redisContext.HashesLength(key);

            //Assert
            Assert.AreEqual(hashEntryToAdd.Length, length);
        }

        #endregion

        #region scripting tests

        [TestMethod]
        public void TestSimpleScript()
        {
            const string script = "return 1";

            var result = redisContext.RunScriptInt(script, null);

            Assert.AreEqual(1, result);
        }

        [TestMethod]
        public void TestSimpleScriptStringArray()
        {
            const string script = "return {'one', 'two'}";

            var result = redisContext.RunScriptStringArray(script, null);

            CollectionAssert.AreEqual(new[] { "one", "two" }, result);
        }

        [TestMethod]
        public void TestSimpleScriptThatCallsRedis()
        {
            const string script = "redis.call('set', @Key1, @IntArg1)";

            var keysAndArgs = redisContext.CreateScriptKeyAndArguments()
                .Apply(x =>
                {
                    x.Key1 = "myTest";
                    x.IntArg1 = 7878;
                });

            redisContext.RunScript(script, keysAndArgs);

            var getResult = redisContext.TryGet("myTest", out int result);
            Assert.IsTrue(getResult);
            Assert.AreEqual(7878, result);
        }

        // key1            - sl window key
        // LongArg1 arg 1       - now in miliseconds
        // int arg 2       - sliding window size in miliseconds
        // string arg 1    - member value
        // int arg 3      - limit

        [TestMethod]
        public void TestSlidingWindowScript()
        {
            var r = new Random();
            var baseline = new DateTime(2018, 01, 01);
            long toMiliseconds(DateTime dt)
            {
                return (long)(dt - baseline).TotalMilliseconds;
            }


            const string script = @"redis.call('ZREMRANGEBYSCORE', @Key1, -1, (@LongArg1 -  @LongArg2))
local windowContent = redis.call('ZRANGE', @Key1, 0, -1)
redis.call('ZADD', @Key1, @LongArg1, @StringArg1)
redis.call('EXPIRE', @Key1, @LongArg2 / 1000)
if (tonumber(@IntArg3) >= #windowContent) 
    then return 0
    else return 1
end";

            var keysAndArgs = redisContext.CreateScriptKeyAndArguments()
                .Apply(x =>
                {
                    //x.Key1 = $"window{r.Next(0, int.MaxValue)}";
                    x.Key1 = "EmailSenderProcessCount1";
                    x.LongArg2 = 600_000;
                    x.IntArg3 = 8_000_000;
                });


            // call the script 4 times
            var runs = 3000;
            var results = new int[runs];
            var timeCounters = new long[runs];
            var sw = Stopwatch.StartNew();
            for (var i = 0; i < runs; i++)
            {
                redisContext.RunScriptInt(script, keysAndArgs.Apply(x =>
                {
                    x.LongArg1 = toMiliseconds(DateTime.Now);
                    x.StringArg1 = $"{{'QueueId':'{Guid.NewGuid()}','__rand':'{Guid.NewGuid()}'}}";
                }));

                timeCounters[i] = sw.ElapsedMilliseconds;

            }

            sw.Stop();

            //assert not more than 5 ms in average
            Assert.IsTrue(sw.Elapsed < TimeSpan.FromMilliseconds(runs * 5));
        }



        // key1            - sl window key
        // LongArg1 arg 1       - now in miliseconds
        // int arg 2       - sliding window size in miliseconds
        // string arg 1    - member value
        // int arg 3      - limit

        [TestMethod]
        public void TestSlidingWindowFunctionality()
        {
            var r = new Random();
            var baseline = new DateTime(2018, 01, 01);
            long toMiliseconds(DateTime dt)
            {
                return (long)(dt - baseline).TotalMilliseconds;
            }

            //redis.call('EXPIRE', @Key1, @LongArg2 / 1000)

            const string script = @"redis.call('ZREMRANGEBYSCORE', @Key1, -1, (@LongArg1 -  @LongArg2))
local windowContent = redis.call('ZRANGE', @Key1, 0, -1)
redis.call('ZADD', @Key1, @LongArg1, @StringArg1)
return (#windowContent + 1)";

            var windowSizeInSeconds = 5;
            var keysAndArgs = redisContext.CreateScriptKeyAndArguments()
                .Apply(x =>
                {
                    //x.Key1 = $"window{r.Next(0, int.MaxValue)}";
                    x.Key1 = $"testB-{r.Next(0, 900_000)}";
                    x.LongArg2 = windowSizeInSeconds * 1000; // sliding window size in miliseconds
                });


            const int runs = 10;
            var results = new int[runs];
            var expected = new int[runs];
            for (var i = 0; i < runs; i++)
            {
                expected[i] = i < windowSizeInSeconds ? i + 1 : windowSizeInSeconds;
            }


            for (var i = 0; i < runs; i++)
            {
                results[i] = redisContext.RunScriptInt(script, keysAndArgs.Apply(x =>
                {
                    x.LongArg1 = toMiliseconds(DateTime.Now);
                    x.StringArg1 = $"{{ 'a': '{Guid.NewGuid()}' }}";
                }));

                Thread.Sleep(1000);
            }

            // assert results
            for (var i = 0; i < runs; i++)
            {
                Assert.AreEqual(expected[i], results[i]);
            }

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

        #region Redis Sorted Sets

        [TestMethod]
        public void TestAddToRedisSortedSet()
        {
            const string key = "k1";
            var values = new[] { ("test2", 14.0), ("foo", 11.0), ("test", 11.0), ("foo", 13.0), ("bar", 12.0) };

            var count = redisContext.SortedSetAdd(key, values);

            Assert.AreEqual(4L, count);
            ValidateSortedSetResults(key, new[] { ("test", 11.0), ("bar", 12.0), ("foo", 13.0), ("test2", 14.0) }, 11, 14);
        }

        [TestMethod]
        public void TestRedisSortedSetCountMembers_Full()
        {
            const string key = "k1";
            var values = new[] { ("test2", 14.0), ("foo", 11.0), ("test", 11.0), ("foo", 13.0), ("bar", 12.0) };

            redisContext.SortedSetAdd(key, values);

            var count = redisContext.SortedSetGetLength(key);

            Assert.AreEqual(4L, count);
        }

        [TestMethod]
        public void TestRedisSortedSetCountMembers_Partial()
        {
            const string key = "k1";
            var values = new[] { ("test2", 14.0), ("foo", 11.0), ("test", 11.0), ("foo", 13.0), ("bar", 12.0) };

            redisContext.SortedSetAdd(key, values);

            var count = redisContext.SortedSetGetLength(key, 12, 13);

            Assert.AreEqual(2L, count);
        }

        [TestMethod]
        public void TestRedisGetSortedSetMembersByScore()
        {
            const string key = "k1";
            var values = new[] { ("test2", 14.0), ("foo", 11.0), ("test", 11.0), ("bar", 12.0) };

            redisContext.SortedSetAdd(key, values);

            var result = redisContext.SortedSetGetRangeByScore(
                key, RedisValueConverter.ToString, 11, 12);

            CollectionAssert.AreEquivalent(new[] { "foo", "test", "bar" }, result);
        }

        [TestMethod]
        public void TestRedisGetSortedSetMembersByScoreDescending()
        {
            const string key = "k1";
            var values = new[] { ("test2", 14.0), ("foo", 11.0), ("test", 11.0), ("bar", 12.0) };

            redisContext.SortedSetAdd(key, values);

            var result = redisContext.SortedSetGetRangeByScore(
                key, RedisValueConverter.ToString, 11, 12, Enums.SortOrder.Descending);

            CollectionAssert.AreEquivalent(new[] { "bar", "test", "foo" }, result);
        }

        [TestMethod]
        public void TestRedisGetSortedSetMembersByRank()
        {
            const string key = "k1";
            var values = new[] { ("test2", 14.0), ("foo", 11.0), ("test", 11.0), ("bar", 12.0) };

            redisContext.SortedSetAdd(key, values);

            var result = redisContext.SortedSetGetRangeByRank(
                key, RedisValueConverter.ToString, 1, 2);

            CollectionAssert.AreEquivalent(new[] { "test", "bar" }, result);
        }

        [TestMethod]
        public void TestRedisGetSortedSetMembersByRankDescending()
        {
            const string key = "k1";
            var values = new[] { ("test2", 14.0), ("foo", 11.0), ("test", 11.0), ("bar", 12.0) };

            redisContext.SortedSetAdd(key, values);

            var result = redisContext.SortedSetGetRangeByRank(
                key, RedisValueConverter.ToString, 1, 2, Enums.SortOrder.Descending);

            CollectionAssert.AreEquivalent(new[] { "bar", "test" }, result);
        }

        [TestMethod]
        public void TestRedisGetSortedSetMembersByScoreWithScores()
        {
            const string key = "k1";
            var values = new[] { ("test2", 14.0), ("foo", 11.0), ("test", 11.0), ("bar", 12.0) };

            redisContext.SortedSetAdd(key, values);

            var result = redisContext.SortedSetGetRangeByScoreWithScores(
                key, RedisValueConverter.ToString, 11, 12);

            CollectionAssert.AreEquivalent(new[] { ("foo", 11.0), ("test", 11.0), ("bar", 12.0) }, result);
        }

        [TestMethod]
        public void TestRedisGetSortedSetMembersByRankWithScores()
        {
            const string key = "k1";
            var values = new[] { ("test2", 14.0), ("foo", 11.0), ("test", 11.0), ("bar", 12.0) };

            redisContext.SortedSetAdd(key, values);

            var result = redisContext.SortedSetGetRangeByRankWithScores(
                key, RedisValueConverter.ToString, 1, 2);

            CollectionAssert.AreEquivalent(new[] { ("test", 11.0), ("bar", 12.0) }, result);
        }

        [TestMethod]
        public void TestRedisRemoveFromSortedSet()
        {
            const string key = "k1";
            var values = new[] { ("test2", 14.0) };

            redisContext.SortedSetAdd(key, values);

            var countRemoved = redisContext.SortedSetRemove(key, new[] { "test2" });

            Assert.AreEqual(1L, countRemoved);
            ValidateSortedSetResults(key, new (string, double)[0], 14, 14);
        }

        [TestMethod]
        public void TestRedisRemoveRangeFromSortedSetByScore()
        {
            const string key = "k1";
            var values = new[] { ("test2", 14.0), ("foo", 11.0), ("test", 11.0), ("bar", 12.0) };

            redisContext.SortedSetAdd(key, values);

            var countRemoved = redisContext.SortedSetRemoveRangeByScore(key, 11, 12);

            Assert.AreEqual(3L, countRemoved);
            ValidateSortedSetResults(key, new[] { ("test2", 14.0) });
        }

        [TestMethod]
        public void TestRedisRemoveRangeFromSortedSetByRank()
        {
            const string key = "k1";
            var values = new[] { ("test2", 14.0), ("foo", 11.0), ("test", 11.0), ("bar", 12.0) };

            redisContext.SortedSetAdd(key, values);

            var countRemoved = redisContext.SortedSetRemoveRangeByRank(key, 0, 1);

            Assert.AreEqual(2L, countRemoved);
            ValidateSortedSetResults(key, new[] { ("bar", 12.0), ("test2", 14.0) });
        }


        private void ValidateSortedSetResults(
            string key, (string, double)[] expected, double scoreStart = double.NegativeInfinity, double scoreEnd = double.PositiveInfinity)
        {
            var valuesFromRedis = redisContext.SortedSetGetRangeByScoreWithScores(
                key, RedisValueConverter.ToString, scoreStart, scoreEnd);

            CollectionAssert.AreEquivalent(expected, valuesFromRedis);
        }

        #endregion

    }
}
