using Microsoft.VisualStudio.TestTools.UnitTesting;
using StackExchange.Redis;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using PubComp.RedisRepo.Exceptions;


namespace PubComp.RedisRepo.IntegrationTests
{
    [TestClass]
    public class TransactionalRedisContextTests
    {
        readonly string ns = "n1";
        readonly string host = "localhost";
        readonly int port = 6379;
        readonly int port_invalid = 6111;
        readonly string password = null;
        readonly int dbId = 10;
        readonly Random random = new Random();

        public TestContext TestContext { get; set; }


        [TestInitialize]
        public void TestInit()
        {
            ThreadPool.SetMinThreads(250, 250);

            ClearAllKeys();
        }


        private RedisContext GetTestConnection(bool isValidConnection = true)
        {
            return new RedisContext(
                            contextNamespace: ns, host: host, port: isValidConnection ? port : port_invalid,
                            password: password, db: dbId,
                            commandFlags: CommandFlags.None, defaultRetries: 5, totalConnections: 1);
        }

        private string GenerateRandomKey()
        {
            var key = $"{TestContext.TestName}{random.Next(1, 100000)}";
            return key;
        }

        private void ClearAllKeys()
        {
            // delete all keys in current namespace, current db
            //var keys = RetryUtil.Retry(() => ctx.GetKeys("*"), 5);
            //RetryUtil.Retry(() => ctx.Delete(keys.ToArray()), 5);
        }

        [TestMethod]
        [ExpectedException(typeof(FailedToConnectException))]
        [TestCategory("Transactional")]
        [TestCategory("Connection validation")]
        public void TestInvalidConnection()
        {
            GetTestConnection(isValidConnection: false);
        }

        #region Simple set and get
        [TestMethod]
        [TestCategory("Transactional")]
        [TestCategory("SetGet")]
        public void TestGetAndSetSameTransactionBool()
        {
            // bool
            TestGetAndSetSameTransaction((ctx, key, val) => ctx.Set(key, val), (ctx, key) => ctx.GetBool(key), 2, true, true).GetAwaiter().GetResult();
        }

        [TestMethod]
        [TestCategory("Transactional")]
        [TestCategory("SetGet")]
        public void TestGetAndSetSameTransactionNullableBool()
        {

            // nullable bool - with value
            TestGetAndSetSameTransaction((ctx, key, val) => ctx.Set(key, val), (ctx, key) => ctx.GetNullableBoolean(key), 2, (bool?)true, true).GetAwaiter().GetResult();
        }

        [TestMethod]
        [TestCategory("Transactional")]
        [TestCategory("SetGet")]
        public void TestGetAndSetSameTransactioNullableBool2()
        {
            // nullable bool - no value
            TestGetAndSetSameTransaction((ctx, key, val) => ctx.Set(key, val), (ctx, key) => ctx.GetNullableBoolean(key), 2, (bool?)null, null)
                .GetAwaiter().GetResult();
        }

        [TestMethod]
        [TestCategory("Transactional")]
        [TestCategory("SetGet")]
        public void TestGetAndSetSameTransactionString()
        {
            // string
            TestGetAndSetSameTransaction((ctx, key, val) => ctx.Set(key, val), (ctx, key) => ctx.GetString(key), 2, "foobar", "foobar")
                .GetAwaiter().GetResult();
        }

        [TestMethod]
        [TestCategory("Transactional")]
        [TestCategory("SetGet")]
        public void TestGetAndSetSameTransactionInt()
        {
            // int
            TestGetAndSetSameTransaction((ctx, key, val) => ctx.Set(key, val), (ctx, key) => ctx.GetInt(key), 2, 444, 444)
                .GetAwaiter().GetResult();
        }


        [TestMethod]
        [TestCategory("Transactional")]
        [TestCategory("SetGet")]
        public void TestGetAndSetSameTransactionNullableInt()
        {
            // nullable int
            TestGetAndSetSameTransaction((ctx, key, val) => ctx.Set(key, val), (ctx, key) => ctx.GetNullableInt(key), 2, (int?)444, 444)
                .GetAwaiter().GetResult();
        }

        [TestMethod]
        [TestCategory("Transactional")]
        [TestCategory("SetGet")]
        public void TestGetAndSetSameTransactionLong()
        {
            // longmul
            TestGetAndSetSameTransaction((ctx, key, val) => ctx.Set(key, val), (ctx, key) => ctx.GetLong(key), 2, 444L, 444)
                .GetAwaiter().GetResult();
        }

        [TestMethod]
        [TestCategory("Transactional")]
        [TestCategory("SetGet")]
        public void TestGetAndSetSameTransactionNullableLong()
        {
            // nullable int
            TestGetAndSetSameTransaction((ctx, key, val) => ctx.Set(key, val), (ctx, key) => ctx.GetNullableLong(key), 2, (long?)444, 444)
                .GetAwaiter().GetResult();
        }

        private async Task TestGetAndSetSameTransaction<T>(
            Action<TransactionalRedisContext, string, T> set,
            Action<TransactionalRedisContext, string> get,
            int expectedResultsCount,
            T setValue,
            T expectedValue)
        {
            var ctx = GetTestConnection();
            var key = GenerateRandomKey();

            var transactionCtx = TransactionalRedisContext.FromRedisContext(ctx);
            transactionCtx.Start();

            set(transactionCtx, key, setValue);
            get(transactionCtx, key);

            var results = transactionCtx.Execute();

            Assert.AreEqual(expectedResultsCount, results.Length);
            var t = results[1] as Task<T>;

            Assert.IsNotNull(t);
            var actual = await t;

            Assert.AreEqual(expectedValue, actual);

            ctx?.CloseConnections();
        }
        #endregion

        #region Sorted set add-get test

        [TestMethod]
        [TestCategory("Transactional")]
        [TestCategory("Sorted Set get-set")]
        public void SortedSetAddGetInt()
        {
            SortedSetAddGet(
                new [] { 12, 13, 14 },
                (tx, key, val, score) => tx.SortedSetAdd(key, val, score),
                (tx, key, start) => tx.SortedSetGetRangeByRankInt(key, rangeStart: start));
        }

        [TestMethod]
        [TestCategory("Transactional")]
        [TestCategory("Sorted Set get-set")]
        public void SortedSetAddGetBool()
        {
            SortedSetAddGet(
                new [] { true, false },
                (tx, key, val, score) => tx.SortedSetAdd(key, val, score),
                (tx, key, start) => tx.SortedSetGetRangeByRankBool(key, rangeStart: start));
        }

        [TestMethod]
        [TestCategory("Transactional")]
        [TestCategory("Sorted Set get-set")]
        public void SortedSetAddGetByteArray()
        {
            SortedSetAddGet(
                new[] { new byte[] {1,2,3}, new byte[]{ 4, 5, 6 } },
                (tx, key, val, score) => tx.SortedSetAdd(key, val, score),
                (tx, key, start) => tx.SortedSetGetRangeByRankByteArray(key, rangeStart: start));
        }


        [TestMethod]
        [TestCategory("Transactional")]
        [TestCategory("Sorted Set get-set")]
        public void SortedSetAddGetDouble()
        {
            SortedSetAddGet(
                new [] { 0.145, 1.778, 4444.4, 321.000001 },
                (tx, key, val, score) => tx.SortedSetAdd(key, val, score),
                (tx, key, start) => tx.SortedSetGetRangeByRankDouble(key, rangeStart: start));
        }


        [TestMethod]
        [TestCategory("Transactional")]
        [TestCategory("Sorted Set get-set")]
        public void SortedSetAddGetString()
        {
            SortedSetAddGet(
                new [] { "ba", "ma", " ", '\t'.ToString() },
                (tx, key, val, score) => tx.SortedSetAdd(key, val, score),
                (tx, key, start) => tx.SortedSetGetRangeByRankString(key, rangeStart: start));
        }

        [TestMethod]
        [TestCategory("Transactional")]
        [TestCategory("Sorted Set get-set")]
        [ExpectedException(typeof(ArgumentNullException), AllowDerivedTypes = false)]
        public void SortedSetAddStringThrowsRagumentNullException()
        {
            SortedSetAddGet(
                new string[] { null },
                (tx, key, val, score) => tx.SortedSetAdd(key, val, score),
                (tx, key, start) => tx.SortedSetGetRangeByRankString(key, rangeStart: start));
        }

        [TestMethod]
        [TestCategory("Transactional")]
        [TestCategory("Sorted Set get-set")]
        public void SortedSetAddGetLong()
        {
            SortedSetAddGet(
                new [] { 4444L, 33332, 12314, 1, 0 },
                (tx, key, val, score) => tx.SortedSetAdd(key, val, score),
                (tx, key, start) => tx.SortedSetGetRangeByRankLong(key, rangeStart: start));
        }


        public void SortedSetAddGet<T>(
            T[] vals, Action<TransactionalRedisContext, string, T, double> sortedSetAdd,
            Action<TransactionalRedisContext, string, int> getRange)
        {
            var key = GenerateRandomKey();
            var valsCount = vals.Length;
            var conn = GetTestConnection();
            try
            {
                var txCtx = TransactionalRedisContext.FromRedisContext(conn);
                txCtx.Start();
                var i = 0;
                foreach (var val in vals)
                {
                    sortedSetAdd(txCtx, key, val, i);
                    i++;
                }

                var addResults = txCtx.Execute();

                Task.WaitAll(addResults);

                txCtx.Start();
                for (var rangeIndex = 0; rangeIndex < valsCount; rangeIndex++)
                {
                    getRange(txCtx, key, rangeIndex);

                }

                var results = txCtx.ExecuteAndWaitTyped<T[]>();

                i = 0;
                while (valsCount > 0)
                {
                    valsCount--;
                    Assert.AreEqual(valsCount + 1, results[i].Length);
                    i++;
                }
            }
            finally
            {
                conn.CloseConnections();
            }
        }

        #endregion

        #region sorted set - get by score tests

        [TestMethod]
        [TestCategory("Transactional")] [TestCategory("Sorted Set Get by Score")]
        public void SortedSetGetbyScoreInt()
        {
            SortedSetGetByScoreTest(
                new []
                {
                    new Tuple<int, double>(10, 10),
                    new Tuple<int, double>(20, 20),
                    new Tuple<int, double>(30, 30),
                },
                new Dictionary<double, int[]>
                {
                    [8] = new [] { 10, 20, 30 },
                    [17] = new [] { 20, 30 },
                    [26] = new [] { 30 },
                },
                (tx, key, value, score) => tx.SortedSetAdd(key, value, score),
                (tx, key, startScore) => tx.SortedSetGetRangeByScoreInt(key, start: startScore));
        }

        [TestMethod]
        [TestCategory("Transactional")] [TestCategory("Sorted Set Get by Score")]
        public void SortedSetGetbyScoreString()
        {
            SortedSetGetByScoreTest(
                new []
                {
                    new Tuple<string, double>("a", 10),
                    new Tuple<string, double>("b", 20),
                    new Tuple<string, double>("ccc", 30),
                },
                new Dictionary<double, string[]>
                {
                    [10] = new [] { "a", "b", "ccc" },
                    [20] = new [] { "b", "ccc" },
                    [30] = new [] { "ccc" },
                },
                (tx, key, value, score) => tx.SortedSetAdd(key, value, score),
                (tx, key, startScore) => tx.SortedSetGetRangeByScoreString(key, start: startScore));
        }

        [TestMethod]
        [TestCategory("Transactional")] [TestCategory("Sorted Set Get by Score")]
        public void SortedSetGetbyScoreDouble()
        {
            SortedSetGetByScoreTest(
                new []
                {
                    new Tuple<double, double>(1, 10),
                    new Tuple<double, double>(2.5, 20),
                    new Tuple<double, double>(33.333, 30),
                },
                new Dictionary<double, double[]>
                {
                    [10] = new [] { 1, 2.5, 33.333 },
                    [20] = new [] { 2.5, 33.333 },
                    [30] = new [] { 33.333 },
                },
                (tx, key, value, score) => tx.SortedSetAdd(key, value, score),
                (tx, key, startScore) => tx.SortedSetGetRangeByScoreDouble(key, start: startScore));
        }

        [TestMethod]
        [TestCategory("Transactional")] [TestCategory("Sorted Set Get by Score")]
        public void SortedSetGetbyScoreBool()
        {
            SortedSetGetByScoreTest(
                new []
                {
                    new Tuple<bool, double>(false, 10),
                    new Tuple<bool, double>(true, 20),
                },
                new Dictionary<double, bool[]>
                {
                    [10] = new [] { false, true },
                    [20] = new [] { true }
                },
                (tx, key, value, score) => tx.SortedSetAdd(key, value, score),
                (tx, key, startScore) => tx.SortedSetGetRangeByScoreBool(key, start: startScore));
        }

        [TestMethod]
        [TestCategory("Transactional")] [TestCategory("Sorted Set Get by Score")]
        public void SortedSetGetbyScoreLong()
        {
            SortedSetGetByScoreTest(
                new []
                {
                    new Tuple<long, double>(1010, 10),
                    new Tuple<long, double>(2020, 20),
                    new Tuple<long, double>(3030, 30),
                    new Tuple<long, double>(4040, 35),
                },
                new Dictionary<double, long[]>
                {
                    [8] = new [] { 1010L, 2020, 3030, 4040 },
                    [18] = new [] { 2020L, 3030, 4040 },
                    [26] = new [] { 3030L, 4040 },
                    [34] = new [] { 4040L },
                },
                (tx, key, value, score) => tx.SortedSetAdd(key, value, score),
                (tx, key, startScore) => tx.SortedSetGetRangeByScoreLong(key, start: startScore));
        }

        public void SortedSetGetByScoreTest<T>(
            Tuple<T, double>[] valswithScores,
            Dictionary<double, T[]> expectedResultsPerScore,
            Action<TransactionalRedisContext, string, T, double> sortedSetAdd,
            Action<TransactionalRedisContext, string, double> getByScore)
        {
            var key = GenerateRandomKey();
            var conn = GetTestConnection();
            try
            {
                var txCtx = TransactionalRedisContext.FromRedisContext(conn);
                txCtx.Start();
                foreach (var val in valswithScores)
                {
                    var value = val.Item1;
                    var score = val.Item2;
                    sortedSetAdd(txCtx, key, value, score);
                }

                foreach (var expected in expectedResultsPerScore)
                {
                    getByScore(txCtx, key, expected.Key);
                }

                //var resultObjects = txCtx.ExecuteAndWait();

                var results = txCtx.ExecuteAndWaitTyped<T[]>();

                Assert.AreEqual(expectedResultsPerScore.Count, results.Length);
                var i = 0;
                foreach (var expected in expectedResultsPerScore)
                {
                    for (var j = 0; j < results[i].Length; j++)
                    {
                        Assert.AreEqual(expected.Value[j], results[i][j]);
                    }

                    i++;
                }
            }
            finally
            {
                conn.CloseConnections();
            }
        }
        #endregion

        #region Sorted Set Remove Range By Score
        [TestMethod]
        [TestCategory("Transactional")]
        [TestCategory("Sorted Set Remove Range By Score")]
        public void SortedSetRemoveRangeByScoreLong()
        {
            SortedSetRemoveRangeByScoreTest(
                new []
                {
                    new Tuple<long, double>(1010, 10),
                    new Tuple<long, double>(2020, 20),
                    new Tuple<long, double>(3030, 30),
                    new Tuple<long, double>(4040, 35),
                },
                new Dictionary<double, long[]>
                {
                    [8] = new []  { 2020L, 3030, 4040 },
                    [18] = new [] { 2020L, 3030, 4040 },
                    [26] = new [] { 3030L, 4040 },
                    [34] = new [] { 4040L },
                },
                5d, 12d,
                (tx, key, value, score) => tx.SortedSetAdd(key, value, score),
                (tx, key, startScore) => tx.SortedSetGetRangeByScoreLong(key, start: startScore));
        }

        [TestMethod]
        [TestCategory("Transactional")]
        [TestCategory("Sorted Set Remove Range By Score")]
        public void SortedSetRemoveRangeByScoreString()
        {
            SortedSetRemoveRangeByScoreTest(
                new []
                {
                    new Tuple<string, double>("ba", 10),
                    new Tuple<string, double>("ma", 20),
                    new Tuple<string, double>("psycho", 30),
                    new Tuple<string, double>("logist", 35),
                },
                new Dictionary<double, string[]>
                {
                    [8] = new  [] { "ma", "psycho", "logist" },
                    [18] = new [] { "ma", "psycho", "logist" },
                    [26] = new [] { "psycho", "logist" },
                    [34] = new [] { "logist" },
                },
                5d, 12d,
                (tx, key, value, score) => tx.SortedSetAdd(key, value, score),
                (tx, key, startScore) => tx.SortedSetGetRangeByScoreString(key, start: startScore));
        }

        public void SortedSetRemoveRangeByScoreTest<T>(
            Tuple<T, double>[] valswithScores,
            Dictionary<double, T[]> expectedResultsPerScore,
            double startScore, double stopScore,
            Action<TransactionalRedisContext, string, T, double> sortedSetAdd,
            Action<TransactionalRedisContext, string, double> getByScore)
        {
            var key = GenerateRandomKey();
            var conn = GetTestConnection();
            try
            {
                var txCtx = TransactionalRedisContext.FromRedisContext(conn);
                txCtx.Start();
                foreach (var val in valswithScores)
                {
                    var value = val.Item1;
                    var score = val.Item2;
                    sortedSetAdd(txCtx, key, value, score);
                }

                var addResults = txCtx.Execute();

                Task.WaitAll(addResults);

                txCtx.Start();
                txCtx.SortedSetRemoveRangeByScore(key, startScore, stopScore);
                foreach (var expected in expectedResultsPerScore)
                {
                    getByScore(txCtx, key, expected.Key);
                }

                var castedResults = txCtx.ExecuteAndWaitTyped<T[]>();

                Assert.AreEqual(expectedResultsPerScore.Count, castedResults.Length);
                var i = 0;
                foreach (var expected in expectedResultsPerScore)
                {
                    for (var j = 0; j < castedResults[i].Length; j++)
                    {
                        Assert.AreEqual(expected.Value[j], castedResults[i][j]);
                    }

                    i++;
                }
            }
            finally
            {
                conn.CloseConnections();
            }
        }
        #endregion

        #region Sliding Window Test

        [TestMethod]
        [TestCategory("Transactional")]
        [TestCategory("Sliding window")]
        public void SlidingWindowTest()
        {
            var key = GenerateRandomKey();
            const int valuesCount = 15;
            const int waitInEachIterationSeconds = 1;
            const int slidingWindowSizeMS = 5000;

            var expectedResultsByTimePassed = new Dictionary<int, int>
            {
                [1] = 11,  [2] = 23,   [3] = 36,   [4] = 50,   [5] = 65,
                [6] = 70,  [7] = 75,   [8] = 80,   [9] = 85,   [10] = 90,
                [11] = 95, [12] = 100, [13] = 105, [14] = 110, [15] = 115,
            };

            var baseline = DateTime.UtcNow;

            var conn = GetTestConnection();
            try
            {
                var txCtx = TransactionalRedisContext.FromRedisContext(conn);
                for (var i = 1; i <= valuesCount; i++)
                {
                    var sw = Stopwatch.StartNew();
                    txCtx.Start();
                    var resultsIindex = 1;
                    var currentIterationScore = (DateTime.UtcNow - baseline).TotalSeconds * 1000;
                    var calculatedRangeEnd = currentIterationScore - slidingWindowSizeMS;
                    var removeRangeEnd = Math.Max(calculatedRangeEnd, 0);

                    // remove older points
                    if (removeRangeEnd > 0)
                    {
                        Console.WriteLine($"removing range. start: 0, end: {removeRangeEnd}");
                        txCtx.SortedSetRemoveRangeByScore(key, 0, removeRangeEnd);
                        resultsIindex++;
                    }

                    // add current point to the sliding window
                    Console.WriteLine($"adding to set. score: {currentIterationScore}");
                    txCtx.SortedSetAdd(key, 10 + i, currentIterationScore);

                    // get the entire set - current window
                    txCtx.SortedSetGetRangeByRankInt(key);

                    // set new TTL - window size -- this is nessassary only for cleaning up after the test
                    txCtx.SetTimeToLive(key, TimeSpan.FromMilliseconds(slidingWindowSizeMS));
                    var resultsTasks = txCtx.Execute();

                    // wait for all of it to finish
                    Task.WaitAll(resultsTasks);

                    // only the second\third command is interesting for us - getting all of the values in the current window
                    var convertedTask = resultsTasks[resultsIindex] as Task<int[]>;
                    Assert.IsNotNull(convertedTask);
                    var windowContent = convertedTask.Result;

                    var sum = windowContent.Sum();
                    Console.WriteLine(
                        $"checking results. iteration: {i}, expected: {expectedResultsByTimePassed[i]}, actual: {sum}");
                    Assert.AreEqual(expectedResultsByTimePassed[i], sum);

                    sw.Stop();

                    Thread.Sleep((waitInEachIterationSeconds * 1000) - (int) sw.ElapsedMilliseconds);
                }
            }
            finally
            {
                conn.CloseConnections();
            }
        }
        #endregion

    }
}
