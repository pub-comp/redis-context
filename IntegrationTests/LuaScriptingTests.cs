using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Text;
using System.Threading;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using PubComp.RedisRepo.IntegrationTests;

namespace PubComp.RedisRepo.IntegrationTests
{
    [TestClass]
    public class LuaScriptingTests : RedisTests
    {
        //private RedisConnectionBuilder connectionBuilder;

        //[TestInitialize]
        //public void TestInit()
        //{
        //    IDecrypt crypt = new PlainEncryptor();

        //    this.connectionBuilder =
        //        new RedisConnectionBuilder(
        //            decrypt: crypt /* no need, no password in local */,
        //            //host: "redis-12349.rediscluster.payoneer.com",
        //            host: "localhost",
        //            //port: 12349,
        //            port: 6379,
        //            //dbId: 0,
        //            dbId: 13,
        //            ctxNamespace: "th",
        //            connectionPoolSize: 1,
        //            encryptedPassword: "9vvDVxZthgY2z8q9+oSwsKSsdxu8inj41LmDDrJKAIE=");
        //}

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
            (redisContext as RedisTestContext)?.Connection?.Dispose();
        }

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
return #windowContent";
            
            var windowSizeInSeconds = 5;
            var keysAndArgs = redisContext.CreateScriptKeyAndArguments()
                .Apply(x =>
                {
                    //x.Key1 = $"window{r.Next(0, int.MaxValue)}";
                    x.Key1 = $"testB-{r.Next(0, 900_000)}";
                    x.LongArg2 = windowSizeInSeconds * 1000; // sliding window size in miliseconds
                    x.IntArg3 = 8_000_000; // limit
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
    }
}

