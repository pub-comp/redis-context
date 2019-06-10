using PubComp.RedisRepo.Enums;
using StackExchange.Redis;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace PubComp.RedisRepo
{
    public class TransactionalRedisContext
    {
        private ITransaction contextTransaction;
        private readonly RedisContext context;
        private readonly CommandFlags commandFlags;

        private TransactionalRedisContext(RedisContext context)
        {
            this.context = context;
            this.commandFlags = context.commandFlags;
        }

        public static TransactionalRedisContext FromRedisContext(RedisContext ctx)
        {
            return new TransactionalRedisContext(ctx);
        }

        public static T[] WaitAndConvertResults<T>(Task[] results)
        {
            Task.WaitAll(results);
            var casted = results.OfType<Task<T>>().Select(t => t.Result).ToArray();
            return casted;
        }

        private RedisKey Key(string key)
        {
            return this.context.Key(key);
        }

        private ITransaction StartTransaction()
        {
            return this.context.Database.CreateTransaction();
        }

        public void Start()
        {
            contextTransaction = StartTransaction();
            tasks = new List<Task>();
        }


        /// <summary>
        /// executes, waits for all results, returns only results that are of type <typeparamref name="T"/>
        /// 
        /// If there are results that are not of type T, they are not returned.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <returns>Array of T - the results</returns>
        public T[] ExecuteAndWaitTyped<T>()
        {
            return WaitAndConvertResults<T>(Execute());
        }


        /// <summary>
        /// Executes the transaction
        /// </summary>
        /// <returns>Array of non-typed tasks</returns>
        public Task[] Execute()
        {
            if (!RetryUtil.Retry(() => this.contextTransaction.Execute(), 5))
            {
                throw new Exception("Could not commit transaction");
            }

            return tasks.ToArray();
        }

        /// <summary>
        /// Executes, waits for all tasks, returns results as array of object
        /// </summary>
        /// <returns>object[] - all the results, boxed</returns>
        public object[] ExecuteAndWait()
        {
            Task.WaitAll(Execute());
            return tasks.Select(t => GetResult(t)).ToArray();
        }

        private static object GetResult(Task task)
        {
            if (task == null)
                return null;

            var taskType = task.GetType();
            if (!taskType.IsGenericType)
                return null;
            
            while (taskType.BaseType != typeof(Task) && taskType.GetGenericTypeDefinition() != typeof(Task<>))
            {
                taskType = taskType.BaseType;
            }

            var result = taskType.GetProperty(nameof(Task<object>.Result)).GetValue(task);
            return result;
        }

        #region Results
        private List<Task> tasks = new List<Task>();

        private void Record(Task t) => tasks.Add(t);
        #endregion

        #region Set
        /// <summary>
        /// <para>Set a value <paramref name="value"/> on key <paramref name="key"/> with optional expiary <paramref name="expiry"/>
        /// results in a boolean - always true of the set was successful
        /// </para> 
        /// <seealso cref="https://redis.io/commands/set"/>
        /// </summary>
        public void Set(string key, string value, TimeSpan? expiry = null)
        {
            Record(this.contextTransaction.StringSetAsync(Key(key), value, expiry: expiry, flags: commandFlags));
        }

        /// <summary>
        /// <para>Set a value <paramref name="value"/> on key <paramref name="key"/> with optional expiary <paramref name="expiry"/>
        /// results in a boolean - always true of the set was successful
        /// </para> 
        /// <seealso cref="https://redis.io/commands/set"/>
        /// </summary>
        public void Set(string key, bool value, TimeSpan? expiry = null)
        {
            var intValue = value ? -1 : 0;
            Record(this.contextTransaction.StringSetAsync(Key(key), intValue, expiry: expiry, flags: commandFlags));
        }

        /// <summary>
        /// <para>Set a value <paramref name="value"/> on key <paramref name="key"/> with optional expiary <paramref name="expiry"/>
        /// results in a boolean - always true of the set was successful
        /// <seealso cref="https://redis.io/commands/set"/>
        /// </para> 
        /// </summary>
        public void Set(string key, bool? value, TimeSpan? expiry = null)
        {
            var intValue = value.HasValue ? (value.Value ? -1 : 0) : (int?)null;
            Record(this.contextTransaction.StringSetAsync(Key(key), intValue, expiry: expiry, flags: commandFlags));
        }

        /// <summary>
        /// <para>Set a value <paramref name="value"/> on key <paramref name="key"/> with optional expiary <paramref name="expiry"/>
        /// results in a boolean - always true of the set was successful
        /// </para> 
        /// <seealso cref="https://redis.io/commands/set"/>
        /// </summary>
        public void Set(string key, double value, TimeSpan? expiry = null)
        {
            Record(this.contextTransaction.StringSetAsync(Key(key), value, expiry: expiry, flags: commandFlags));
        }

        /// <summary>
        /// <para>Set a value <paramref name="value"/> on key <paramref name="key"/> with optional expiary <paramref name="expiry"/>
        /// results in a boolean - always true of the set was successful
        /// </para> 
        /// <seealso cref="https://redis.io/commands/set"/>
        /// </summary>
        public void Set(string key, double? value, TimeSpan? expiry = null)
        {
            Record(this.contextTransaction.StringSetAsync(Key(key), value, expiry: expiry, flags: commandFlags));
        }

        /// <summary>
        /// <para>Set a value <paramref name="value"/> on key <paramref name="key"/> with optional expiary <paramref name="expiry"/>
        /// results in a boolean - always true of the set was successful
        /// </para> 
        /// <seealso cref="https://redis.io/commands/set"/>
        /// </summary>
        public void Set(string key, int value, TimeSpan? expiry = null)
        {
            Record(this.contextTransaction.StringSetAsync(Key(key), value, expiry: expiry, flags: commandFlags));
        }

        /// <summary>
        /// <para>Set a value <paramref name="value"/> on key <paramref name="key"/> with optional expiary <paramref name="expiry"/>
        /// results in a boolean - always true of the set was successful
        /// </para> 
        /// <seealso cref="https://redis.io/commands/set"/>
        /// </summary>
        public void Set(string key, int? value, TimeSpan? expiry = null)
        {
            Record(this.contextTransaction.StringSetAsync(Key(key), value, expiry: expiry, flags: commandFlags));
        }

        /// <summary>
        /// <para>Set a value <paramref name="value"/> on key <paramref name="key"/> with optional expiary <paramref name="expiry"/>
        /// results in a boolean - always true of the set was successful
        /// </para> 
        /// <seealso cref="https://redis.io/commands/set"/>
        /// </summary>
        public void Set(string key, long value, TimeSpan? expiry = null)
        {
            Record(this.contextTransaction.StringSetAsync(Key(key), value, expiry: expiry, flags: commandFlags));
        }

        /// <summary>
        /// <para>Set a value <paramref name="value"/> on key <paramref name="key"/> with optional expiary <paramref name="expiry"/>
        /// results in a boolean - always true of the set was successful
        /// </para> 
        /// <seealso cref="https://redis.io/commands/set"/>
        /// </summary>
        public void Set(string key, long? value, TimeSpan? expiry = null)
        {
            Record(this.contextTransaction.StringSetAsync(Key(key), value, expiry: expiry, flags: commandFlags));
        }

        #endregion

        #region Get
        
        private Task<RedisValue> Get(string key)
        {
            return this.contextTransaction.StringGetAsync(Key(key), flags: commandFlags);
        }

        /// <summary>
        /// <para><seealso cref="https://redis.io/commands/set"/>
        /// results in a string
        /// </para>
        /// </summary>
        /// <param name="key"></param>
        public void GetString(string key)
        {
            Record(Get(key).ContinueWith(rv => rv.Result.ToStringDefault()));
        }

        /// <summary>
        /// <para><seealso cref="https://redis.io/commands/set"/>
        /// results in a bool
        /// </para>
        /// </summary>
        /// <param name="key"></param>
        public void GetBool(string key)
        {
            Record(Get(key).ContinueWith(rv => rv.Result.ToBoolDefault()));
        }

        /// <summary>
        /// <para><seealso cref="https://redis.io/commands/set"/>
        /// results in a nullable bool
        /// </para>
        /// </summary>
        /// <param name="key"></param>
        public void GetNullableBoolean (string key)
        {
            Record(Get(key).ContinueWith(rv => rv.Result.ToNullableBoolDefault()));
        }

        /// <summary>
        /// <para><seealso cref="https://redis.io/commands/set"/>
        /// results in a double
        /// </para>
        /// </summary>
        /// <param name="key"></param>
        public void GetDouble(string key)
        {
            Record(Get(key).ContinueWith(rv => rv.Result.ToDoubleDefault()));
        }

        /// <summary>
        /// <para><seealso cref="https://redis.io/commands/set"/>
        /// results in a nullable double
        /// </para>
        /// </summary>
        /// <param name="key"></param>
        public void GetNullableDouble(string key)
        {
            Record(Get(key).ContinueWith(rv => rv.Result.ToNullableDoubleDefault()));
        }

        /// <summary>
        /// <para><seealso cref="https://redis.io/commands/set"/>
        /// results in an int
        /// </para>
        /// </summary>
        /// <param name="key"></param>
        public void GetInt(string key)
        {
            Record(Get(key).ContinueWith(rv => rv.Result.ToIntDefault()));
        }

        /// <summary>
        /// <para><seealso cref="https://redis.io/commands/set"/>
        /// results in a nullable int
        /// </para>
        /// </summary>
        /// <param name="key"></param>
        public void GetNullableInt(string key)
        {
            Record(Get(key).ContinueWith(rv => rv.Result.ToNullableIntDefault()));
        }

        /// <summary>
        /// <para><seealso cref="https://redis.io/commands/set"/>
        /// results in a long
        /// </para>
        /// </summary>
        /// <param name="key"></param>
        public void GetLong(string key)
        {
            Record(Get(key).ContinueWith(rv => rv.Result.ToLongDefault()));
        }

        /// <summary>
        /// <para><seealso cref="https://redis.io/commands/set"/>
        /// results in a nullable long
        /// </para>
        /// </summary>
        /// <param name="key"></param>
        public void GetNullableLong(string key)
        {
            Record(Get(key).ContinueWith(rv => rv.Result.ToNullableLongDefault()));
        }
        #endregion

        #region Delete

        /// <summary>
        /// Removes a specified key
        /// Results in boolean - true if key was removed.
        /// <see cref="https://redis.io/commands/del"/>
        /// </summary>
        /// <param name="key">key to delete</param>
        public void Delete(string key)
        {
            Record(this.contextTransaction.KeyDeleteAsync(Key(key), flags: commandFlags));
        }

        /// <summary>
        /// Removes the specified keys
        /// Results in long - number of keys removed
        /// <see cref="https://redis.io/commands/del"/>
        /// </summary>
        /// <param name="keys">keys to delete</param>
        public void Delete(params string[] keys)
        {
            Record(this.contextTransaction.KeyDeleteAsync(keys.Select(k => (RedisKey)Key(k)).ToArray(), flags: commandFlags));
        }

        #endregion

        #region SetOrAppend

        public void SetOrAppend(string key, string value)
        {
            Record(this.contextTransaction.StringAppendAsync(Key(key), value, flags: commandFlags));
        }
        #endregion

        #region Increment

        public void Increment(string key, long value)
        {
            Record(this.contextTransaction.StringIncrementAsync(Key(key), value, flags: commandFlags));
        }

        public void Increment(string key, double value)
        {
            Record(this.contextTransaction.StringIncrementAsync(Key(key), value, flags: commandFlags));
        }

        #endregion

        #region Decrement

        public void Decrement(string key, long value)
        {
            Record(this.contextTransaction.StringDecrementAsync(Key(key), value, flags: commandFlags));
        }

        public void Decrement(string key, double value)
        {
            Record(this.contextTransaction.StringDecrementAsync(Key(key), value, flags: commandFlags));
        }

        #endregion

        #region AtomicExchange

        public void AtomicExchange(string key, string value)
        {
            var rv = this.contextTransaction.StringGetSetAsync(Key(key), (RedisValue)value, flags: commandFlags);
            Record(rv.ContinueWith((val) => val.Result.ToDotNetString(out string previousValue) ? previousValue : default(string)));
        }

        public void AtomicExchange(string key, int? value)
        {
            var rv = this.contextTransaction.StringGetSetAsync(Key(key), (RedisValue)value, flags: commandFlags);
            Record(rv.ContinueWith((val) => val.Result.ToNullableInt(out int? previousValue) ? previousValue : default(int?)));
        }

        public void AtomicExchange(string key, int value)
        {
            var rv = this.contextTransaction.StringGetSetAsync(Key(key), (RedisValue)value, flags: commandFlags);
            Record(rv.ContinueWith((val) => val.Result.ToInt(out int previousValue) ? previousValue : default(int)));
        }

        public void AtomicExchange(string key, long? value)
        {
            var rv = this.contextTransaction.StringGetSetAsync(Key(key), (RedisValue)value, flags: commandFlags);
            Record(rv.ContinueWith((val) => val.Result.ToNullableLong(out long? previousValue) ? previousValue : default(long?)));
        }

        public void AtomicExchange(string key, long value)
        {
            var rv = this.contextTransaction.StringGetSetAsync(Key(key), (RedisValue)value, flags: commandFlags);
            Record(rv.ContinueWith((val) => val.Result.ToLong(out long previousValue) ? previousValue : default(long)));
        }

        public void AtomicExchange(string key, double? value)
        {
            var rv = this.contextTransaction.StringGetSetAsync(Key(key), (RedisValue)value, flags: commandFlags);
            Record(rv.ContinueWith((val) => val.Result.ToNullableDouble(out double? previousValue) ? previousValue : default(double?)));
        }

        public void AtomicExchange(string key, double value)
        {
            var rv = this.contextTransaction.StringGetSetAsync(Key(key), (RedisValue)value, flags: commandFlags);
            Record(rv.ContinueWith((val) => val.Result.ToDouble(out double previousValue) ? previousValue : default(double)));
        }

        public void AtomicExchange(string key, bool? value)
        {
            var rv = this.contextTransaction.StringGetSetAsync(Key(key), (RedisValue)value, flags: commandFlags);
            Record(rv.ContinueWith((val) => val.Result.ToNullableBool(out bool? previousValue) ? previousValue : default(bool?)));
        }

        public void AtomicExchange(string key, bool value)
        {
            var rv = this.contextTransaction.StringGetSetAsync(Key(key), (RedisValue)value, flags: commandFlags);
            Record(rv.ContinueWith((val) => val.Result.ToBool(out bool previousValue) ? previousValue : default(bool)));
        }

        #endregion

        #region TimeToLive
        public void GetTimeToLive(string key)
        {
            Record(this.contextTransaction.KeyTimeToLiveAsync(Key(key), flags: commandFlags));
        }

        public void SetTimeToLive(string key, TimeSpan? expiry)
        {
            Record(this.contextTransaction.KeyExpireAsync(Key(key), expiry, flags: commandFlags));
        }

        #endregion

        #region Sorted Set

        #region Sorted Set Add

        public void SortedSetAdd(string key, string value, double score)
        {
            if (value == null)
            {
                throw new ArgumentNullException(nameof(value));
            }

            Record(this.contextTransaction.SortedSetAddAsync(Key(key), value, score, flags: commandFlags));
        }

        public void SortedSetAdd(string key, byte[] value, double score)
        {
            if (value == null)
            {
                throw new ArgumentNullException(nameof(value));
            }

            Record(this.contextTransaction.SortedSetAddAsync(Key(key), value, score, flags: commandFlags));
        }

        public void SortedSetAdd(string key, bool value, double score)
        {
            var intValue = value ? -1 : 0;
            Record(this.contextTransaction.SortedSetAddAsync(Key(key), intValue, score, flags: commandFlags));
        }


        public void SortedSetAdd(string key, int value, double score)
        {
            Record(this.contextTransaction.SortedSetAddAsync(Key(key), value, score, flags: commandFlags));
        }


        public void SortedSetAdd(string key, double value, double score)
        {
            Record(this.contextTransaction.SortedSetAddAsync(Key(key), value, score, flags: commandFlags));
        }

        
        public void SortedSetAdd(string key, long value, double score)
        {
            Record(this.contextTransaction.SortedSetAddAsync(Key(key), value, score, flags: commandFlags));
        }


        #endregion

        #region Sorted Set Operation
        /// <summary>
        /// Perform set operations on SortedSet values
        /// Records the number of items in the destination set.
        /// </summary>
        /// <param name="destinationKey">the key of the destination sorted set.</param>
        /// <param name="sourceKeys">keys of all the source sorted sets.</param>
        /// <param name="weights">Use this to apply weights to the elements opon aggregation</param>
        /// <param name="op">Union/Intersect/Diff</param>
        /// <param name="aggregation">Aggregation funciton to apply on the elements that are "combined"</param>
        public void SortedSetOperation(
            string destinationKey,
            string[] sourceKeys,
            double[] weights = null,
            Enums.SetOperation op = Enums.SetOperation.Union,
            Enums.SetOperationAggregation aggregation = Enums.SetOperationAggregation.Sum)
        {

            Record(
                        this.contextTransaction.SortedSetCombineAndStoreAsync(
                            op.ToRedisOperation(),
                            Key(destinationKey),
                            sourceKeys.Select((k) => Key(k)).ToArray(),
                            weights: weights,
                            aggregate: aggregation.ToRedisAggregate(),
                            flags: commandFlags)
                    );

        }

        #endregion

        #region Sorted Set Get Range by Rank 
        public void SortedSetGetRangeByRankString(string key, long rangeStart = 0, long rangeEnd = -1, Enums.SortOrder sortOrder = SortOrder.Ascending)
        {
            var t = this.contextTransaction.SortedSetRangeByRankWithScoresAsync(Key(key), start: rangeStart, stop: rangeEnd, order: sortOrder.ToRedisOrder(), flags: commandFlags);
            Record(t.ContinueWith((sse) => sse.Result.Select((s) => s.Element.ToStringDefault()).ToArray()));
        }

        public void SortedSetGetRangeByRankInt(string key, long rangeStart = 0, long rangeEnd = -1, Enums.SortOrder sortOrder = SortOrder.Ascending)
        {
            var t = this.contextTransaction.SortedSetRangeByRankWithScoresAsync(Key(key), start: rangeStart, stop: rangeEnd, order: sortOrder.ToRedisOrder(), flags: commandFlags);
            Record(t.ContinueWith((sse) => sse.Result.Select((s) => s.Element.ToIntDefault()).ToArray()));
        }

        public void SortedSetGetRangeByRankDouble(string key, long rangeStart = 0, long rangeEnd = -1, Enums.SortOrder sortOrder = SortOrder.Ascending)
        {
            var t = this.contextTransaction.SortedSetRangeByRankWithScoresAsync(Key(key), start: rangeStart, stop: rangeEnd, order: sortOrder.ToRedisOrder(), flags: commandFlags);
            Record(t.ContinueWith((sse) => sse.Result.Select((s) => s.Element.ToDoubleDefault()).ToArray()));
        }
        
        public void SortedSetGetRangeByRankLong(string key, long rangeStart = 0, long rangeEnd = -1, Enums.SortOrder sortOrder = SortOrder.Ascending)
        {
            var t = this.contextTransaction.SortedSetRangeByRankWithScoresAsync(Key(key), start: rangeStart, stop: rangeEnd, order: sortOrder.ToRedisOrder(), flags: commandFlags);
            Record(t.ContinueWith((sse) => sse.Result.Select((s) => s.Element.ToLongDefault()).ToArray()));
        }
        
        public void SortedSetGetRangeByRankBool(string key, long rangeStart = 0, long rangeEnd = -1, Enums.SortOrder sortOrder = SortOrder.Ascending)
        {
            var t = this.contextTransaction.SortedSetRangeByRankWithScoresAsync(Key(key), start: rangeStart, stop: rangeEnd, order: sortOrder.ToRedisOrder(), flags: commandFlags);
            Record(t.ContinueWith((sse) => sse.Result.Select((s) => s.Element.ToBoolDefault()).ToArray()));
        }

        public void SortedSetGetRangeByRankByteArray(string key, long rangeStart = 0, long rangeEnd = -1, Enums.SortOrder sortOrder = SortOrder.Ascending)
        {
            var t = this.contextTransaction.SortedSetRangeByRankWithScoresAsync(Key(key), start: rangeStart, stop: rangeEnd, order: sortOrder.ToRedisOrder(), flags: commandFlags);
            Record(t.ContinueWith((sse) => sse.Result.Select((s) => (byte[])s.Element).ToArray()));
        }


        #endregion

        #region Sorted Set Get Range by Score
        public void SortedSetGetRangeByScoreString(string key, double start = double.NegativeInfinity, double stop = double.PositiveInfinity, Enums.SortOrder sortOrder = SortOrder.Ascending, long skip = 0, long take = -1)
        {
            var t = this.contextTransaction.SortedSetRangeByScoreWithScoresAsync(Key(key), start: start, stop: stop, exclude: Exclude.None,
                order: sortOrder.ToRedisOrder(), skip: skip, take: take, flags: commandFlags);

            Record(t.ContinueWith((sse) => sse.Result.Select((s) => s.Element.ToStringDefault()).ToArray()));
        }

        public void SortedSetGetRangeByScoreInt(string key, double start = double.NegativeInfinity, double stop = double.PositiveInfinity, Enums.SortOrder sortOrder = SortOrder.Ascending, long skip = 0, long take = -1)
        {
            var t = this.contextTransaction.SortedSetRangeByScoreWithScoresAsync(Key(key), start: start, stop: stop, exclude: Exclude.None, order: sortOrder.ToRedisOrder(), skip: skip, take: take, flags: commandFlags);
            Record(t.ContinueWith((sse) => sse.Result.Select((s) => s.Element.ToIntDefault()).ToArray()));
        }

        public void SortedSetGetRangeByScoreDouble(string key, double start = double.NegativeInfinity, double stop = double.PositiveInfinity, Enums.SortOrder sortOrder = SortOrder.Ascending, long skip = 0, long take = -1)
        {
            var t = this.contextTransaction.SortedSetRangeByScoreWithScoresAsync(Key(key), start: start, stop: stop, exclude: Exclude.None,
                order: sortOrder.ToRedisOrder(), skip: skip, take: take, flags: commandFlags);

            Record(t.ContinueWith((sse) => sse.Result.Select((s) => s.Element.ToDoubleDefault()).ToArray()));
        }
        
        public void SortedSetGetRangeByScoreLong(string key, double start = double.NegativeInfinity, double stop = double.PositiveInfinity, Enums.SortOrder sortOrder = SortOrder.Ascending, long skip = 0, long take = -1)
        {
            var t = this.contextTransaction.SortedSetRangeByScoreWithScoresAsync(Key(key), start: start, stop: stop, exclude: Exclude.None,
                order: sortOrder.ToRedisOrder(), skip: skip, take: take, flags: commandFlags);

            Record(t.ContinueWith((sse) => sse.Result.Select((s) => s.Element.ToLongDefault()).ToArray()));
        }

        public void SortedSetGetRangeByScoreBool(string key, double start = double.NegativeInfinity, double stop = double.PositiveInfinity, Enums.SortOrder sortOrder = SortOrder.Ascending, long skip = 0, long take = -1)
        {
            var t = this.contextTransaction.SortedSetRangeByScoreWithScoresAsync(Key(key), start: start, stop: stop, exclude: Exclude.None,
                order: sortOrder.ToRedisOrder(), skip: skip, take: take, flags: commandFlags);

            Record(t.ContinueWith((sse) => sse.Result.Select((s) => s.Element.ToBoolDefault()).ToArray()));
        }

        #endregion

        #region Sorted Set Get Rank for Value
        public void SortedSetGetRankForValue(string key, string value, Enums.SortOrder sortOrder = SortOrder.Ascending)
        {
            Record(this.contextTransaction.SortedSetRankAsync(Key(key), value, sortOrder.ToRedisOrder()));
        }

        public void SortedSetGetRankForValue(string key, bool value, Enums.SortOrder sortOrder = SortOrder.Ascending)
        {
            var intValue = value ? -1 : 0;
            Record(this.contextTransaction.SortedSetRankAsync(Key(key), intValue, sortOrder.ToRedisOrder()));
        }

        public void SortedSetGetRankForValue(string key, bool? value, Enums.SortOrder sortOrder = SortOrder.Ascending)
        {
            var intValue = value.HasValue ? (value.Value ? -1 : 0) : (int?)null;
            Record(this.contextTransaction.SortedSetRankAsync(Key(key), intValue, sortOrder.ToRedisOrder()));
        }

        public void SortedSetGetRankForValue(string key, double value, Enums.SortOrder sortOrder = SortOrder.Ascending)
        {
            Record(this.contextTransaction.SortedSetRankAsync(Key(key), value, sortOrder.ToRedisOrder()));
        }

        public void SortedSetGetRankForValue(string key, double? value, Enums.SortOrder sortOrder = SortOrder.Ascending)
        {
            Record(this.contextTransaction.SortedSetRankAsync(Key(key), value, sortOrder.ToRedisOrder()));
        }

        public void SortedSetGetRankForValue(string key, int value, Enums.SortOrder sortOrder = SortOrder.Ascending)
        {
            Record(this.contextTransaction.SortedSetRankAsync(Key(key), value, sortOrder.ToRedisOrder()));
        }

        public void SortedSetGetRankForValue(string key, int? value, Enums.SortOrder sortOrder = SortOrder.Ascending)
        {
            Record(this.contextTransaction.SortedSetRankAsync(Key(key), value, sortOrder.ToRedisOrder()));
        }

        public void SortedSetGetRankForValue(string key, long value, Enums.SortOrder sortOrder = SortOrder.Ascending)
        {
            Record(this.contextTransaction.SortedSetRankAsync(Key(key), value, sortOrder.ToRedisOrder()));
        }

        public void SortedSetGetRankForValue(string key, long? value, Enums.SortOrder sortOrder = SortOrder.Ascending)
        {
            Record(this.contextTransaction.SortedSetRankAsync(Key(key), value, sortOrder.ToRedisOrder()));
        }

        #endregion

        #region Sorted Set Remove Range By Rank
        public void SortedSetRemoveRangeByRank(string key, long start, long end = -1)
        {
            Record(this.contextTransaction.SortedSetRemoveRangeByRankAsync(Key(key), start, end, flags: commandFlags));
        }
        #endregion

        #region Sorted Set Remove By Score
        public void SortedSetRemoveRangeByScore(string key, double start, double stop)
        {
            Record(this.contextTransaction.SortedSetRemoveRangeByScoreAsync(Key(key), start, stop, Exclude.None, commandFlags));
        }
        #endregion

        #endregion




    }
}
