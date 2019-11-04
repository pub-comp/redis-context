using System;
using System.Collections;
using System.Collections.Generic;
using StackExchange.Redis;

namespace PubComp.RedisRepo
{
    public interface IRedisContext
    {
        bool AtomicExchange(string key, bool value);
        bool? AtomicExchange(string key, bool? value);
        double AtomicExchange(string key, double value);
        double? AtomicExchange(string key, double? value);
        int AtomicExchange(string key, int value);
        int? AtomicExchange(string key, int? value);
        long AtomicExchange(string key, long value);
        long? AtomicExchange(string key, long? value);
        string AtomicExchange(string key, string value);
        double Decrement(string key, double value);
        long Decrement(string key, long value);
        void Delete(params string[] keys);
        void Delete(string key);
        IEnumerable<string> GetKeys(string pattern = null);
        bool Exists(string key);
        long MultipleExist(string[] keys);
        TimeSpan? GetTimeToLive(string key);
        double Increment(string key, double value);
        long Increment(string key, long value);
        void Set(string key, bool value, TimeSpan? expiry = null);
        bool Set(string key, string value, Enums.When when, TimeSpan? expiry = null);
        void Set(string key, bool? value, TimeSpan? expiry = null);
        void Set(string key, double value, TimeSpan? expiry = null);
        void Set(string key, double? value, TimeSpan? expiry = null);
        void Set(string key, int value, TimeSpan? expiry = null);
        void Set(string key, int? value, TimeSpan? expiry = null);
        void Set(string key, long value, TimeSpan? expiry = null);
        void Set(string key, long? value, TimeSpan? expiry = null);
        void Set(string key, string value, TimeSpan? expiry = null);
        void SetOrAppend(string key, string value);
        void SetTimeToLive(string key, TimeSpan? expiry);
        bool TryGet(string key, out bool value);
        bool TryGet(string key, out bool? value);
        bool TryGet(string key, out double value);
        bool TryGet(string key, out double? value);
        bool TryGet(string key, out int value);
        bool TryGet(string key, out int? value);
        bool TryGet(string key, out long value);
        bool TryGet(string key, out long? value);
        bool TryGet(string key, out string value);

        #region Redis Lists

        /// <summary>
        /// Adds <paramref name="value"/> to the end of a list that is at <paramref name="key"/>.
        /// If the list doesn't exist then it is created.
        /// </summary>
        /// <returns>The length of the list after the addition</returns>
        long AddToList(string key, string value);

        /// <summary>
        /// Adds <paramref name="values"/> to the end of a list that is at <paramref name="key"/>.
        /// If the list doesn't exist then it is created.
        /// </summary>
        /// <returns>The length of the list after the addition</returns>
        long AddRangeToList(string key, string[] values);

        /// <summary>
        /// Returns the list that is at <paramref name="key"/>.
        /// If <paramref name="start"/> and <paramref name="stop"/> are not given then the whole list will be returned.
        /// Else, a sub-list is returned that starts at the index <paramref name="start"/> and stops at the index <paramref name="stop"/>.
        /// Please note that the list is zero-based indexed (so 0 is is the first element).
        /// If <paramref name="start"/> or <paramref name="stop"/> is negative then it means it's counted from the end of the list (-1 is the last element, -2 is the element before the last element and so on).
        /// If the index is out-of-bounds then instead of throwing an exception the index is initialized to the nearest boundary (start or end of the list), and only then the operation will be done.
        /// </summary>
        string[] GetList(string key, long start = 0, long stop = -1);

        #endregion

        #region Redis Sets

        bool SetAdd<T>(string key, T value);

        long SetAdd<T>(string key, T[] values);

        bool SetRemove<T>(string key, T value);

        long SetRemove<T>(string key, T[] values);

        long SetLength(string key);

        T[] SetGetItems<T>(string key, Func<object, T> redisValueConverter);

        T[] SetsUnion<T>(string[] keys, Func<object, T> redisValueConverter);

        T[] SetsIntersect<T>(string[] keys, Func<object, T> redisValueConverter);

        T[] SetsDiff<T>(string[] keys, Func<object, T> redisValueConverter);

        void AddToSet(string key, string[] values);

        long CountSetMembers(string key);

        string[] GetSetMembers(string key);

        /// <summary>
        /// Get the diff between the set at index 0 of <paramref name="keys"/> and all other sets in <paramref name="keys"/>
        /// </summary>
        string[] GetSetsDifference(string[] keys);

        /// <summary>
        /// Union sets at keys <paramref name="setKeys"/>
        /// </summary>
        string[] UnionSets(string[] keys);

        /// <summary>
        /// Intersect sets at keys <paramref name="keys"/>
        /// </summary>
        string[] IntersectSets(string[] keys);

        /// <summary>
        /// Get the diff between the set at index 0 of <paramref name="keys"/> and all other sets in <paramref name="keys"/>
        /// store the result at <param name="destinationKey"></param>
        /// </summary>
        void StoreSetsDifference(string destinationKey, string[] keys);

        /// <summary>
        /// Union sets at keys <paramref name="keys"/>
        /// store the result at <param name="destinationKey"></param>
        /// </summary>
        void UnionSetsAndStore(string destinationKey, string[] keys);

        /// <summary>
        /// Intersect sets at keys <paramref name="keys"/>
        /// store the result at <param name="destinationKey"></param>
        /// </summary>
        void IntersectSetsAndStore(string destinationKey, string[] keys);

        bool SetContains(string key, string member);

        bool TryGetDistributedLock(string lockObjectName, string lockerName, TimeSpan lockTtl);

        void ReleaseDistributedLock(string lockObjectName, string lockerName);

        #endregion

        #region Redis Hashes

        /// <summary>
        /// Add or update new pairs to a specific key in Hashes data type
        /// </summary>
        /// <param name="key"></param>
        /// <param name="value">Pairs of fields and values</param>
        void HashesSet(string key, IDictionary<object, object> value);

        /// <summary>
        /// Add or update new pair of field and value to a specific key in Hashes data type
        /// </summary>
        void HashesSet<T, TK>(string key, T fieldName, TK value);

        bool HashesTryGetField<T, TK>(string key, T fieldName, out TK value);

        IDictionary<object, object> HashesGetAll(string key);

        bool HashesDeleteField(string key, string fieldName);

        bool HashesDeleteField(string key, bool fieldName);

        bool HashesDeleteField(string key, int fieldName);

        bool HashesDeleteField(string key, long fieldName);

        bool HashesDeleteField(string key, double fieldName);

        /// <summary>
        /// Returns the number of fields in the Hashes data type for a specific key
        /// </summary>
        long HashesLength(string key);

        #endregion

        #region Lua Scripting

        /// <summary>
        /// Return a RedisScriptKeysAndArguments instance that can be passed later alongside a script
        /// </summary>
        RedisScriptKeysAndArguments CreateScriptKeyAndArguments();

        /// <summary>
        /// Run a lua script against the connected redis instance
        /// </summary>
        /// <param name="script">the script to run. Keys should be @Key1, @Key2 ... @Key10. Int arguments: @IntArg1 .. @IntArg20. String arguments: @StringArg1 .. @StringArg20</param>
        /// <param name="keysAndParameters">an instance of RedisScriptKeysAndArguments</param>
        void RunScript(string script, RedisScriptKeysAndArguments keysAndParameters);

        /// <summary>
        /// Run a lua script against the connected redis instance
        /// </summary>
        /// <param name="script">the script to run. Keys should be @Key1, @Key2 ... @Key10. Int arguments: @IntArg1 .. @IntArg20. String arguments: @StringArg1 .. @StringArg20</param>
        /// <param name="keysAndParameters">an instance of RedisScriptKeysAndArguments</param>
        /// <returns>result as string</returns>
        string RunScriptString(string script, RedisScriptKeysAndArguments keysAndParameters);

        /// <summary>
        /// Run a lua script against the connected redis instance
        /// </summary>
        /// <param name="script">the script to run. Keys should be @Key1, @Key2 ... @Key10. Int arguments: @IntArg1 .. @IntArg20. String arguments: @StringArg1 .. @StringArg20</param>
        /// <param name="keysAndParameters">an instance of RedisScriptKeysAndArguments</param>
        /// <returns>result as bool</returns>
        bool RunScriptBool(string script, RedisScriptKeysAndArguments keysAndParameters);

        /// <summary>
        /// Run a lua script against the connected redis instance
        /// </summary>
        /// <param name="script">the script to run. Keys should be @Key1, @Key2 ... @Key10. Int arguments: @IntArg1 .. @IntArg20. String arguments: @StringArg1 .. @StringArg20</param>
        /// <param name="keysAndParameters">an instance of RedisScriptKeysAndArguments</param>
        /// <returns>result as int</returns>
        int RunScriptInt(string script, RedisScriptKeysAndArguments keysAndParameters);

        /// <summary>
        /// Run a lua script against the connected redis instance
        /// </summary>
        /// <param name="script">the script to run. Keys should be @Key1, @Key2 ... @Key10. Int arguments: @IntArg1 .. @IntArg20. String arguments: @StringArg1 .. @StringArg20</param>
        /// <param name="keysAndParameters">an instance of RedisScriptKeysAndArguments</param>
        /// <returns>result as string</returns>
        long RunScriptLong(string script, RedisScriptKeysAndArguments keysAndParameters);

        /// <summary>
        /// Run a lua script against the connected redis instance
        /// </summary>
        /// <param name="script">the script to run. Keys should be @Key1, @Key2 ... @Key10. Int arguments: @IntArg1 .. @IntArg20. String arguments: @StringArg1 .. @StringArg20</param>
        /// <param name="keysAndParameters">an instance of RedisScriptKeysAndArguments</param>
        /// <returns>result as double</returns>
        double RunScriptDouble(string script, RedisScriptKeysAndArguments keysAndParameters);

        /// <summary>
        /// Run a lua script against the connected redis instance
        /// </summary>
        /// <param name="script">the script to run. Keys should be @Key1, @Key2 ... @Key10. Int arguments: @IntArg1 .. @IntArg20. String arguments: @StringArg1 .. @StringArg20</param>
        /// <param name="keysAndParameters">an instance of RedisScriptKeysAndArguments</param>
        /// <returns>result as byte array</returns>
        byte[] RunScriptByteArray(string script, RedisScriptKeysAndArguments keysAndParameters);

        /// <summary>
        /// Run a lua script against the connected redis instance
        /// </summary>
        /// <param name="script">the script to run. Keys should be @Key1, @Key2 ... @Key10. Int arguments: @IntArg1 .. @IntArg20. String arguments: @StringArg1 .. @StringArg20</param>
        /// <param name="keysAndParameters">an instance of RedisScriptKeysAndArguments</param>
        /// <returns>result as string[]</returns>
        string[] RunScriptStringArray(string script, RedisScriptKeysAndArguments keysAndParameters);

        #endregion

        #region Redis Sorted Sets

        #region AddToSortedSet

        bool AddToSortedSet<T>(
            string key, T value, double score, Enums.When when = Enums.When.Always);

        #endregion

        #region AddToSortedSet[]

        long SortedSetAdd<T>(
            string key, (T value, double score)[] values,
            Enums.When when = Enums.When.Always);

        #endregion

        long SortedSetGetLength(
            string key, double min = double.NegativeInfinity,
            double max = double.PositiveInfinity, Enums.Exclude exclude = Enums.Exclude.None);

        #region GetRange

        T[] SortedSetGetRangeByScore<T>(
            string key, Func<object, T> redisValueConverter,
            double start = double.NegativeInfinity, double end = double.PositiveInfinity,
            Enums.SortOrder order = Enums.SortOrder.Ascending);

        T[] SortedSetGetRangeByRank<T>(
            string key, Func<object, T> redisValueConverter,
            long start = 0, long end = -1, Enums.SortOrder order = Enums.SortOrder.Ascending);

        List<(T value, double score)> SortedSetGetRangeByScoreWithScores<T>(
            string key, Func<object, T> redisValueConverter,
            double start = double.NegativeInfinity,
            double end = double.PositiveInfinity, Enums.SortOrder order = Enums.SortOrder.Ascending);

        List<(T value, double score)> SortedSetGetRangeByRankWithScores<T>(
            string key, Func<object, T> redisValueConverter,
            long start = 0, long end = -1, Enums.SortOrder order = Enums.SortOrder.Ascending);

        #endregion

        #region Remove

        bool SortedSetRemove<T>(string key, T value);

        long SortedSetRemove<T>(string key, T[] values);

        long SortedSetRemoveRangeByScore(
            string key, double start, double end, Enums.Exclude exclude = Enums.Exclude.None);

        long SortedSetRemoveRangeByRank(
            string key, long start, long end);

        #endregion

        #endregion
    }
}