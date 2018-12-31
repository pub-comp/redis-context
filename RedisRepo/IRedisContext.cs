using System;
using System.Collections.Generic;

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
        TimeSpan? GetTimeToLive(string key);
        double Increment(string key, double value);
        long Increment(string key, long value);
        void Set(string key, bool value, TimeSpan? expiry = null);
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

        #region Redis Sets

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



        #endregion
    }
}