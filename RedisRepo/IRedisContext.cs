using System;
using System.Collections.Generic;

namespace Payoneer.Infra.Repo
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
    }
}