using System;
using System.Collections.Generic;
using StackExchange.Redis;
using PubComp.RedisRepo.Enums;

namespace PubComp.RedisRepo
{
    public static class Extensions
    {
        public static RedisKey[] ToRedisKeysArray(this string[] keys)
        {
            if (keys == null || keys.Length == 0) return Array.Empty<RedisKey>();

            return Array.ConvertAll(keys, x => (RedisKey)x);
        }
    }

    internal static class ExcludeExtensions
    {
        private static Dictionary<Enums.Exclude, StackExchange.Redis.Exclude> mapping =
            new Dictionary<Enums.Exclude, StackExchange.Redis.Exclude>();

        static ExcludeExtensions()
        {
            mapping[Enums.Exclude.None] = StackExchange.Redis.Exclude.None;
            mapping[Enums.Exclude.Both] = StackExchange.Redis.Exclude.Both;
            mapping[Enums.Exclude.Start] = StackExchange.Redis.Exclude.Start;
            mapping[Enums.Exclude.Stop] = StackExchange.Redis.Exclude.Stop;
        }

        internal static StackExchange.Redis.Exclude ToSE(this Enums.Exclude order)
        {
            var res = StackExchange.Redis.Exclude.None;
            mapping.TryGetValue(order, out res);

            return res;
        }
    }

    internal static class SetOperationsExtensions
    {
        private static Dictionary<Enums.SetOperation, StackExchange.Redis.SetOperation> mapping =
            new Dictionary<Enums.SetOperation, StackExchange.Redis.SetOperation>();

        static SetOperationsExtensions()
        {
            mapping[Enums.SetOperation.Union] = StackExchange.Redis.SetOperation.Union;
            mapping[Enums.SetOperation.Intersect] = StackExchange.Redis.SetOperation.Intersect;
            mapping[Enums.SetOperation.Difference] = StackExchange.Redis.SetOperation.Difference;
        }

        internal static StackExchange.Redis.SetOperation ToSE(this Enums.SetOperation op)
        {
            var res = StackExchange.Redis.SetOperation.Union;
            mapping.TryGetValue(op, out res);
            return res;
        }

    }

    internal static class SetOPerationAggregationsExtensions
    {
        private static Dictionary<SetOperationAggregation, Aggregate> mapping =
            new Dictionary<SetOperationAggregation, Aggregate>();

        static SetOPerationAggregationsExtensions()
        {
            mapping[SetOperationAggregation.Sum] = Aggregate.Sum;
            mapping[SetOperationAggregation.Max] = Aggregate.Max;
            mapping[SetOperationAggregation.Min] = Aggregate.Min;
        }

        internal static Aggregate ToSE(this SetOperationAggregation agg)
        {
            var res = Aggregate.Sum;
            mapping.TryGetValue(agg, out res);

            return res;
        }
    }

    internal static class SortOrdersExtensions
    {
        private static Dictionary<SortOrder, StackExchange.Redis.Order> mapping =
            new Dictionary<SortOrder, StackExchange.Redis.Order>();

        static SortOrdersExtensions()
        {
            mapping[SortOrder.Ascending] = StackExchange.Redis.Order.Ascending;
            mapping[SortOrder.Descending] = StackExchange.Redis.Order.Descending;
        }

        internal static StackExchange.Redis.Order ToSE(this SortOrder order)
        {
            var res = StackExchange.Redis.Order.Ascending;
            mapping.TryGetValue(order, out res);

            return res;
        }
    }

    internal static class WhenExtensions
    {
        private static Dictionary<Enums.When, StackExchange.Redis.When> mapping =
            new Dictionary<Enums.When, StackExchange.Redis.When>();

        static WhenExtensions()
        {
            mapping[Enums.When.Always] = StackExchange.Redis.When.Always;
            mapping[Enums.When.Exists] = StackExchange.Redis.When.Exists;
            mapping[Enums.When.NotExists] = StackExchange.Redis.When.NotExists;
        }

        internal static StackExchange.Redis.When ToSE(this Enums.When order)
        {
            var res = StackExchange.Redis.When.Always;
            mapping.TryGetValue(order, out res);

            return res;
        }
    }
}
