using System.Collections.Generic;

namespace PubComp.RedisRepo.Enums
{
    public enum SortOrder { Ascending, Descending }

    internal static class SortOrdersExtensions
    {
        private static Dictionary<SortOrder, StackExchange.Redis.Order> mapping = new Dictionary<SortOrder, StackExchange.Redis.Order>();

        static SortOrdersExtensions()
        {
            mapping[SortOrder.Ascending] = StackExchange.Redis.Order.Ascending;
            mapping[SortOrder.Descending] = StackExchange.Redis.Order.Descending;
        }

        internal static StackExchange.Redis.Order ToRedisOrder(this SortOrder order)
        {
            var res = StackExchange.Redis.Order.Ascending;
            mapping.TryGetValue(order, out res);

            return res;
        }
    }
}
