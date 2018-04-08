using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace PubComp.RedisRepo.Enums
{
    public enum SortOrders { Ascending, Descending }

    internal static class SortOrdersExtensions
    {
        private static Dictionary<SortOrders, StackExchange.Redis.Order> mapping = new Dictionary<SortOrders, StackExchange.Redis.Order>();

        static SortOrdersExtensions()
        {
            mapping[SortOrders.Ascending] = StackExchange.Redis.Order.Ascending;
            mapping[SortOrders.Descending] = StackExchange.Redis.Order.Descending;
        }

        internal static StackExchange.Redis.Order ToRedisOrder(this SortOrders order)
        {
            var res = StackExchange.Redis.Order.Ascending;
            mapping.TryGetValue(order, out res);

            return res;
        }
    }
}
