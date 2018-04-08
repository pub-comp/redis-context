using StackExchange.Redis;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace PubComp.RedisRepo.Enums
{
    public enum SetOPerationAggregations { Sum, Min, Max };

    internal static class SetOPerationAggregationsExtensions
    {
        private static Dictionary<SetOPerationAggregations, Aggregate> mapping = new Dictionary<SetOPerationAggregations, Aggregate>();
        static SetOPerationAggregationsExtensions()
        {
            mapping[SetOPerationAggregations.Max] = Aggregate.Max;
            mapping[SetOPerationAggregations.Min] = Aggregate.Min;
            mapping[SetOPerationAggregations.Sum] = Aggregate.Sum;
        }

        internal static Aggregate ToRedisAggregate(this SetOPerationAggregations agg)
        {
            var res = Aggregate.Sum;
            mapping.TryGetValue(agg, out res);

            return res;
        }
    }

    
}
