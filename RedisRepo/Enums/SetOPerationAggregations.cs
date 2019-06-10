using StackExchange.Redis;
using System.Collections.Generic;

namespace PubComp.RedisRepo.Enums
{
    public enum SetOperationAggregation { Sum, Min, Max };

    internal static class SetOPerationAggregationsExtensions
    {
        private static Dictionary<SetOperationAggregation, Aggregate> mapping = new Dictionary<SetOperationAggregation, Aggregate>();
        static SetOPerationAggregationsExtensions()
        {
            mapping[SetOperationAggregation.Max] = Aggregate.Max;
            mapping[SetOperationAggregation.Min] = Aggregate.Min;
            mapping[SetOperationAggregation.Sum] = Aggregate.Sum;
        }

        internal static Aggregate ToRedisAggregate(this SetOperationAggregation agg)
        {
            var res = Aggregate.Sum;
            mapping.TryGetValue(agg, out res);

            return res;
        }
    }

    
}
