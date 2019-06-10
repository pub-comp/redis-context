using System.Collections.Generic;

namespace PubComp.RedisRepo.Enums
{
    public enum SetOperation { Union, Intersect, Difference }

    internal static class SetOperationsExtensions
    {
        private static Dictionary<SetOperation, StackExchange.Redis.SetOperation> mapping = new Dictionary<SetOperation, StackExchange.Redis.SetOperation>();

        static SetOperationsExtensions()
        {
            mapping[SetOperation.Union] = StackExchange.Redis.SetOperation.Union;
            mapping[SetOperation.Intersect] = StackExchange.Redis.SetOperation.Intersect;
            mapping[SetOperation.Difference] = StackExchange.Redis.SetOperation.Difference;
        }

        internal static StackExchange.Redis.SetOperation ToRedisOperation(this SetOperation op)
        {
            var res = StackExchange.Redis.SetOperation.Union;
            mapping.TryGetValue(op, out res);
            return res;
        }

    }
}
