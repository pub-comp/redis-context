using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Payoneer.Infra.RedisRepo.Enums
{
    public enum SetOperations { Union, Intersect, Difference }

    internal static class SetOperationsExtensions
    {
        private static Dictionary<SetOperations, StackExchange.Redis.SetOperation> mapping = new Dictionary<SetOperations, StackExchange.Redis.SetOperation>();

        static SetOperationsExtensions()
        {
            mapping[SetOperations.Union] = StackExchange.Redis.SetOperation.Union;
            mapping[SetOperations.Intersect] = StackExchange.Redis.SetOperation.Intersect;
            mapping[SetOperations.Difference] = StackExchange.Redis.SetOperation.Difference;
        }

        internal static StackExchange.Redis.SetOperation ToRedisOperation(this SetOperations op)
        {
            var res = StackExchange.Redis.SetOperation.Union;
            mapping.TryGetValue(op, out res);
            return res;
        }

    }
}
