using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using StackExchange.Redis;

namespace PubComp.RedisRepo
{
    public static class Extensions
    {
        public static RedisKey[] ToRedisKeysArray(this string[] keys)
        {
            if (keys == null || keys.Length == 0) return Array.Empty<RedisKey>();

            return Array.ConvertAll(keys, x => (RedisKey) x);
        }
    }
}
