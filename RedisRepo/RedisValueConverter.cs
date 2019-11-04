using StackExchange.Redis;

namespace PubComp.RedisRepo
{
    public static class RedisValueConverter
    {
        public static byte[] ToBinary(object redisValue)
        {
            return ((RedisValue)redisValue).ToBinaryDefault();
        }

        public static string ToString(object redisValue)
        {
            return ((RedisValue)redisValue).ToStringDefault();
        }

        public static bool ToBool(object redisValue)
        {
            return ((RedisValue)redisValue).ToBoolDefault();
        }

        public static double ToDouble(object redisValue)
        {
            return ((RedisValue)redisValue).ToDoubleDefault();
        }

        public static int ToInt(object redisValue)
        {
            return ((RedisValue)redisValue).ToIntDefault();
        }

        public static long ToLong(object redisValue)
        {
            return ((RedisValue)redisValue).ToLongDefault();
        }

        public static bool? ToNullableBool(object redisValue)
        {
            return ((RedisValue)redisValue).ToNullableBoolDefault();
        }

        public static double? ToNullableDouble(object redisValue)
        {
            return ((RedisValue)redisValue).ToNullableDoubleDefault();
        }

        public static int? ToNullableInt(object redisValue)
        {
            return ((RedisValue)redisValue).ToNullableIntDefault();
        }

        public static long? ToNullableLong(object redisValue)
        {
            return ((RedisValue)redisValue).ToNullableLongDefault();
        }
    }
}
