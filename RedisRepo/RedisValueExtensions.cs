using StackExchange.Redis;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace PubComp.RedisRepo
{
    public static class RedisValueExtensions
    {
        private delegate bool TConvert<T>(RedisValue rv, out T value);

        private static T ToTDefault<T>(this RedisValue redisValue, TConvert<T> converter, T def = default(T)) 
        {
            return converter(redisValue, out T res) ? res : def;
        }

        internal static string ToStringDefault(this RedisValue redisValue)
        {
            return ToTDefault(redisValue, ToDotNetString, default(string));
        }

        internal static int ToIntDefault(this RedisValue redisValue)
        {
            return ToTDefault(redisValue, ToInt, default(int));
        }

        internal static int? ToNullableIntDefault(this RedisValue redisValue)
        {
            return ToTDefault(redisValue, ToNullableInt, (int?)null);
        }

        internal static double ToDoubleDefault(this RedisValue redisValue)
        {
            return ToTDefault(redisValue, ToDouble, default(double));
        }

        internal static double? ToNullableDoubleDefault(this RedisValue redisValue)
        {
            return ToTDefault(redisValue, ToNullableDouble, (double?)null);
        }

        internal static bool ToBoolDefault(this RedisValue redisValue)
        {
            return ToTDefault(redisValue, ToBool, default(bool));
        }

        internal static bool? ToNullableBoolDefault(this RedisValue redisValue)
        {
            return ToTDefault(redisValue, ToNullableBool, (bool?)null);
        }

        internal static long ToLongDefault(this RedisValue redisValue)
        {
            return ToTDefault(redisValue, ToLong, default(long));
        }

        internal static long? ToNullableLongDefault(this RedisValue redisValue)
        {
            return ToTDefault(redisValue, ToNullableLong, (long?)null);
        }
        
        internal static bool ToDotNetString(this RedisValue redisValue, out string value)
        {
            value = redisValue.HasValue ? redisValue.ToString() : default(string);
            return redisValue.HasValue;
        }

        internal static bool ToNullableInt(this RedisValue redisValue, out int? value)
        {
            value = null;

            if (!redisValue.HasValue)
                return false;

            if (redisValue.IsNull)
                return true;

            if (redisValue.TryParse(out int result))
            {
                value = result;
                return true;
            }

            return false;
        }

        internal static bool ToInt(this RedisValue redisValue, out int value)
        {
            value = default(int);

            if (!redisValue.HasValue)
                return false;

            if (redisValue.IsNull)
                return true;

            return redisValue.TryParse(out value);
        }

        internal static bool ToNullableLong(this RedisValue redisValue, out long? value)
        {
            value = null;

            if (!redisValue.HasValue)
                return false;

            if (redisValue.IsNull)
                return true;

            if (redisValue.TryParse(out long result))
            {
                value = result;
                return true;
            }

            return false;
        }

        internal static bool ToLong(this RedisValue redisValue, out long value)
        {
            value = default(long);

            if (!redisValue.HasValue)
                return false;

            if (redisValue.IsNull)
                return true;

            return redisValue.TryParse(out value);
        }

        internal static bool ToNullableDouble(this RedisValue redisValue, out double? value)
        {
            value = null;

            if (!redisValue.HasValue)
                return false;

            if (redisValue.IsNull)
                return true;

            double result;
            if (redisValue.TryParse(out result))
            {
                value = result;
                return true;
            }

            return false;
        }

        internal static bool ToDouble(this RedisValue redisValue, out double value)
        {
            value = default(double);

            if (!redisValue.HasValue)
                return false;

            if (redisValue.IsNull)
                return true;

            return redisValue.TryParse(out value);
        }

        internal static bool ToNullableBool(this RedisValue redisValue, out bool? value)
        {
            var result = ToNullableInt(redisValue, out int? intValue);
            value = intValue.HasValue ? intValue.Value != 0 : (bool?)null;
            return result;
        }

        internal static bool ToBool(this RedisValue redisValue, out bool value)
        {
            var result = ToInt(redisValue, out int intValue);
            value = intValue != 0;
            return result;
        }
    }
}
