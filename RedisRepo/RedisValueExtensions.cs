using StackExchange.Redis;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;

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

        internal static bool ToBinary(this RedisValue redisValue, out byte[] value)
        {
            if (redisValue.IsNull)
            {
                value = null;
                return false;
            }

            value = (byte[])redisValue;
            return true;
        }

        internal static byte[] ToBinaryDefault(this RedisValue redisValue)
        {
            return (redisValue.IsNull ? null : (byte[])redisValue);
        }

        internal static RedisValue ToRedis<T>(this T value)
        {
            if (typeof(T) == typeof(byte[]) || value is byte[])
                return (byte[])(object)value;

            if (typeof(T) == typeof(string) || value is string)
                return (string)(object)value;

            if (typeof(T) == typeof(bool) || value is bool)
                return Convert.ToBoolean(value);

            if (typeof(T) == typeof(bool?))
                return value != null ? Convert.ToBoolean(value) : (bool?)null;

            if (typeof(T) == typeof(int) || value is int)
                return Convert.ToInt32(value);

            if (typeof(T) == typeof(int?))
                return value != null ? Convert.ToInt32(value) : (int?)null;

            if (typeof(T) == typeof(double) || value is double)
                return Convert.ToDouble(value);

            if (typeof(T) == typeof(double?))
                return value != null ? Convert.ToDouble(value) : (double?)null;

            if (typeof(T) == typeof(long) || value is long)
                return Convert.ToInt64(value);

            if (typeof(T) == typeof(long?))
                return value != null ? Convert.ToInt64(value) : (long?)null;

            throw new NotSupportedException(typeof(T).FullName);
        }

        internal static RedisValue[] ToRedisArray<T>(this IEnumerable<T> values)
        {
            if (typeof(T) == typeof(byte[]))
                return values.Select(v => (RedisValue)(byte[])(object)v).ToArray();

            if (typeof(T) == typeof(string))
                return values.OfType<string>().Select(v => (RedisValue)v).ToArray();

            if (typeof(T) == typeof(bool))
                return values.OfType<bool>().Select(v => (RedisValue)v).ToArray();

            if (typeof(T) == typeof(bool?))
                return values.OfType<bool?>().Select(v => (RedisValue)v).ToArray();

            if (typeof(T) == typeof(int))
                return values.OfType<int>().Select(v => (RedisValue)v).ToArray();

            if (typeof(T) == typeof(int?))
                return values.OfType<int?>().Select(v => (RedisValue)v).ToArray();

            if (typeof(T) == typeof(double))
                return values.OfType<double>().Select(v => (RedisValue)v).ToArray();

            if (typeof(T) == typeof(double?))
                return values.OfType<double?>().Select(v => (RedisValue)v).ToArray();

            if (typeof(T) == typeof(long))
                return values.OfType<long>().Select(v => (RedisValue)v).ToArray();

            if (typeof(T) == typeof(long?))
                return values.OfType<long?>().Select(v => (RedisValue)v).ToArray();

            throw new NotSupportedException(typeof(T).FullName);
        }
    }
}
