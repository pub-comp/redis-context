using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using StackExchange.Redis;
using NLog;

namespace Payoneer.Infra.Repo
{
    public class RedisContext : IRedisContext
    {
        private readonly CommandFlags commandFlags;
        private readonly IConnectionMultiplexer connection;
        private readonly int databaseNumber;
        private readonly List<string> hosts;
        private readonly ILogger log;
        private readonly int defaultRetries = 5;

        public RedisContext(string host = "localhost", int port = 6379, string password = null, int db = 0)
            : this(
                ToConnectionString(host, port, password, db),
                commandFlags: CommandFlags.None, defaultRetries: 5)
        {
        }

        public RedisContext(string host, int port, string password, int db,
            CommandFlags commandFlags, int defaultRetries)
            : this(
                  ToConnectionString(host, port, password, db),
                  commandFlags, defaultRetries)
        {
        }

        public RedisContext(string connectionString)
            : this(connectionString, commandFlags: CommandFlags.None, defaultRetries: 5)
        {
        }

        public RedisContext(string connectionString,
            CommandFlags commandFlags, int defaultRetries)
        {
            this.log = LogManager.GetLogger(typeof(RedisContext).FullName);
            var connectionOptions = ToConnectionOptions(connectionString);
            this.databaseNumber = connectionOptions.RedisConfigurationOptions.DefaultDatabase ?? 0;
            this.hosts = connectionOptions.Hosts;
            this.connection = ConnectionMultiplexer.Connect(connectionOptions.RedisConfigurationOptions);
            this.commandFlags = commandFlags;
        }

        #region Properties

        protected IConnectionMultiplexer Connection => connection;

        protected int DatabaseNumber => databaseNumber;

        protected IDatabase Database => this.connection.GetDatabase(db: this.databaseNumber);

        #endregion

        #region ConnectionString

        protected class RedisConnectionOptions
        {
            public ConfigurationOptions RedisConfigurationOptions { get; set; }
            public List<string> Hosts { get; set; }
        }

        private static RedisConnectionOptions ToConnectionOptions(string connectionString)
        {
            const string prefix = @"redis://";
            if (string.IsNullOrEmpty(connectionString))
                connectionString = @"redis://localhost:6379";

            var queryIndex = connectionString.IndexOf('?');

            string hosts = connectionString, queryString = null;

            if (queryIndex >= 0)
            {
                queryString = connectionString.Substring(queryIndex);
                hosts = connectionString.Substring(0, queryIndex);
            }

            if (hosts.ToLowerInvariant().StartsWith(prefix))
                hosts = hosts.Substring(prefix.Length);

            string userInfo = null;

            var atIndex = hosts.IndexOf('@');
            if (atIndex >= 0)
            {
                userInfo = hosts.Substring(0, atIndex);
                if (userInfo.Length == 0)
                    userInfo = null;

                hosts = hosts.Substring(atIndex + 1);
            }

            var hostNames = hosts.Split(new[] { ',' }, StringSplitOptions.RemoveEmptyEntries);
            if (hostNames.Length == 0)
                hostNames = new[] { "localhost" };

            var arguments = !string.IsNullOrEmpty(queryString)
                ? ParseQuery(queryString)
                : new Dictionary<string, string>();

            if (!string.IsNullOrEmpty(userInfo))
                arguments[nameof(ConfigurationOptions.Password)] = userInfo;

            var database = GetValue(arguments, nameof(ConfigurationOptions.DefaultDatabase), 0);

            var cfg = new ConfigurationOptions
            {
                AbortOnConnectFail = GetValue(arguments, nameof(ConfigurationOptions.AbortOnConnectFail), true),
                AllowAdmin = GetValue(arguments, nameof(ConfigurationOptions.AllowAdmin), false),
                ConnectRetry = GetValue(arguments, nameof(ConfigurationOptions.ConnectRetry), 0),
                ConnectTimeout = GetValue(arguments, nameof(ConfigurationOptions.ConnectTimeout), 5000),
                ClientName = GetValue(arguments, nameof(ConfigurationOptions.ClientName), null),
                DefaultDatabase = database,
                KeepAlive = GetValue(arguments, nameof(ConfigurationOptions.KeepAlive), -1),
                Password = GetValue(arguments, nameof(ConfigurationOptions.Password), null),
                ResolveDns = GetValue(arguments, nameof(ConfigurationOptions.ResolveDns), false),
                SyncTimeout = GetValue(arguments, nameof(ConfigurationOptions.SyncTimeout), 1),
                ServiceName = GetValue(arguments, nameof(ConfigurationOptions.ServiceName), null),
                WriteBuffer = GetValue(arguments, nameof(ConfigurationOptions.WriteBuffer), 4096),
                Ssl = GetValue(arguments, nameof(ConfigurationOptions.Ssl), false),
                SslHost = GetValue(arguments, nameof(ConfigurationOptions.SslHost), null),
                ConfigurationChannel = GetValue(arguments, nameof(ConfigurationOptions.ConfigurationChannel), null),
                TieBreaker = GetValue(arguments, nameof(ConfigurationOptions.TieBreaker), null),
            };
            cfg.ResponseTimeout = GetValue(arguments, nameof(ConfigurationOptions.ResponseTimeout), cfg.SyncTimeout);

            var endPoints = cfg.EndPoints;
            foreach (var hostName in hostNames)
                endPoints.Add(hostName);

            return new RedisConnectionOptions
            {
                Hosts = hostNames.ToList(),
                RedisConfigurationOptions = cfg
            };
        }

        private static string ToConnectionString(string host = "localhost", int port = 6379, string password = null, int db = 0)
        {
            var userInfo = !string.IsNullOrEmpty(password) ? password + '@' : string.Empty;
            return $"{userInfo}{host}:{port}?defaultdatabase={db}";
        }

        private static string GetValue(Dictionary<string, string> arguments, string argumentName, string defaultValue)
        {
            if (arguments.TryGetValue(argumentName.ToLowerInvariant(), out string value))
                return value;
            return defaultValue;
        }

        private static int GetValue(Dictionary<string, string> arguments, string argumentName, int defaultValue)
        {
            if (arguments.TryGetValue(argumentName.ToLowerInvariant(), out string s)
                && int.TryParse(s, out int value))
            {
                return value;
            }

            return defaultValue;
        }

        private static bool GetValue(Dictionary<string, string> arguments, string argumentName, bool defaultValue)
        {
            if (arguments.TryGetValue(argumentName.ToLowerInvariant(), out string s)
                && bool.TryParse(s, out bool value))
            {
                return value;
            }

            return defaultValue;
        }

        private static Dictionary<string, string> ParseQuery(string uriQuery)
        {
            var arguments = uriQuery
                .Substring(1) // Remove '?'
                .Split('&')
                .Select(q =>
                {
                    var kvArray = q.Split('=');
                    if (kvArray.Length == 2)
                        return new KeyValuePair<string, string>(kvArray[0], kvArray[1]);
                    return (KeyValuePair<string, string>?)null;
                })
                .Where(kv => kv.HasValue)
                .GroupBy(kv => kv.Value.Key)
                .ToDictionary(g => g.Key.ToLowerInvariant(), g => g.First()?.Value);

            return arguments;
        }

        #endregion

        #region ToTypeX

        protected static bool ToString(RedisValue redisValue, out string value)
        {
            value = redisValue.HasValue ? redisValue.ToString() : default(string);
            return redisValue.HasValue;
        }

        protected static bool ToNullableInt(RedisValue redisValue, out int? value)
        {
            value = null;

            if (!redisValue.HasValue)
                return false;

            if (redisValue.IsNull)
                return true;

            int result;
            if (redisValue.TryParse(out result))
            {
                value = result;
                return true;
            }

            return false;
        }

        protected static bool ToInt(RedisValue redisValue, out int value)
        {
            value = default(int);

            if (!redisValue.HasValue)
                return false;

            if (redisValue.IsNull)
                return true;

            return redisValue.TryParse(out value);
        }

        protected static bool ToNullableLong(RedisValue redisValue, out long? value)
        {
            value = null;

            if (!redisValue.HasValue)
                return false;

            if (redisValue.IsNull)
                return true;

            long result;
            if (redisValue.TryParse(out result))
            {
                value = result;
                return true;
            }

            return false;
        }

        protected static bool ToLong(RedisValue redisValue, out long value)
        {
            value = default(long);

            if (!redisValue.HasValue)
                return false;

            if (redisValue.IsNull)
                return true;

            return redisValue.TryParse(out value);
        }

        protected static bool ToNullableDouble(RedisValue redisValue, out double? value)
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

        protected static bool ToDouble(RedisValue redisValue, out double value)
        {
            value = default(double);

            if (!redisValue.HasValue)
                return false;

            if (redisValue.IsNull)
                return true;

            return redisValue.TryParse(out value);
        }

        protected static bool ToNullableBool(RedisValue redisValue, out bool? value)
        {
            var result = ToNullableInt(redisValue, out int? intValue);
            value = intValue.HasValue ? intValue.Value != 0 : (bool?)null;
            return result;
        }

        protected static bool ToBool(RedisValue redisValue, out bool value)
        {
            var result = ToInt(redisValue, out int intValue);
            value = intValue != 0;
            return result;
        }

        #endregion

        #region Retry

        private const int NoRetries = 1;
        private const int RetryDelay = 500;

        protected static TResult Retry<TResult>(Func<TResult> func, int maxAttempts)
        {
            TResult result = default(TResult);

            for (int attempts = 0; attempts < maxAttempts; attempts++)
            {
                try
                {
                    result = func();
                    break;
                }
                catch (Exception ex)
                {
                    if (!TestExceptionForRetry(ex))
                    {
                        throw;
                    }

                    if (attempts < maxAttempts - 1)
                    {
                        LogManager.GetLogger(typeof(RedisContext).FullName).Warn(
                            ex, $"Retrying, attempt #{attempts}");
                        Thread.Sleep(RetryDelay * (attempts + 1));
                    }
                    else
                    {
                        LogManager.GetLogger(typeof(RedisContext).FullName).Error(
                            ex, $"Failed, attempt #{attempts}");
                        throw;
                    }
                }
            }

            return result;
        }

        protected static void Retry(Action action, int maxAttempts)
        {
            for (int attempts = 0; attempts < maxAttempts; attempts++)
            {
                try
                {
                    action();
                    break;
                }
                catch (Exception ex)
                {
                    if (!TestExceptionForRetry(ex))
                    {
                        throw;
                    }

                    if (attempts < maxAttempts - 1)
                    {
                        LogManager.GetLogger(typeof(RedisContext).FullName).Warn(
                            ex, $"Retrying, attempt #{attempts}");
                        Thread.Sleep(RetryDelay * (attempts + 1));
                    }
                    else
                    {
                        LogManager.GetLogger(typeof(RedisContext).FullName).Error(
                            ex, $"Failed, attempt #{attempts}");
                        throw;
                    }
                }
            }
        }

        protected static bool TestExceptionForRetry(Exception ex)
        {
            return (ex is TimeoutException || ex is RedisException
                || ex is AggregateException agrEx
                    && !agrEx.InnerExceptions.Any(ex2 => ex2 is OutOfMemoryException || ex2 is StackOverflowException)
                    && agrEx.InnerExceptions.Any(ex2 => ex2 is TimeoutException || ex2 is RedisException)
                    );
        }

        #endregion

        #region TryGet

        public bool TryGet(string key, out string value)
        {
            var redisValue = Retry(() =>
                this.Database.StringGet(key, flags: commandFlags), defaultRetries);
            return ToString(redisValue, out value);
        }

        public bool TryGet(string key, out int? value)
        {
            var redisValue = Retry(() =>
                this.Database.StringGet(key, flags: commandFlags), defaultRetries);
            return ToNullableInt(redisValue, out value);
        }

        public bool TryGet(string key, out int value)
        {
            var redisValue = Retry(() =>
                this.Database.StringGet(key, flags: commandFlags), defaultRetries);
            return ToInt(redisValue, out value);
        }

        public bool TryGet(string key, out long? value)
        {
            var redisValue = Retry(() =>
                this.Database.StringGet(key, flags: commandFlags), defaultRetries);
            return ToNullableLong(redisValue, out value);
        }

        public bool TryGet(string key, out long value)
        {
            var redisValue = Retry(() =>
                this.Database.StringGet(key, flags: commandFlags), defaultRetries);
            return ToLong(redisValue, out value);
        }

        public bool TryGet(string key, out double? value)
        {
            var redisValue = Retry(() =>
                this.Database.StringGet(key, flags: commandFlags), defaultRetries);
            return ToNullableDouble(redisValue, out value);
        }

        public bool TryGet(string key, out double value)
        {
            var redisValue = Retry(() =>
                this.Database.StringGet(key, flags: commandFlags), defaultRetries);
            return ToDouble(redisValue, out value);
        }

        public bool TryGet(string key, out bool? value)
        {
            var redisValue = Retry(() =>
                this.Database.StringGet(key, flags: commandFlags), defaultRetries);
            return ToNullableBool(redisValue, out value);
        }

        public bool TryGet(string key, out bool value)
        {
            var redisValue = Retry(() =>
                this.Database.StringGet(key, flags: commandFlags), defaultRetries);
            return ToBool(redisValue, out value);
        }

        #endregion

        #region Set

        public void Set(string key, string value, TimeSpan? expiry = null)
        {
            Retry(() =>
                this.Database.StringSet(key, value, expiry: expiry, flags: commandFlags), defaultRetries);
        }

        public void Set(string key, bool value, TimeSpan? expiry = null)
        {
            var intValue = value ? -1 : 0;
            Retry(() =>
                this.Database.StringSet(key, intValue, expiry: expiry, flags: commandFlags), defaultRetries);
        }

        public void Set(string key, bool? value, TimeSpan? expiry = null)
        {
            var intValue = value.HasValue ? (value.Value ? -1 : 0) : (int?)null;
            Retry(() =>
                this.Database.StringSet(key, intValue, expiry: expiry, flags: commandFlags), defaultRetries);
        }

        public void Set(string key, double value, TimeSpan? expiry = null)
        {
            Retry(() =>
                this.Database.StringSet(key, value, expiry: expiry, flags: commandFlags), defaultRetries);
        }

        public void Set(string key, double? value, TimeSpan? expiry = null)
        {
            Retry(() =>
                this.Database.StringSet(key, value, expiry: expiry, flags: commandFlags), defaultRetries);
        }

        public void Set(string key, int value, TimeSpan? expiry = null)
        {
            Retry(() =>
                this.Database.StringSet(key, value, expiry: expiry, flags: commandFlags), defaultRetries);
        }

        public void Set(string key, int? value, TimeSpan? expiry = null)
        {
            Retry(() =>
                this.Database.StringSet(key, value, expiry: expiry, flags: commandFlags), defaultRetries);
        }

        public void Set(string key, long value, TimeSpan? expiry = null)
        {
            Retry(() =>
                this.Database.StringSet(key, value, expiry: expiry, flags: commandFlags), defaultRetries);
        }

        public void Set(string key, long? value, TimeSpan? expiry = null)
        {
            Retry(() =>
                this.Database.StringSet(key, value, expiry: expiry, flags: commandFlags), defaultRetries);
        }

        #endregion

        #region Delete

        public void Delete(string key)
        {
            Retry(() =>
                this.Database.KeyDelete(key, flags: commandFlags), defaultRetries);
        }

        public void Delete(params string[] keys)
        {
            Retry(() =>
                this.Database.KeyDelete(keys.Select(k => (RedisKey)k).ToArray(), flags: commandFlags), defaultRetries);
        }

        #endregion

        #region SetOrAppend

        public void SetOrAppend(string key, string value)
        {
            Retry(() => this.Database.StringAppend(key, value, flags: commandFlags), NoRetries);
        }

        #endregion

        #region Increment

        public long Increment(string key, long value)
        {
            return Retry(() => this.Database.StringIncrement(key, value, flags: commandFlags), NoRetries);
        }

        public double Increment(string key, double value)
        {
            return Retry(() => this.Database.StringIncrement(key, value, flags: commandFlags), NoRetries);
        }

        #endregion

        #region Decrement

        public long Decrement(string key, long value)
        {
            return Retry(() => this.Database.StringDecrement(key, value, flags: commandFlags), NoRetries);
        }

        public double Decrement(string key, double value)
        {
            return Retry(() => this.Database.StringDecrement(key, value, flags: commandFlags), NoRetries);
        }

        #endregion

        #region AtomicExchange
        
        public string AtomicExchange(string key, string value)
        {
            var redisValue = Retry(() =>
                this.Database.StringGetSet(key, (RedisValue)value, flags: commandFlags), NoRetries);
            return ToString(redisValue, out string previousValue) ? previousValue : default(string);
        }

        public int? AtomicExchange(string key, int? value)
        {
            var redisValue = Retry(() =>
                this.Database.StringGetSet(key, (RedisValue)value, flags: commandFlags), NoRetries);
            return ToNullableInt(redisValue, out int? previousValue) ? previousValue : default(int?);
        }

        public int AtomicExchange(string key, int value)
        {
            var redisValue = Retry(() =>
                this.Database.StringGetSet(key, (RedisValue)value, flags: commandFlags), NoRetries);
            return ToInt(redisValue, out int previousValue) ? previousValue : default(int);
        }

        public long? AtomicExchange(string key, long? value)
        {
            var redisValue = Retry(() =>
                this.Database.StringGetSet(key, (RedisValue)value, flags: commandFlags), NoRetries);
            return ToNullableLong(redisValue, out long? previousValue) ? previousValue : default(long?);
        }

        public long AtomicExchange(string key, long value)
        {
            var redisValue = Retry(() =>
                this.Database.StringGetSet(key, (RedisValue)value, flags: commandFlags), NoRetries);
            return ToLong(redisValue, out long previousValue) ? previousValue : default(long);
        }

        public double? AtomicExchange(string key, double? value)
        {
            var redisValue = Retry(() =>
                this.Database.StringGetSet(key, (RedisValue)value, flags: commandFlags), NoRetries);
            return ToNullableDouble(redisValue, out double? previousValue) ? previousValue : default(double?);
        }

        public double AtomicExchange(string key, double value)
        {
            var redisValue = Retry(() =>
                this.Database.StringGetSet(key, (RedisValue)value, flags: commandFlags), NoRetries);
            return ToDouble(redisValue, out double previousValue) ? previousValue : default(double);
        }

        public bool? AtomicExchange(string key, bool? value)
        {
            var redisValue = Retry(() =>
                this.Database.StringGetSet(key, (RedisValue)value, flags: commandFlags), NoRetries);
            return ToNullableBool(redisValue, out bool? previousValue) ? previousValue : default(bool?);
        }

        public bool AtomicExchange(string key, bool value)
        {
            var redisValue = Retry(() =>
                this.Database.StringGetSet(key, (RedisValue)value, flags: commandFlags), NoRetries);
            return ToBool(redisValue, out bool previousValue) && previousValue;
        }

        #endregion

        #region TimeToLive

        public TimeSpan? GetTimeToLive(string key)
        {
            return Retry(() => this.Database.KeyTimeToLive(key, flags: commandFlags), defaultRetries);
        }

        public void SetTimeToLive(string key, TimeSpan? expiry)
        {
            var redisValue = Retry(() => this.Database.StringGet(key, flags: commandFlags), defaultRetries);

            // If key in DB
            // If expiry was requested, then update
            // If no expiry was requested, then update only if there is currently an expiry set
            if (redisValue != default(RedisValue)
                && (expiry.HasValue || GetTimeToLive(key).HasValue))
            {
                Retry(() => this.Database.KeyExpire(key, expiry), defaultRetries);
            }
        }

        #endregion

        #region GetKeys

        /// <summary>
        /// Do not use in production!
        /// </summary>
        /// <param name="pattern"></param>
        /// <returns></returns>
        public IEnumerable<string> GetKeys(string pattern = null)
        {
            return this.connection.GetServer(hosts.First())
                .Keys(this.databaseNumber, pattern).ToList()
                .Select(rk => rk.ToString()).ToList();
        }

        #endregion
    }
}
