using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using NLog;
using StackExchange.Redis;

namespace PubComp.RedisRepo
{
    public class RedisContext : IRedisContext
    {
        private const int DefaultTotalConnections = 2;

        private readonly string contextNamespace;

        private readonly int databaseNumber;
        private readonly List<string> hosts;
        private readonly ILogger log;
        private readonly int defaultRetries = 5;

        private IConnectionMultiplexer[] connections;

        internal readonly CommandFlags commandFlags;
        private readonly int totalConnections;

        private ConcurrentDictionary<string, LoadedLuaScript> loadedScripts = new ConcurrentDictionary<string, LoadedLuaScript>();

        public RedisContext(
            string contextNamespace,
            string host = "localhost", int port = 6379, string password = null, int db = 0)
            : this(
                contextNamespace,
                ToConnectionString(host, port, password, db),
                commandFlags: CommandFlags.None, defaultRetries: 5, totalConnections: DefaultTotalConnections)
        {
        }

        public RedisContext(
            string contextNamespace,
            string host, int port, string password, int db,
            CommandFlags commandFlags, int defaultRetries)
            : this(
                contextNamespace,
                ToConnectionString(host, port, password, db),
                commandFlags, defaultRetries, totalConnections: DefaultTotalConnections)
        {
        }

        public RedisContext(
            string contextNamespace,
            string host, int port, string password, int db,
            CommandFlags commandFlags, int defaultRetries, int totalConnections)
             : this(
                contextNamespace,
                ToConnectionString(host, port, password, db),
                commandFlags, defaultRetries, totalConnections)
        {
        }

        public RedisContext(string contextNamespace, string connectionString)
            : this(
                contextNamespace, connectionString, commandFlags: CommandFlags.None, defaultRetries: 5, totalConnections: DefaultTotalConnections)
        {
        }

        public RedisContext(
            string contextNamespace, string connectionString,
            CommandFlags commandFlags, int defaultRetries,
            int totalConnections)
        {
            this.log = LogManager.GetLogger(typeof(RedisContext).FullName);
            this.contextNamespace = contextNamespace;
            var connectionOptions = ToConnectionOptions(connectionString);
            this.databaseNumber = connectionOptions.RedisConfigurationOptions.DefaultDatabase ?? 0;
            this.hosts = connectionOptions.Hosts;
            this.commandFlags = commandFlags;
            this.totalConnections = totalConnections;
            InitConnections(connectionOptions);
        }

        private void InitConnections(RedisConnectionOptions options)
        {
            if (this.totalConnections < 1)
            {
                throw new ArgumentException("total connections must be greater than 0", nameof(this.totalConnections));
            }

            this.connections = new IConnectionMultiplexer[this.totalConnections];
            for (int i = 0; i < totalConnections; i++)
            {
                this.connections[i] = ConnectionMultiplexer.Connect(options.RedisConfigurationOptions);
                this.connections[i].PreserveAsyncOrder = false;
            }
        }

        #region Properties

        private int connectionIndex = 0;
        protected IConnectionMultiplexer Connection
        {
            get
            {
                var result = this.connections[connectionIndex];
                connectionIndex = (connectionIndex + 1) % totalConnections;
                return result;
            }
        }

        protected int DatabaseNumber => databaseNumber;

        internal protected virtual IDatabase Database => this.Connection.GetDatabase(db: this.databaseNumber);

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
                arguments[nameof(ConfigurationOptions.Password).ToLowerInvariant()] = userInfo;

            var database = GetValue(arguments, nameof(ConfigurationOptions.DefaultDatabase), 0);

            var cfg = new ConfigurationOptions
            {
                AbortOnConnectFail = GetValue(arguments, nameof(ConfigurationOptions.AbortOnConnectFail), false),
                AllowAdmin = GetValue(arguments, nameof(ConfigurationOptions.AllowAdmin), false),
                ConnectRetry = GetValue(arguments, nameof(ConfigurationOptions.ConnectRetry), 2),
                ConnectTimeout = GetValue(arguments, nameof(ConfigurationOptions.ConnectTimeout), 5000),
                ClientName = GetValue(arguments, nameof(ConfigurationOptions.ClientName), null),
                DefaultDatabase = database,
                KeepAlive = GetValue(arguments, nameof(ConfigurationOptions.KeepAlive), -1),
                Password = GetValue(arguments, nameof(ConfigurationOptions.Password), null),
                ResolveDns = GetValue(arguments, nameof(ConfigurationOptions.ResolveDns), false),
                SyncTimeout = GetValue(arguments, nameof(ConfigurationOptions.SyncTimeout), 1000),
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

        #region retries
        public static TResult Retry<TResult>(Func<TResult> func, int maxAttempts)
        {
            return RetryUtil.Retry(func, maxAttempts);
        }
        public static void Retry(Action action, int maxAttempts)
        {
            RetryUtil.Retry(action, maxAttempts);
        }
        #endregion

        public virtual string Key(string key)
        {
            return !string.IsNullOrEmpty(contextNamespace)
                ? string.Concat("ns=", contextNamespace, ":k=", key)
                : key;
        }

        #region TryGet

        public bool TryGet(string key, out string value)
        {
            var redisValue = Retry(() =>
                this.Database.StringGet(Key(key), flags: commandFlags), defaultRetries);
            return redisValue.ToDotNetString(out value);
        }

        public bool TryGet(string key, out int? value)
        {
            var redisValue = Retry(() =>
                this.Database.StringGet(Key(key), flags: commandFlags), defaultRetries);
            return redisValue.ToNullableInt(out value);
        }

        public bool TryGet(string key, out int value)
        {
            var redisValue = Retry(() =>
                this.Database.StringGet(Key(key), flags: commandFlags), defaultRetries);
            return redisValue.ToInt(out value);
        }

        public bool TryGet(string key, out long? value)
        {
            var redisValue = Retry(() =>
                this.Database.StringGet(Key(key), flags: commandFlags), defaultRetries);
            return redisValue.ToNullableLong(out value);
        }

        public bool TryGet(string key, out long value)
        {
            var redisValue = Retry(() =>
                this.Database.StringGet(Key(key), flags: commandFlags), defaultRetries);
            return redisValue.ToLong(out value);
        }

        public bool TryGet(string key, out double? value)
        {
            var redisValue = Retry(() =>
                this.Database.StringGet(Key(key), flags: commandFlags), defaultRetries);
            return redisValue.ToNullableDouble(out value);
        }

        public bool TryGet(string key, out double value)
        {
            var redisValue = Retry(() =>
                this.Database.StringGet(Key(key), flags: commandFlags), defaultRetries);
            return redisValue.ToDouble(out value);
        }

        public bool TryGet(string key, out bool? value)
        {
            var redisValue = Retry(() =>
                this.Database.StringGet(Key(key), flags: commandFlags), defaultRetries);
            return redisValue.ToNullableBool(out value);
        }

        public bool TryGet(string key, out bool value)
        {
            var redisValue = Retry(() =>
                this.Database.StringGet(Key(key), flags: commandFlags), defaultRetries);

            return redisValue.ToBool(out value);
        }

        #endregion

        #region Set (Set value)

        public void Set(string key, string value, TimeSpan? expiry = null)
        {
            Retry(() =>
                this.Database.StringSet(Key(key), value, expiry: expiry, flags: commandFlags), defaultRetries);
        }

        public bool Set(string key, string value, Enums.When when, TimeSpan? expiry = null)
        {
            return Retry(() =>
                this.Database.StringSet(Key(key), value, expiry: expiry, when: when.ToSE(), flags: commandFlags), defaultRetries);
        }

        public void Set(string key, bool value, TimeSpan? expiry = null)
        {
            var intValue = value ? -1 : 0;
            Retry(() =>
                this.Database.StringSet(Key(key), intValue, expiry: expiry, flags: commandFlags), defaultRetries);
        }

        public void Set(string key, bool? value, TimeSpan? expiry = null)
        {
            var intValue = value.HasValue ? (value.Value ? -1 : 0) : (int?)null;
            Retry(() =>
                this.Database.StringSet(Key(key), intValue, expiry: expiry, flags: commandFlags), defaultRetries);
        }

        public void Set(string key, double value, TimeSpan? expiry = null)
        {
            Retry(() =>
                this.Database.StringSet(Key(key), value, expiry: expiry, flags: commandFlags), defaultRetries);
        }

        public void Set(string key, double? value, TimeSpan? expiry = null)
        {
            Retry(() =>
                this.Database.StringSet(Key(key), value, expiry: expiry, flags: commandFlags), defaultRetries);
        }

        public void Set(string key, int value, TimeSpan? expiry = null)
        {
            Retry(() =>
                this.Database.StringSet(Key(key), value, expiry: expiry, flags: commandFlags), defaultRetries);
        }

        public void Set(string key, int? value, TimeSpan? expiry = null)
        {
            Retry(() =>
                this.Database.StringSet(Key(key), value, expiry: expiry, flags: commandFlags), defaultRetries);
        }

        public void Set(string key, long value, TimeSpan? expiry = null)
        {
            Retry(() =>
                this.Database.StringSet(Key(key), value, expiry: expiry, flags: commandFlags), defaultRetries);
        }

        public void Set(string key, long? value, TimeSpan? expiry = null)
        {
            Retry(() =>
                this.Database.StringSet(Key(key), value, expiry: expiry, flags: commandFlags), defaultRetries);
        }

        #endregion

        #region Delete

        public void Delete(string key)
        {
            Retry(() =>
                this.Database.KeyDelete(Key(key), flags: commandFlags), defaultRetries);
        }

        public void Delete(params string[] keys)
        {
            Retry(() =>
                this.Database.KeyDelete(keys.Select(k => (RedisKey)Key(k)).ToArray(), flags: commandFlags), defaultRetries);
        }

        #endregion

        #region SetOrAppend

        public void SetOrAppend(string key, string value)
        {
            Retry(() => this.Database.StringAppend(Key(key), value, flags: commandFlags), RetryUtil.NoRetries);
        }

        #endregion

        #region Redis Lists

        /// <summary>
        /// Adds <paramref name="value"/> to the end of a list that is at <paramref name="key"/>.
        /// If the list doesn't exist then it is created.
        /// </summary>
        /// <returns>The length of the list after the addition</returns>
        public long AddToList(string key, string value)
        {
            return AddRangeToList(key, new[] { value });
        }

        /// <summary>
        /// Adds <paramref name="values"/> to the end of a list that is at <paramref name="key"/>.
        /// If the list doesn't exist then it is created.
        /// </summary>
        /// <returns>The length of the list after the addition</returns>
        public long AddRangeToList(string key, string[] values)
        {
            var results = Retry(() => this.Database.ListRightPush(Key(key), values.ToRedisValueArray(), flags: commandFlags), defaultRetries);
            return results;
        }

        /// <summary>
        /// Returns the list that is at <paramref name="key"/>.
        /// If <paramref name="start"/> and <paramref name="stop"/> are not given then the whole list will be returned.
        /// Else, a sub-list is returned that starts at the index <paramref name="start"/> and stops at the index <paramref name="stop"/>.
        /// Please note that the list is zero-based indexed (so 0 is is the first element).
        /// If <paramref name="start"/> or <paramref name="stop"/> is negative then it means it's counted from the end of the list (-1 is the last element, -2 is the element before the last element and so on).
        /// If the index is out-of-bounds then instead of throwing an exception the index is initialized to the nearest boundary (start or end of the list), and only then the operation will be done.
        /// </summary>
        public string[] GetList(string key, long start = 0, long stop = -1)
        {
            var results = Retry(() => this.Database.ListRange(Key(key), start, stop, flags: commandFlags), defaultRetries);
            return results.ToStringArray();
        }

        #endregion

        #region Redis Sets

        public bool SetAdd<T>(string key, T value)
        {
            var redisValue = value.ToRedis();

            var result = Retry(() =>
                this.Database.SetAdd(
                    Key(key), redisValue, commandFlags), defaultRetries);

            return result;
        }

        public long SetAdd<T>(string key, T[] values)
        {
            var redisValues = values?.Select(val => val.ToRedis()).ToArray();

            var result = Retry(() =>
                this.Database.SetAdd(
                    Key(key), redisValues, commandFlags), defaultRetries);

            return result;
        }

        public bool SetRemove<T>(string key, T value)
        {
            var redisValue = value.ToRedis();

            var result = Retry(() =>
                this.Database.SetRemove(
                    Key(key), redisValue, commandFlags), defaultRetries);

            return result;
        }

        public long SetRemove<T>(string key, T[] values)
        {
            var redisValues = values?.Select(val => val.ToRedis()).ToArray();

            var result = Retry(() =>
                this.Database.SetRemove(
                    Key(key), redisValues, commandFlags), defaultRetries);

            return result;
        }

        public long SetLength(string key)
        {
            var result = Retry(() =>
                this.Database.SetLength(Key(key), commandFlags), defaultRetries);

            return result;
        }

        public void AddToSet(string key, string[] values)
        {
            Retry(() => this.Database.SetAdd(Key(key), values.ToRedisValueArray(), flags: commandFlags), defaultRetries);
        }

        public long CountSetMembers(string key)
        {
            return Retry(() => this.Database.SetLength(Key(key), flags: commandFlags), defaultRetries);
        }

        public string[] GetSetMembers(string key)
        {
            var results = Retry(() => this.Database.SetMembers(Key(key), flags: commandFlags), defaultRetries);
            return results.ToStringArray();
        }

        /// <summary>
        /// Get the diff between the set at index 0 of <paramref name="keys"/> and all other sets in <paramref name="keys"/>
        /// </summary>
        public string[] GetSetsDifference(string[] keys)
        {
            return OperateOnSet(
                SetOperation.Difference,
                keys);
        }

        /// <summary>
        /// Union sets at keys <paramref name="setKeys"/>
        /// </summary>
        public string[] UnionSets(string[] keys)
        {
            return OperateOnSet(SetOperation.Union, keys);
        }

        /// <summary>
        /// Intersect sets at keys <paramref name="keys"/>
        /// </summary>
        public string[] IntersectSets(string[] keys)
        {
            return OperateOnSet(SetOperation.Intersect, keys);
        }

        /// <summary>
        /// Get the diff between the set at index 0 of <paramref name="keys"/> and all other sets in <paramref name="keys"/>
        /// store the result at <param name="destinationKey"></param>
        /// </summary>
        public void StoreSetsDifference(string destinationKey, string[] keys)
        {
            OperateOnSetAndStore(
                SetOperation.Difference,
                destinationKey,
                keys);
        }

        /// <summary>
        /// Union sets at keys <paramref name="keys"/>
        /// store the result at <param name="destinationKey"></param>
        /// </summary>
        public void UnionSetsAndStore(string destinationKey, string[] keys)
        {
            OperateOnSetAndStore(SetOperation.Union, destinationKey, keys);
        }

        /// <summary>
        /// Intersect sets at keys <paramref name="keys"/>
        /// store the result at <param name="destinationKey"></param>
        /// </summary>
        public void IntersectSetsAndStore(string destinationKey, string[] keys)
        {
            OperateOnSetAndStore(SetOperation.Intersect, destinationKey, keys);
        }

        public bool SetContains(string key, string member)
        {
            return Retry(() => this.Database.SetContains(Key(key), member, commandFlags), defaultRetries);
        }

        #region set helpers
        private string[] OperateOnSet(SetOperation op, string[] keys)
        {
            if (keys == null || keys.Length == 0) return null;

            var redisKeys = keys.Select(c => (RedisKey)Key(c)).ToArray();
            var results =
                 Retry(() => this.Database.SetCombine(op, redisKeys, commandFlags), defaultRetries);

            return results?.ToStringArray();
        }

        private void OperateOnSetAndStore(SetOperation op, string destinationKey, string[] keys)
        {
            if (keys == null || keys.Length == 0)
                return;

            var redisKeys = keys.Select(c => (RedisKey)Key(c)).ToArray();

            Retry(() => this.Database.SetCombineAndStore(op, Key(destinationKey), redisKeys, commandFlags), defaultRetries);

        }
        #endregion

        #endregion

        #region Increment

        public long Increment(string key, long value)
        {
            return Retry(() => this.Database.StringIncrement(Key(key), value, flags: commandFlags), RetryUtil.NoRetries);
        }

        public double Increment(string key, double value)
        {
            return Retry(() => this.Database.StringIncrement(Key(key), value, flags: commandFlags), RetryUtil.NoRetries);
        }

        #endregion

        #region Decrement

        public long Decrement(string key, long value)
        {
            return Retry(() => this.Database.StringDecrement(Key(key), value, flags: commandFlags), RetryUtil.NoRetries);
        }

        public double Decrement(string key, double value)
        {
            return Retry(() => this.Database.StringDecrement(Key(key), value, flags: commandFlags), RetryUtil.NoRetries);
        }

        #endregion

        #region AtomicExchange

        public string AtomicExchange(string key, string value)
        {
            var redisValue = Retry(() =>
                this.Database.StringGetSet(Key(key), (RedisValue)value, flags: commandFlags), RetryUtil.NoRetries);
            return redisValue.ToDotNetString(out string previousValue) ? previousValue : default(string);
        }

        public int? AtomicExchange(string key, int? value)
        {
            var redisValue = Retry(() =>
                this.Database.StringGetSet(Key(key), (RedisValue)value, flags: commandFlags), RetryUtil.NoRetries);
            return redisValue.ToNullableInt(out int? previousValue) ? previousValue : default(int?);
        }

        public int AtomicExchange(string key, int value)
        {
            var redisValue = Retry(() =>
                this.Database.StringGetSet(Key(key), (RedisValue)value, flags: commandFlags), RetryUtil.NoRetries);
            return redisValue.ToInt(out int previousValue) ? previousValue : default(int);
        }

        public long? AtomicExchange(string key, long? value)
        {
            var redisValue = Retry(() =>
                this.Database.StringGetSet(Key(key), (RedisValue)value, flags: commandFlags), RetryUtil.NoRetries);
            return redisValue.ToNullableLong(out long? previousValue) ? previousValue : default(long?);
        }

        public long AtomicExchange(string key, long value)
        {
            var redisValue = Retry(() =>
                this.Database.StringGetSet(Key(key), (RedisValue)value, flags: commandFlags), RetryUtil.NoRetries);
            return redisValue.ToLong(out long previousValue) ? previousValue : default(long);
        }

        public double? AtomicExchange(string key, double? value)
        {
            var redisValue = Retry(() =>
                this.Database.StringGetSet(Key(key), (RedisValue)value, flags: commandFlags), RetryUtil.NoRetries);
            return redisValue.ToNullableDouble(out double? previousValue) ? previousValue : default(double?);
        }

        public double AtomicExchange(string key, double value)
        {
            var redisValue = Retry(() =>
                this.Database.StringGetSet(Key(key), (RedisValue)value, flags: commandFlags), RetryUtil.NoRetries);
            return redisValue.ToDouble(out double previousValue) ? previousValue : default(double);
        }

        public bool? AtomicExchange(string key, bool? value)
        {
            var redisValue = Retry(() =>
                this.Database.StringGetSet(Key(key), (RedisValue)value, flags: commandFlags), RetryUtil.NoRetries);
            return redisValue.ToNullableBool(out bool? previousValue) ? previousValue : default(bool?);
        }

        public bool AtomicExchange(string key, bool value)
        {
            var redisValue = Retry(() =>
                this.Database.StringGetSet(Key(key), (RedisValue)value, flags: commandFlags), RetryUtil.NoRetries);
            return redisValue.ToBool(out bool previousValue) && previousValue;
        }

        #endregion

        #region TimeToLive

        public TimeSpan? GetTimeToLive(string key)
        {
            return Retry(() => this.Database.KeyTimeToLive(Key(key), flags: commandFlags), defaultRetries);
        }

        public void SetTimeToLive(string key, TimeSpan? expiry)
        {
            var keyExists = Retry(() => this.Database.KeyExists(Key(key), flags: commandFlags), defaultRetries);

            // If key in DB
            // If expiry was requested, then update
            // If no expiry was requested, then update only if there is currently an expiry set
            if (keyExists && (expiry.HasValue || GetTimeToLive(Key(key)).HasValue))
            {
                Retry(() => this.Database.KeyExpire(Key(key), expiry, flags: commandFlags), defaultRetries);
            }
        }

        #endregion

        #region Distributed Lock

        /// <summary>
        /// Try get a distributed lock on object <paramref name="lockObjectName"/> for the locker name <paramref name="lockerName"/>.
        /// If the lock succeeds, it will be available for <paramref name="lockTtl"/>
        /// </summary>
        /// <param name="lockObjectName"></param>
        /// <param name="lockerName"></param>
        /// <param name="lockTtl"></param>
        /// <returns></returns>
        public bool TryGetDistributedLock(string lockObjectName, string lockerName, TimeSpan lockTtl)
        {
            var isNew = this.Set(lockObjectName, lockerName, when: Enums.When.NotExists, expiry: lockTtl);
            if (isNew) return true;

            return this.TryGet(lockObjectName, out string currentLockerName) && currentLockerName == lockerName;
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
            var keys = this.Connection.GetServer(hosts.First())
                .Keys(this.databaseNumber, Key(pattern)).ToList()
                .Select(rk => rk.ToString()).ToList();

            if (string.IsNullOrEmpty(contextNamespace))
                return keys;

            var keyPrefixLength = Key(string.Empty).Length;
            var keysWithoutPrefix = keys.Select(k => k.Substring(keyPrefixLength)).ToList();
            return keysWithoutPrefix;
        }

        #endregion

        #region Lua Scripting

        public RedisScriptKeysAndArguments CreateScriptKeyAndArguments()
        {
            return new RedisScriptKeysAndArguments(Key);
        }

        /// <summary>
        /// Run a lua script against the connected redis instance
        /// </summary>
        /// <param name="script">the script to run. Keys should be @Key1, @Key2 ... @Key10. Int arguments: @IntArg1 .. @IntArg20. String arguments: @StringArg1 .. @StringArg20</param>
        /// <param name="keysAndParameters">an instance of RedisScriptKeysAndArguments</param>
        public void RunScript(string script, RedisScriptKeysAndArguments keysAndParameters)
        {
            Retry(() => this.RunScriptInternal(script, keysAndParameters), defaultRetries);
        }

        /// <summary>
        /// Run a lua script against the connected redis instance
        /// </summary>
        /// <param name="script">the script to run. Keys should be @Key1, @Key2 ... @Key10. Int arguments: @IntArg1 .. @IntArg20. String arguments: @StringArg1 .. @StringArg20</param>
        /// <param name="keysAndParameters">an instance of RedisScriptKeysAndArguments</param>
        /// <returns>result as string</returns>
        public string RunScriptString(string script, RedisScriptKeysAndArguments keysAndParameters)
        {
            var result = Retry(() => this.RunScriptInternal(script, keysAndParameters), defaultRetries);
            return (string)result;
        }

        /// <summary>
        /// Run a lua script against the connected redis instance
        /// </summary>
        /// <param name="script">the script to run. Keys should be @Key1, @Key2 ... @Key10. Int arguments: @IntArg1 .. @IntArg20. String arguments: @StringArg1 .. @StringArg20</param>
        /// <param name="keysAndParameters">an instance of RedisScriptKeysAndArguments</param>
        /// <returns>result as int</returns>
        public int RunScriptInt(string script, RedisScriptKeysAndArguments keysAndParameters)
        {
            var result = Retry(() => this.RunScriptInternal(script, keysAndParameters), defaultRetries);

            return (int)result;
        }

        /// <summary>
        /// Run a lua script against the connected redis instance
        /// </summary>
        /// <param name="script">the script to run. Keys should be @Key1, @Key2 ... @Key10. Int arguments: @IntArg1 .. @IntArg20. String arguments: @StringArg1 .. @StringArg20</param>
        /// <param name="keysAndParameters">an instance of RedisScriptKeysAndArguments</param>
        /// <returns>result as string</returns>
        public long RunScriptLong(string script, RedisScriptKeysAndArguments keysAndParameters)
        {
            var result = Retry(() => this.RunScriptInternal(script, keysAndParameters), defaultRetries);

            return (long)result;
        }

        /// <summary>
        /// Run a lua script against the connected redis instance
        /// </summary>
        /// <param name="script">the script to run. Keys should be @Key1, @Key2 ... @Key10. Int arguments: @IntArg1 .. @IntArg20. String arguments: @StringArg1 .. @StringArg20</param>
        /// <param name="keysAndParameters">an instance of RedisScriptKeysAndArguments</param>
        /// <returns>result as double</returns>
        public double RunScriptDouble(string script, RedisScriptKeysAndArguments keysAndParameters)
        {
            var result = Retry(() => this.RunScriptInternal(script, keysAndParameters), defaultRetries);

            return (double)result;
        }

        /// <summary>
        /// Run a lua script against the connected redis instance
        /// </summary>
        /// <param name="script">the script to run. Keys should be @Key1, @Key2 ... @Key10. Int arguments: @IntArg1 .. @IntArg20. String arguments: @StringArg1 .. @StringArg20</param>
        /// <param name="keysAndParameters">an instance of RedisScriptKeysAndArguments</param>
        /// <returns>result as byte array</returns>
        public byte[] RunScriptByteArray(string script, RedisScriptKeysAndArguments keysAndParameters)
        {
            var result = Retry(() => this.RunScriptInternal(script, keysAndParameters), defaultRetries);

            return (byte[])result;
        }

        /// <summary>
        /// Run a lua script against the connected redis instance
        /// </summary>
        /// <param name="script">the script to run. Keys should be @Key1, @Key2 ... @Key10. Int arguments: @IntArg1 .. @IntArg20. String arguments: @StringArg1 .. @StringArg20</param>
        /// <param name="keysAndParameters">an instance of RedisScriptKeysAndArguments</param>
        /// <returns>result as string[]</returns>
        public string[] RunScriptStringArray(string script, RedisScriptKeysAndArguments keysAndParameters)
        {
            var result = Retry(() => this.RunScriptInternal(script, keysAndParameters), defaultRetries);

            return (string[])result;
        }

        /// <summary>
        /// Run a lua script against the connected redis instance
        /// </summary>
        /// <param name="script">the script to run. Keys should be @Key1, @Key2 ... @Key10. Int arguments: @IntArg1 .. @IntArg20. String arguments: @StringArg1 .. @StringArg20</param>
        /// <param name="keysAndParameters">an instance of RedisScriptKeysAndArguments</param>
        /// <returns></returns>
        private RedisResult RunScriptInternal(string script, RedisScriptKeysAndArguments keysAndParameters)
        {
            var conn = this.Connection;
            var db = conn.GetDatabase(this.DatabaseNumber);

            if (!this.loadedScripts.TryGetValue(script, out var loadedLuaScript))
            {
                var server = conn.GetServer(hosts.First());
                var prepared = LuaScript.Prepare(script);
                this.loadedScripts[script] = loadedLuaScript = prepared.Load(server);
            }

            try
            {
                return loadedLuaScript.Evaluate(db, keysAndParameters);
            }
            catch (RedisServerException)
            {
                // TODO: validate that the message is NOSCRIPT
                var server = conn.GetServer(hosts.First());
                var prepared = LuaScript.Prepare(script);
                this.loadedScripts[script] = loadedLuaScript = prepared.Load(server);

                // run
                return loadedLuaScript.Evaluate(db, keysAndParameters);
            }
        }

        #endregion

        public void CloseConnections()
        {
            if (this.connections == null) return;

            foreach (var conn in this.connections)
            {
                conn?.Dispose();
            }
        }

        #region Redis Sorted Sets

        #region AddToSortedSet

        public bool AddToSortedSet<T>(
            string key, T value, double score, Enums.When when = Enums.When.Always)
        {
            var result = Retry(() =>
                this.Database.SortedSetAdd(
                    Key(key), value.ToRedis(), score, when.ToSE(), commandFlags), defaultRetries);

            return result;
        }

        #endregion

        #region AddToSortedSet[]

        public long SortedSetAdd<T>(
            string key, (T value, double score)[] values,
            Enums.When when = Enums.When.Always)
        {
            var sortedSetEntries = values?.Select(val =>
                new SortedSetEntry(val.value.ToRedis(), val.score)).ToArray();

            var result = Retry(() =>
                this.Database.SortedSetAdd(
                    Key(key), sortedSetEntries, when.ToSE(), commandFlags), defaultRetries);

            return result;
        }

        #endregion

        public long SortedSetGetLength(
            string key, double min = double.NegativeInfinity,
            double max = double.PositiveInfinity, Enums.Exclude exclude = Enums.Exclude.None)
        {
            return Retry(() =>
                this.Database.SortedSetLength(
                    Key(key), min, max, exclude.ToSE(), commandFlags), defaultRetries);
        }

        #region GetRange

        public T[] SortedSetGetRangeByScore<T>(
            string key, Func<object, T> redisValueConverter,
            double start = double.NegativeInfinity, double end = double.PositiveInfinity, Enums.SortOrder order = Enums.SortOrder.Ascending)
        {
            var results = Retry(() =>
                this.Database.SortedSetRangeByScore(
                    Key(key), start, end, order: order.ToSE(), flags: commandFlags), defaultRetries);

            return results.Select(r => redisValueConverter(r)).ToArray();
        }

        public T[] SortedSetGetRangeByRank<T>(
            string key, Func<object, T> redisValueConverter,
            long start = 0, long end = -1, Enums.SortOrder order = Enums.SortOrder.Ascending)
        {
            var results = Retry(() =>
                this.Database.SortedSetRangeByRank(
                    Key(key), start, end, order.ToSE(), commandFlags), defaultRetries);

            return results.Select(r => redisValueConverter(r)).ToArray();
        }

        public List<(T value, double score)> SortedSetGetRangeByScoreWithScores<T>(
            string key, Func<object, T> redisValueConverter,
            double start = double.NegativeInfinity,
            double end = double.PositiveInfinity, Enums.SortOrder order = Enums.SortOrder.Ascending)
        {
            var results = Retry(() =>
                this.Database.SortedSetRangeByScoreWithScores(
                    Key(key), start, end, order: order.ToSE(), flags: commandFlags), defaultRetries);

            return results.Select((SortedSetEntry r) => (redisValueConverter(r.Element), r.Score)).ToList();
        }

        public List<(T value, double score)> SortedSetGetRangeByRankWithScores<T>(
            string key, Func<object, T> redisValueConverter,
            long start = 0, long end = -1, Enums.SortOrder order = Enums.SortOrder.Ascending)
        {
            var results = Retry(() =>
                this.Database.SortedSetRangeByRankWithScores(
                    Key(key), start, end, order.ToSE(), commandFlags), defaultRetries);

            return results.Select((SortedSetEntry r) => (redisValueConverter(r.Element), r.Score)).ToList();
        }

        #endregion

        #region Remove

        public bool SortedSetRemove<T>(string key, T value)
        {
            var results = Retry(() =>
                this.Database.SortedSetRemove(
                    Key(key), value.ToRedis(), commandFlags), defaultRetries);

            return results;
        }

        public long SortedSetRemove<T>(string key, T[] values)
        {
            var results = Retry(() =>
                this.Database.SortedSetRemove(
                    Key(key), values.ToRedisArray(), commandFlags), defaultRetries);

            return results;
        }

        public long SortedSetRemoveRangeByScore(
            string key, double start, double end, Enums.Exclude exclude = Enums.Exclude.None)
        {
            var results = Retry(() =>
                this.Database.SortedSetRemoveRangeByScore(
                    Key(key), start, end, exclude.ToSE(), commandFlags), defaultRetries);

            return results;
        }

        public long SortedSetRemoveRangeByRank(
            string key, long start, long end)
        {
            var results = Retry(() =>
                this.Database.SortedSetRemoveRangeByRank(
                    Key(key), start, end, commandFlags), defaultRetries);

            return results;
        }

        #endregion

        #endregion
    }
}
