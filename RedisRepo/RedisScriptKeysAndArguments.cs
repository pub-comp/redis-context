using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using StackExchange.Redis;

namespace PubComp.RedisRepo
{
    public class RedisScriptKeysAndArguments
    {
        private readonly Func<string, string> keyConverter;

        internal RedisScriptKeysAndArguments(Func<string, string> keyConverter = null)
        {
            this.keyConverter = keyConverter ?? (x => x);

        }

        #region Keys

        public RedisScriptKeysAndArguments Apply(Action<RedisScriptKeysAndArguments> action)
        {
            action(this);
            return this;
        }

        private RedisKey _key1;

        public RedisKey Key1
        {
            get => this._key1;
            set => this._key1 = this.keyConverter(value);
        }

        private RedisKey _key2;

        public RedisKey Key2
        {
            get => this._key2;
            set => this._key2 = this.keyConverter(value);
        }

        private RedisKey _key3;

        public RedisKey Key3
        {
            get => this._key3;
            set => this._key3 = this.keyConverter(value);
        }

        private RedisKey _key4;

        public RedisKey Key4
        {
            get => this._key4;
            set => this._key4 = this.keyConverter(value);
        }

        private RedisKey _key5;

        public RedisKey Key5
        {
            get => this._key5;
            set => this._key5 = this.keyConverter(value);
        }

        private RedisKey _key6;

        public RedisKey Key6
        {
            get => this._key6;
            set => this._key6 = this.keyConverter(value);
        }

        private RedisKey _key7;

        public RedisKey Key7
        {
            get => this._key7;
            set => this._key7 = this.keyConverter(value);
        }

        private RedisKey _key8;

        public RedisKey Key8
        {
            get => this._key8;
            set => this._key8 = this.keyConverter(value);
        }

        private RedisKey _key9;

        public RedisKey Key9
        {
            get => this._key9;
            set => this._key9 = this.keyConverter(value);
        }

        private RedisKey _key10;

        public RedisKey Key10
        {
            get => this._key10;
            set => this._key10 = this.keyConverter(value);
        }

        #endregion

        #region Int Arguments

        public int IntArg1 { get; set; }
        public int IntArg2 { get; set; }
        public int IntArg3 { get; set; }
        public int IntArg4 { get; set; }
        public int IntArg5 { get; set; }
        public int IntArg6 { get; set; }
        public int IntArg7 { get; set; }
        public int IntArg8 { get; set; }
        public int IntArg9 { get; set; }
        public int IntArg10 { get; set; }
        public int IntArg11 { get; set; }
        public int IntArg12 { get; set; }
        public int IntArg13 { get; set; }
        public int IntArg14 { get; set; }
        public int IntArg15 { get; set; }
        public int IntArg16 { get; set; }
        public int IntArg17 { get; set; }
        public int IntArg18 { get; set; }
        public int IntArg19 { get; set; }
        public int IntArg20 { get; set; }

        #endregion

        #region Long Arguments

        public long LongArg1 { get; set; }
        public long LongArg2 { get; set; }
        public long LongArg3 { get; set; }
        public long LongArg4 { get; set; }
        public long LongArg5 { get; set; }
        public long LongArg6 { get; set; }
        public long LongArg7 { get; set; }
        public long LongArg8 { get; set; }
        public long LongArg9 { get; set; }
        public long LongArg10 { get; set; }
        public long LongArg11 { get; set; }
        public long LongArg12 { get; set; }
        public long LongArg13 { get; set; }
        public long LongArg14 { get; set; }
        public long LongArg15 { get; set; }
        public long LongArg16 { get; set; }
        public long LongArg17 { get; set; }
        public long LongArg18 { get; set; }
        public long LongArg19 { get; set; }
        public long LongArg20 { get; set; }

        #endregion

        #region String Arguments

        public string StringArg1 { get; set; }
        public string StringArg2 { get; set; }
        public string StringArg3 { get; set; }
        public string StringArg4 { get; set; }
        public string StringArg5 { get; set; }
        public string StringArg6 { get; set; }
        public string StringArg7 { get; set; }
        public string StringArg8 { get; set; }
        public string StringArg9 { get; set; }
        public string StringArg10 { get; set; }
        public string StringArg11 { get; set; }
        public string StringArg12 { get; set; }
        public string StringArg13 { get; set; }
        public string StringArg14 { get; set; }
        public string StringArg15 { get; set; }
        public string StringArg16 { get; set; }
        public string StringArg17 { get; set; }
        public string StringArg18 { get; set; }
        public string StringArg19 { get; set; }
        public string StringArg20 { get; set; }

        #endregion

        public void SetKeys(IList<string> keysInOrder)
        {
            if (keysInOrder == null || !keysInOrder.Any())
            {
                return;
            }

            var setters = this.GetType().GetProperties().Where(p => p.Name.StartsWith("Key"))
                .Select(p => p.SetMethod).ToList();

            for (var i = 1; i <= 20; i++)
            {
                if (keysInOrder.Count < i)
                {
                    break;
                }

                var keyValue = keysInOrder[i - 1];
                var i1 = i;
                setters.FirstOrDefault(x => x.Name == $"set_Key{i1}")
                    ?.Invoke(this, new object[] { (RedisKey)$"ns=amit:k={keyValue}" });
            }
        }

        public void SetStringArguments(IList<object> argumentsInOrder)
        {
            SetArguments(argumentsInOrder, "String");
        }

        public void SetIntArguments(IList<object> argumentsInOrder)
        {
            SetArguments(argumentsInOrder, "Int");
        }

        private void SetArguments(IList<object> argumentsInOrder, string typePrefix)
        {
            if (argumentsInOrder == null || !argumentsInOrder.Any())
            {
                return;
            }

            var prefix = $"{typePrefix}Arg";
            var setters = this.GetType().GetProperties().Where(p => p.Name.StartsWith(prefix))
                .Select(p => p.SetMethod).ToList();

            for (var i = 1; i <= 20; i++)
            {
                if (argumentsInOrder.Count < i)
                {
                    break;
                }

                var arg = argumentsInOrder[i - 1];
                var i1 = i;
                setters.FirstOrDefault(x => x.Name == $"set_{prefix}{i1}")?.Invoke(this, new[] { arg });
            }
        }
    }
}