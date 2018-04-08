using NLog;
using StackExchange.Redis;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Payoneer.Infra.RedisRepo
{
    public static class RetryUtil
    {
        #region Retry

        internal const int NoRetries = 1;
        private const int RetryDelay = 50;

        public static TResult Retry<TResult>(Func<TResult> func, int maxAttempts)
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

        public static void Retry(Action action, int maxAttempts)
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

        public static bool TestExceptionForRetry(Exception ex)
        {
            return
                (
                    ex is TimeoutException
                    || ex is RedisException
                    || (
                            ex is AggregateException agrEx &&
                            !agrEx.InnerExceptions.Any(ex2 => ex2 is OutOfMemoryException || ex2 is StackOverflowException) &&
                            agrEx.InnerExceptions.Any(ex2 => ex2 is TimeoutException || ex2 is RedisException)
                        )
                );
        }

        #endregion
    }
}
