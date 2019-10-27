using System;
using System.Collections.Generic;
using System.Runtime.Serialization;
using System.Text;

namespace PubComp.RedisRepo.Exceptions
{
    [Serializable]
    public class FailedToConnectException : ArgumentException
    {
        //
        // For guidelines regarding the creation of new exception types, see
        //    http://msdn.microsoft.com/library/default.asp?url=/library/en-us/cpgenref/html/cpconerrorraisinghandlingguidelines.asp
        // and
        //    http://msdn.microsoft.com/library/default.asp?url=/library/en-us/dncscol/html/csharp07192001.asp
        //

        public FailedToConnectException()
        {
        }

        public FailedToConnectException(string message) : base(message)
        {
        }

        public FailedToConnectException(string message, Exception inner) : base(message, inner)
        {
        }

        protected FailedToConnectException(
            SerializationInfo info,
            StreamingContext context) : base(info, context)
        {
        }
    }
}
