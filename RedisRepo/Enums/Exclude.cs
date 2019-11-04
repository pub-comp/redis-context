namespace PubComp.RedisRepo.Enums
{
    public enum Exclude
    {
        /// <summary>
        /// Both start and stop are inclusive
        /// </summary>
        None = 0,

        /// <summary>
        /// Start is exclusive, stop is inclusive
        /// </summary>
        Start = 1,

        /// <summary>
        /// Start is inclusive, stop is exclusive
        /// </summary>
        Stop = 2,

        /// <summary>
        /// Both start and stop are exclusive
        /// </summary>
        Both = 3
    }
}
