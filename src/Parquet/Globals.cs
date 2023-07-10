namespace Parquet {
    /// <summary>
    /// Global values.
    /// CI should run envsubst to put actual values inside it. Prefer global constants to reflection as reflection is not
    /// available everywhere.
    /// </summary>
    public static class Globals {

        /// <summary>
        /// Library version
        /// </summary>
        public static readonly string Version = "${VERSION}";

        /// <summary>
        /// Github Actions stamped commit SHA. For more supported variables see
        /// https://docs.github.com/en/actions/learn-github-actions/variables
        /// </summary>
        public static readonly string GithubSha = "${GITHUB_SHA}";
    }
}
