using System.Net.Http;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Threading.Tasks;

namespace Parquet.Floor.Controllers {

    [JsonSourceGenerationOptions(WriteIndented = true)]
    [JsonSerializable(typeof(GitHub.Release))]
    internal partial class GitHubJsonContext : JsonSerializerContext {

    }

    class GitHub {

        public class Release {

            [JsonPropertyName("url")]
            public string? Url { get; set; }

            [JsonPropertyName("html_url")]
            public string? HtmlUrl { get; set; }

            [JsonPropertyName("tag_name")]
            public string? TagName { get; set; }

            [JsonPropertyName("name")]
            public string? Name { get; set; }

            [JsonPropertyName("body")]
            public string? Body { get; set; }
        }
        public async Task<Release> GetLatestRelease(string author, string repo) {
            string url = $"https://api.github.com/repos/{author}/{repo}/releases/latest";
            var request = new HttpRequestMessage(HttpMethod.Get, url);
            request.Headers.Add("User-Agent", "Parquet.Floor");
            HttpResponseMessage response = await new HttpClient().SendAsync(request);
            response.EnsureSuccessStatusCode();
            string jsonString = await response.Content.ReadAsStringAsync();
            return JsonSerializer.Deserialize(jsonString, GitHubJsonContext.Default.Release)!;
        }
    }
}
