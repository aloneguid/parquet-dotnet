using System.Collections.Generic;
using System.Net.Http;
using System;
using System.Text;
using System.Threading.Tasks;

namespace Parquet.Floor {

    /// <summary>
    /// Simple stats tracker (no personal data allowed)
    /// </summary>
    public class Tracker : IAsyncDisposable {
        private readonly string _url;
        private readonly HttpClient _httpClient;
        private readonly Dictionary<string, string> _constants = new Dictionary<string, string>();
        private readonly List<string> _events = new List<string>();
        private readonly object _lock = new object();

#pragma warning disable CS8618 // Non-nullable field must contain a non-null value when exiting constructor. Consider adding the 'required' modifier or declaring as nullable.
        public static Tracker Instance { get; set; }
#pragma warning restore CS8618 // Non-nullable field must contain a non-null value when exiting constructor. Consider adding the 'required' modifier or declaring as nullable.

        public Dictionary<string, string> Constants => _constants;

        public Tracker(string key, string version, string url = "https://alt.aloneguid.uk/events") {
            _url = $"{url}?key={key}";
            _httpClient = new HttpClient();
            _constants["version"] = version;
        }

        private string CreatePayloadLine(string eventName, Dictionary<string, string>? extras = null) {
            var payload = new Dictionary<string, string>(_constants);

            payload["t"] = DateTime.UtcNow.ToString("o");
            payload["e"] = eventName;

            if(extras != null) {
                foreach(KeyValuePair<string, string> item in extras) {
                    payload[item.Key] = item.Value;
                }
            }

            var sb = new StringBuilder();
            sb.Append('{');
            foreach(KeyValuePair<string, string> item in payload) {
                if(sb.Length > 1) {
                    sb.Append(',');
                }
                sb.Append($"\"{item.Key}\": \"{item.Value.ToEscapedJsonValueString()}\"");
            }
            sb.Append('}');

            return sb.ToString();
        }

        public void Track(string eventName, Dictionary<string, string>? extras = null, bool force = false) {

            _events.Add(CreatePayloadLine(eventName, extras));

#pragma warning disable CS4014 // Because this call is not awaited, execution of the current method continues before the call is completed
            if(force) {
                FlushAsync();
            }
#pragma warning restore CS4014 // Because this call is not awaited, execution of the current method continues before the call is completed
        }

        public async ValueTask FlushAsync() {

            if(_events.Count == 0) {
                return;
            }

            var request = new HttpRequestMessage(HttpMethod.Post, _url);
            string content = "[" + string.Join(",", _events) + "]";
            request.Content = new StringContent(content, Encoding.UTF8, "application/json");
            await _httpClient.SendAsync(request);
            _events.Clear();
        }

        public ValueTask DisposeAsync() {
            return FlushAsync();
        }
    }
}