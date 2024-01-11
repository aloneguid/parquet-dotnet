using System.Collections.Generic;
using System.Net.Http;
using System;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;
using NetBox.Performance;
using System.Linq;
using System.Text.Json.Serialization;

namespace Parquet.Floor {

    /// <summary>
    /// Simple stats tracker (no personal data allowed)
    /// </summary>
    public class Tracker {
        private readonly string _url;
        private readonly HttpClient _httpClient;
        private readonly string _version;
        private readonly Dictionary<string, string> _constants = new Dictionary<string, string>();

        class TrackClosure : IAsyncDisposable {
            private readonly TimeMeasure _tm = new TimeMeasure();
            private readonly Tracker _parent;
            private readonly string _eventName;
            private readonly Dictionary<string, string> _extras;

            public TrackClosure(Tracker parent, string eventName, Dictionary<string, string>? extras) {
                _parent = parent;
                _eventName = eventName;
                _extras = extras ?? new Dictionary<string, string>();
            }

            public async ValueTask DisposeAsync() {
                // duration in ms
                _extras["dms"] = _tm.ElapsedMilliseconds.ToString();

                await _parent.TrackAsync(_eventName, _extras);
            }
        }


        public static Tracker? Instance { get; set; }

        public Dictionary<string, string> Constants => _constants;

        public Tracker(string key, string version, string url = "https://alt.aloneguid.uk/events") {
            _url = $"{url}?key={key}";
            _httpClient = new HttpClient();
            _version = version;
            _constants["version"] = version;
        }

        public void Track(string eventName, Dictionary<string, string>? extras = null) {
#pragma warning disable CS4014 // Because this call is not awaited, execution of the current method continues before the call is completed
            TrackAsync(eventName, extras);
#pragma warning restore CS4014 // Because this call is not awaited, execution of the current method continues before the call is completed
        }

        private async ValueTask TrackAsync(string eventName, Dictionary<string, string>? extras = null) {

            var payload = new Dictionary<string, string>(_constants);

            var request = new HttpRequestMessage(HttpMethod.Post, _url);
            payload["t"] = DateTime.UtcNow.ToString("o");
            payload["e"] = eventName;

            if(extras != null) {
                foreach(KeyValuePair<string, string> item in extras) {
                    payload[item.Key] = item.Value;
                }
            }

            var sb = new StringBuilder();
            sb.Append("{");
            foreach(KeyValuePair<string, string> item in payload) {
                if(sb.Length > 1) {
                    sb.Append(",");
                }
                sb.Append($"\"{item.Key}\": \"{item.Value}\"");
            }
            sb.Append("}");

            request.Content = new StringContent(sb.ToString(), Encoding.UTF8, "application/json");
            await _httpClient.SendAsync(request);
        }

        public IAsyncDisposable TrackWithTime(string eventName, Dictionary<string, string>? extras = null) {
            return new TrackClosure(this, eventName, extras);
        }
    }
}