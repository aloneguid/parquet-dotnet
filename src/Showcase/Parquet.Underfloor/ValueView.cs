using System;
using System.Collections.Generic;
using System.Text;
using System.Text.Json;
using Grey;
using TextCopy;
using static Grey.App;

namespace Parquet.Underfloor;

static class ValueView {

    private static Window? _valueWindow;
    private static CodeEditor? _ce;
    private static string? _viewingvValue;

    public static void Render(string key, string? value) {
        if(Button($"{Icon.Copy_all}##{key}")) {
            ClipboardService.SetText(value ?? "");
            Notify($"Copied '{key}'");
        }

        SL();
        if(Button($"{Icon.View_list}##{key}")) {
            // view details
            ViewValue(value);
        }

        SL();
        Label(value ?? "");
    }

    public static void RenderViewer() {
        if(_valueWindow != null) {
            _valueWindow.Run(() => {
                if(Button("prettify")) {
                    FormatJson();
                }
                SL();
                if(Button("un-BASE64")) {
                    Unbase64();
                }

                if(_ce != null) {
                    _ce.Render();
                }
            });
        }
    }

    private static void FormatJson() {
        if(_viewingvValue == null)
            return;

        // try pretty-formatting
        try {
            string prettyJson = JsonSerializer.Serialize(JsonSerializer.Deserialize<JsonElement>(_viewingvValue),
                new JsonSerializerOptions { WriteIndented = true });
            ViewValue(prettyJson);
        } catch {

        }
    }

    private static void Unbase64() {
        if(_viewingvValue == null)
            return;

        // try un-base64-ing
        try {
            byte[] un = Convert.FromBase64String(_viewingvValue);
            ViewValue(Encoding.UTF8.GetString(un));
        } catch {

        }
    }

    private static void ViewValue(string? value) {
        if(_valueWindow == null) {
            _valueWindow = new Window("Value", initialWidth: 500);
        }
        if(_ce == null) {
            _ce = new CodeEditor(ProgrammingLanguage.Json);
        }
        _viewingvValue = value;
        _ce.Text = value ?? "";
    }
}
