using System;
using System.Collections.Generic;
using System.Text;
using Grey;
using static Grey.App;

namespace Parquet.Underfloor;

static class ValueView {

    private static Window? _valueWindow;
    private static CodeEditor? _ce;
    private static string? _viewingvValue;

    public static void Render(string key, string? value) {
        if(Button($"{Icon.Content_copy}##{key}")) {
            // copy to clipboard
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
                if(_ce != null) {
                    _ce.Render();
                }
            });
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
