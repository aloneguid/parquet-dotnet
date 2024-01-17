using System;
using System.Collections.Generic;
using System.Linq;
using System.Security.Policy;
using System.Text;
using System.Threading.Tasks;
using Avalonia.Controls;
using CommunityToolkit.Mvvm.ComponentModel;

namespace Parquet.Floor.ViewModels {
    partial class TelemetryAgreementViewModel : ViewModelBase {

        [ObservableProperty]
        private bool _decisionMade;

        public TelemetryAgreementViewModel() {

            DecisionMade = Settings.Instance.TelemetryDecisionMade;

#if DEBUG
            if(Design.IsDesignMode) {
                DecisionMade = false;
            }
#endif
        }

        public bool Agree() {
            Settings.Instance.TelemetryDecisionMade = true;
            Settings.Instance.BasicTelemetryEnabled = true;
            return true;
        }

        public bool OptOut() {
            Settings.Instance.TelemetryDecisionMade = true;
            Settings.Instance.BasicTelemetryEnabled = false;
            return true;
        }
    }
}
