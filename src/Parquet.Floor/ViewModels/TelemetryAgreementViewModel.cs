using System;
using System.Collections.Generic;
using System.Linq;
using System.Security.Policy;
using System.Text;
using System.Threading.Tasks;
using ActiproSoftware.Extensions;
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

        public void Agree() {
            MakeDecision(true);
        }

        public void OptOut() {
            MakeDecision(false);
        }

        private void MakeDecision(bool optIn) {
            DecisionMade = true;
            Settings.Instance.TelemetryDecisionMade = true;
            Settings.Instance.BasicTelemetryEnabled = optIn;

            if(Tracker.Instance!.Constants.ContainsKey(Settings.TelemetryConstant)) {
                Tracker.Instance.Constants.Remove(Settings.TelemetryConstant);
            }
            Tracker.Instance.Constants.Add(Settings.TelemetryConstant, Settings.Instance.BasicTelemetryEnabled.ToString());
        }
    }
}
