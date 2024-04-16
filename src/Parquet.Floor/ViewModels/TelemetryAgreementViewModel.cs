using Avalonia.Controls;
using CommunityToolkit.Mvvm.ComponentModel;

namespace Parquet.Floor.ViewModels {
    partial class TelemetryAgreementViewModel : ViewModelBase {

        [ObservableProperty]
        private bool _agreementPassed;

        public TelemetryAgreementViewModel() {

            AgreementPassed = Settings.Instance.TelemetryAgreementPassed;

#if DEBUG
            if(Design.IsDesignMode) {
                AgreementPassed = false;
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
            AgreementPassed = true;
            Settings.Instance.TelemetryAgreementPassed = true;
        }
    }
}
