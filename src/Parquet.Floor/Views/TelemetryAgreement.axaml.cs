using Avalonia.Controls;
using Parquet.Floor.ViewModels;

namespace Parquet.Floor.Views {
    public partial class TelemetryAgreement : UserControl {
        public TelemetryAgreement() {
            InitializeComponent();

            DataContext = new TelemetryAgreementViewModel();
        }
    }
}
