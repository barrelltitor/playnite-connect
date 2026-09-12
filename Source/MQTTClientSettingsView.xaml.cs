using System.Text.RegularExpressions;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Input;

namespace MQTTClient
{
    public partial class MQTTClientSettingsView : UserControl
    {
        public MQTTClientSettingsView()
        {
            InitializeComponent();
        }

        private void NumberValidationTextBox(object sender, TextCompositionEventArgs e)
        {
            var regex = new Regex("[^0-9]+");
            e.Handled = regex.IsMatch(e.Text);
        }

        private void PasswordBox_OnPasswordChanged(object sender, RoutedEventArgs e)
        {
            var context = DataContext as MQTTClientSettingsViewModel;
            context?.SavePassword(PasswordBox.Password);
        }

        private void CopyCoverApiToken_Click(object sender, RoutedEventArgs e)
        {
            var context = DataContext as MQTTClientSettingsViewModel;
            if (context != null)
            {
                Clipboard.SetText(context.Settings.CoverApiToken);
            }
        }

        private void RegenerateCoverApiToken_Click(object sender, RoutedEventArgs e)
        {
            var context = DataContext as MQTTClientSettingsViewModel;
            if (context == null)
            {
                return;
            }
            if (MessageBox.Show("Regenerate the Cover API token? Home Assistant will need the new token immediately.", "Playnite Connect", MessageBoxButton.YesNo) == MessageBoxResult.Yes)
            {
                context.RegenerateCoverApiToken();
            }
        }

        private void EnableCoverApiNetwork_Click(object sender, RoutedEventArgs e)
        {
            var context = DataContext as MQTTClientSettingsViewModel;
            if (context == null)
            {
                return;
            }
            var confirmation = MessageBox.Show(
                "Allow Home Assistant to reach the Cover API over your private network?\n\nWindows will ask for administrator permission to create a URL reservation and firewall rule limited to the Home Assistant IP entered above.",
                "Playnite Connect",
                MessageBoxButton.YesNo);
            if (confirmation != MessageBoxResult.Yes)
            {
                return;
            }
            var enabled = context.EnableCoverApiNetworkAccess(out var message);
            MessageBox.Show(message, "Playnite Connect", MessageBoxButton.OK, enabled ? MessageBoxImage.Information : MessageBoxImage.Warning);
        }
    }
}