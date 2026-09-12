using Playnite.SDK;
using Playnite.SDK.Data;
using System;
using System.Collections.Generic;
using System.Security.Cryptography;
using System.Text;

namespace MQTTClient
{
    public class MQTTClientSettings : ObservableObject
    {
        private string clientId = "Playnite";
        private string deviceId = "playnite";
        private string serverAdress = "localhost";
        private bool useSecureConnection;
        private string certificatePath;
        private int? port = 1883;
        private string username;
        private byte[] password;
        private bool showProgress = true;
        private bool showStatusChanged = true;
        private bool notifications = true;
        private bool coverApiEnabled;
        private bool coverApiNetworkAccess;
        private int coverApiPort = 19829;
        private string coverApiHomeAssistantAddress;
        private string coverApiToken;

        public string ClientId { get => clientId; set => SetValue(ref clientId, value); }
        public string DeviceId { get => deviceId; set => SetValue(ref deviceId, value); }
        public string ServerAddress { get => serverAdress; set => SetValue(ref serverAdress, value); }
        public string Username { get => username; set => SetValue(ref username, value); }
        public byte[] Password { get => password; set => SetValue(ref password, value); }
        public int? Port { get => port; set => SetValue(ref port, value); }
        public bool UseSecureConnection { get => useSecureConnection; set => SetValue(ref useSecureConnection, value); }
        public bool ShowProgress { get => showProgress; set => SetValue(ref showProgress, value); }
        public bool ShowStatusChanged { get => showStatusChanged; set => SetValue(ref showStatusChanged, value); }
        public bool Notifications { get => notifications; set => SetValue(ref notifications, value); }
        public string CertificatePath { get => certificatePath; set => SetValue(ref certificatePath, value); }

        // The cover API is local-only until the user explicitly enables LAN access.
        public bool CoverApiEnabled { get => coverApiEnabled; set => SetValue(ref coverApiEnabled, value); }
        public bool CoverApiNetworkAccess { get => coverApiNetworkAccess; set => SetValue(ref coverApiNetworkAccess, value); }
        public int CoverApiPort { get => coverApiPort; set => SetValue(ref coverApiPort, value); }
        public string CoverApiHomeAssistantAddress { get => coverApiHomeAssistantAddress; set => SetValue(ref coverApiHomeAssistantAddress, value); }
        public string CoverApiToken { get => coverApiToken; set => SetValue(ref coverApiToken, value); }
    }

    public class MQTTClientSettingsViewModel : ObservableObject, ISettings
    {
        private readonly MQTTClient plugin;
        private MQTTClientSettings settings;
        public MQTTClientSettings Settings { get => settings; set { settings = value; OnPropertyChanged(); } }
        private MQTTClientSettings editingClone { get; set; }

        public MQTTClientSettingsViewModel(MQTTClient plugin)
        {
            this.plugin = plugin;
            Settings = plugin.LoadPluginSettings<MQTTClientSettings>() ?? new MQTTClientSettings();
            EnsureCoverApiToken();
        }

        public void SavePassword(string password)
        {
            settings.Password = ProtectedData.Protect(Encoding.UTF8.GetBytes(password), plugin.Id.ToByteArray(), DataProtectionScope.CurrentUser);
        }

        public void RegenerateCoverApiToken()
        {
            Settings.CoverApiToken = CreateCoverApiToken();
            plugin.SavePluginSettings(Settings);
            plugin.RestartCoverApi();
        }

        public bool EnableCoverApiNetworkAccess(out string message)
        {
            return plugin.EnableCoverApiNetworkAccess(out message);
        }

        public void BeginEdit()
        {
            // Code executed when the settings view opens and the user starts editing values.
            editingClone = Serialization.GetClone(Settings);
        }

        public void CancelEdit()
        {
            // Code executed when the user cancels changes made since BeginEdit was called.
            Settings = editingClone;
        }

        public void EndEdit()
        {
            // Code executed when the user confirms changes made since BeginEdit was called.
            plugin.SavePluginSettings(Settings);
            plugin.ApplySettings();
        }

        public bool VerifySettings(out List<string> errors)
        {
            // Code executed when the user decides to confirm changes made since BeginEdit was called.
            // Executed before EndEdit is called and EndEdit is not called if false is returned.
            // The returned errors are presented to the user if verification fails.
            errors = new List<string>();
            if (string.IsNullOrWhiteSpace(Settings.ServerAddress))
            {
                errors.Add("MQTT server address is required.");
            }
            if (!Settings.Port.HasValue || Settings.Port < 1 || Settings.Port > 65535)
            {
                errors.Add("MQTT port must be between 1 and 65535.");
            }
            if (string.IsNullOrWhiteSpace(Settings.DeviceId) ||
                Settings.DeviceId.IndexOfAny(new[] { '/', '+', '#' }) >= 0)
            {
                errors.Add("Device ID must not be blank or contain /, +, or #.");
            }
            if (Settings.CoverApiPort < 1024 || Settings.CoverApiPort > 65535)
            {
                errors.Add("Cover API port must be between 1024 and 65535.");
            }
            return errors.Count == 0;
        }
        private void EnsureCoverApiToken()
        {
            if (!string.IsNullOrWhiteSpace(Settings.CoverApiToken))
            {
                return;
            }
            Settings.CoverApiToken = CreateCoverApiToken();
            plugin.SavePluginSettings(Settings);
        }

        private static string CreateCoverApiToken()
        {
            var bytes = new byte[32];
            using (var random = new RNGCryptoServiceProvider())
            {
                random.GetBytes(bytes);
            }
            return "pc_" + BitConverter.ToString(bytes).Replace("-", string.Empty).ToLowerInvariant();
        }
    }
}