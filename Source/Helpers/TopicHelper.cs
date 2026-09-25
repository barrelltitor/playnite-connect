using MQTTnet.Client;
using System.Threading;

namespace PlayniteConnect.Helpers
{
    public class TopicHelper
    {
        private readonly MqttClient client;


        // Settings can be edited while MQTT is still connected. Keep routing on the
        // device ID that established the active session until the next connection
        // has been configured, so the old retained availability state is cleared.
        private string activeDeviceId;

        public TopicHelper(MqttClient client, PlayniteConnectSettingsViewModel settings)
        {
            this.client = client;
            activeDeviceId = settings.Settings.DeviceId;
        }

        public void SetActiveDeviceId(string deviceId)
        {
            Volatile.Write(ref activeDeviceId, deviceId);
        }

        public bool TryGetTopic(string subTopic, out string topicOut)
        {
            var deviceId = Volatile.Read(ref activeDeviceId);
            if (!client.IsConnected || string.IsNullOrEmpty(deviceId))
            {
                topicOut = null;
                return false;
            }

            if (!string.IsNullOrEmpty(subTopic))
            {
                topicOut = $"playnite/{deviceId}/{subTopic}";
                return true;
            }

            topicOut = null;
            return false;
        }
    }
}