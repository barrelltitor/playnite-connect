
using Microsoft.Win32;
using MQTTClient.Helpers;
using MQTTnet;
using MQTTnet.Client;
using MQTTnet.Protocol;
using Newtonsoft.Json;
using Playnite.SDK;
using Playnite.SDK.Events;
using Playnite.SDK.Models;
using Playnite.SDK.Plugins;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Net;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Windows.Controls;

namespace MQTTClient
{
    public class MQTTClient : GenericPlugin
    {
        private static readonly ILogger logger = LogManager.GetLogger();

        private readonly List<MainMenuItem> mainMenuItems;

        private readonly List<SidebarItem> sidebarItems;

        private readonly MqttClient client;

        private readonly MQTTClientSettingsViewModel settings;

        private readonly CoverApiServer coverApiServer;

        private readonly TopicHelper topicHelper;

        private readonly ObjectSerializer serializer;

        private readonly CancellationTokenSource applicationClosingCompletionSource;

        private readonly IProgress<float> sidebarProgress;

        private readonly SemaphoreSlim librarySyncLock = new SemaphoreSlim(1, 1);

        private readonly IProgress<ConnectionState> connectedState;

        private PowerModes lastPowerMode;

        public MQTTClient(IPlayniteAPI api) : base(api)
        {
            serializer = new ObjectSerializer();
            settings = new MQTTClientSettingsViewModel(this);
            coverApiServer = new CoverApiServer(PlayniteApi, () => settings.Settings.CoverApiToken);
            Properties = new GenericPluginProperties
            {
                HasSettings = true
            };

            applicationClosingCompletionSource = new CancellationTokenSource();
            client = (MqttClient)new MqttFactory().CreateMqttClient();
            topicHelper = new TopicHelper(client, settings);
            
            var progressSidebar = new SidebarItem
            {
                Visible = true,
                Icon = Path.Combine(Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location), "Resources", "icon.png"),
                Activated = SideButtonActivated,
                ProgressMaximum = 1
            };
            sidebarItems = new List<SidebarItem>
            {
                progressSidebar
            };
            sidebarProgress = new Progress<float>(progress => progressSidebar.ProgressValue = progress);
            connectedState = new Progress<ConnectionState>(v =>
            {
                switch (v)
                {

                    case ConnectionState.Disconnected:
                        progressSidebar.Title = "MQTT (disconnected)";
                        break;
                    case ConnectionState.Connected:
                        progressSidebar.Title = "MQTT (connected)";
                        break;
                    case ConnectionState.Connecting:
                        progressSidebar.Title = "MQTT (connecting)";
                        break;
                    default:
                        throw new ArgumentOutOfRangeException(nameof(v), v, null);
                }
            });
            connectedState.Report(ConnectionState.Disconnected);
            mainMenuItems = new List<MainMenuItem>
            {
                new MainMenuItem
                {
                    Description = "Reconnect", MenuSection = "@Playnite Connect", Action = ReconnectMenuAction
                },
                new MainMenuItem
                {
                    Description = "Disconnect", MenuSection = "@Playnite Connect", Action = DisconnectMenuAction
                }
            };
        }

        public async Task StartDisconnect(bool notify = false)
        {
            if (!client.IsConnected)
            {
                return;
            }

            try
            {
                if (topicHelper.TryGetTopic(Topics.ConnectionSubTopic, out var connectionTopic))
                {
                    await client.PublishStringAsync(connectionTopic, "offline", retain: true);
                }
            }
            catch (Exception exception)
            {
                // An unavailable broker must not prevent a local disconnect.
                logger.Warn(exception, "Failed to publish MQTT offline status before disconnecting.");
            }
            finally
            {
                try
                {
                    await client.DisconnectAsync();
                }
                catch (Exception exception)
                {
                    logger.Warn(exception, "Failed to disconnect MQTT client cleanly.");
                }
            }

            if (notify && !client.IsConnected)
            {
                PlayniteApi.Dialogs.ShowMessage("MQTT Disconnected", "MQTT Status");
            }
        }
        public async Task<MqttClientConnectResult> StartConnectionTask(bool notifyCompletion, IProgress<float> progress = null,CancellationToken cancellationToken = default)
        {
            var optionsUnBuilt = new MqttClientOptionsBuilder().WithClientId(settings.Settings.ClientId)
                .WithTcpServer(settings.Settings.ServerAddress, settings.Settings.Port)
                .WithCredentials(settings.Settings.Username, LoadPassword())
                .WithCleanSession().WithKeepAlivePeriod(TimeSpan.FromSeconds(5));

            if (settings.Settings.UseSecureConnection)
            {
                optionsUnBuilt = optionsUnBuilt.WithTlsOptions(o =>
                {
                    o.UseTls();
                    if (!string.IsNullOrEmpty(settings.Settings.CertificatePath) && File.Exists(settings.Settings.CertificatePath))
                    {
                        var caChain = new X509Certificate2Collection();
                        caChain.Import(settings.Settings.CertificatePath);
                        o.WithClientCertificates(caChain);
                        o.WithRevocationMode(X509RevocationMode.NoCheck);
                    }
                });
            }

            var options = optionsUnBuilt.Build();
            try
            {
                progress?.Report(0.1f);
                var connectionResult = await client.ConnectAsync(options, cancellationToken);
                if (notifyCompletion && client.IsConnected)
                {
                    PlayniteApi.Dialogs.ShowMessage("MQTT Connected","MQTT Status");
                }
                if (settings.Settings.Notifications && client.IsConnected)
                {
                    //PlayniteApi.Notifications.Add("Playnite MQTT Library", "MQTT Connected", NotificationType.Info);
                    PlayniteApi.Notifications.Add(
                        new NotificationMessage(Guid.NewGuid().ToString(), DateTime.Now.ToString("dd/MM/yyyy HH:mm:ss") + "\nMQTT Connected", NotificationType.Info)
                    );
                }
                if (client.IsConnected)
                {
                    logger.Debug("MQTT Connected");
                }

                return connectionResult;
            }
            catch (Exception e)
            {
                PlayniteApi.Dialogs.ShowErrorMessage(
                    $"MQTT: {e.Message}",
                    "MQTT Error");
            }

            return null;
        }

        public GlobalProgressResult StartConnection(bool notifyCompletion = false)
        {
            connectedState.Report(ConnectionState.Connecting);
            if (client.IsConnected)
            {
                PlayniteApi.Notifications.Add(
                    new NotificationMessage(Guid.NewGuid().ToString(), "Connection to MQTT underway", NotificationType.Error));
                throw new Exception("Connection to MQTT underway");
            }

            return PlayniteApi.Dialogs.ActivateGlobalProgress(
                args =>
                {
                    args.ProgressMaxValue = 1;
                    args.CurrentProgressValue = 0;
                    StartConnectionTask(notifyCompletion, sidebarProgress, args.CancelToken).ContinueWith(t => args.CurrentProgressValue = 1,args.CancelToken);
                },
                new GlobalProgressOptions($"Connection to MQTT ({settings.Settings.ServerAddress}:{settings.Settings.Port})", true));
        }

        private string LoadPassword()
        {
            if (settings.Settings.Password == null)
            {
                return "";
            }

            return Encoding.UTF8.GetString(ProtectedData.Unprotect(settings.Settings.Password, Id.ToByteArray(), DataProtectionScope.CurrentUser));
        }

        private void SideButtonActivated()
        {
            if (client.IsConnected)
            {
                _ = DisconnectFromUiAsync(settings.Settings.ShowStatusChanged);
                return;
            }

            StartConnection(settings.Settings.ShowStatusChanged);
        }

        private async Task DisconnectFromUiAsync(bool notifyCompletion)
        {
            await StartDisconnect(notifyCompletion);
            if (settings.Settings.Notifications && !client.IsConnected)
            {
                PlayniteApi.Notifications.Add(
                    new NotificationMessage(
                        Guid.NewGuid().ToString(),
                        DateTime.Now.ToString("dd/MM/yyyy HH:mm:ss") + "\nMQTT Disconnected",
                        NotificationType.Info));
            }
        }
        private async Task ClientOnConnectedAsync(EventArgs eventArgs)
        {
            sidebarProgress.Report(0.6f);
            if (topicHelper.TryGetTopic(Topics.ConnectionSubTopic, out var connectionTopic))
            {
                await client.PublishStringAsync(connectionTopic, "online", cancellationToken: applicationClosingCompletionSource.Token, retain: true);
            }

            await SubscribeToLibraryProtocolAsync(applicationClosingCompletionSource.Token);
            await PublishLibrarySnapshotAsync(null, applicationClosingCompletionSource.Token);
            sidebarProgress.Report(1f);
            connectedState.Report(ConnectionState.Connected);
        }

        private void DisconnectMenuAction(MainMenuItemActionArgs obj)
        {
            _ = DisconnectFromUiAsync(settings.Settings.ShowStatusChanged);
        }

        private void ReconnectMenuAction(MainMenuItemActionArgs obj)
        {
            _ = ReconnectAsync();
        }

        private async Task ReconnectAsync()
        {
            await StartDisconnect();
            StartConnection(true);
        }
        private Task ClientOnDisconnectedAsync(EventArgs eventArgs)
        {
            connectedState.Report(ConnectionState.Disconnected);
            sidebarProgress.Report(-1);

            logger.Debug("MQTT client disconnected.");
            if (lastPowerMode == PowerModes.Resume)
            {
                lastPowerMode = 0;
                logger.Debug("Last power modes is Resume. Connecting...");
                Task.Run(async () =>
                {
                    var sidebarItem = sidebarItems.First();
                    sidebarItem.ProgressMaximum = 1;
                    sidebarItem.ProgressValue = 0;
                    try
                    {
                        await StartConnectionTask(false, cancellationToken: applicationClosingCompletionSource.Token);
                        sidebarItem.ProgressValue = 1;
                        logger.Debug("MQTT client reconnected after disconnect on power resume.");
                    }
                    catch (Exception ex)
                    {
                        logger.Debug($"MQTT reconnection failed on power resume. Exception: {ex}");
                    }
                });
            }

            return Task.CompletedTask;
        }

        private async Task SubscribeToLibraryProtocolAsync(CancellationToken cancellationToken)
        {
            var topics = new[]
            {
                Topics.LibraryRequestSubTopic,
                Topics.LibraryCommandSubTopic,
                Topics.LibraryCoverRequestSubTopic
            };
            foreach (var subTopic in topics)
            {
                if (!topicHelper.TryGetTopic(subTopic, out var topic))
                {
                    continue;
                }

                await client.SubscribeAsync(
                    new MqttClientSubscribeOptionsBuilder()
                        .WithTopicFilter(topic, MqttQualityOfServiceLevel.AtLeastOnce)
                        .Build(),
                    cancellationToken);
            }
        }

        private async Task ClientOnApplicationMessageReceivedAsync(MqttApplicationMessageReceivedEventArgs args)
        {
            if (!topicHelper.TryGetTopic(Topics.LibraryRequestSubTopic, out var requestTopic) ||
                !topicHelper.TryGetTopic(Topics.LibraryCommandSubTopic, out var commandTopic) ||
                !topicHelper.TryGetTopic(Topics.LibraryCoverRequestSubTopic, out var coverRequestTopic))
            {
                return;
            }

            var messageTopic = args.ApplicationMessage.Topic;
            var payloadSegment = args.ApplicationMessage.PayloadSegment;
            var payload = payloadSegment.Array == null
                ? string.Empty
                : Encoding.UTF8.GetString(payloadSegment.Array, payloadSegment.Offset, payloadSegment.Count);
            try
            {
                if (string.Equals(messageTopic, requestTopic, StringComparison.Ordinal))
                {
                    var request = JsonConvert.DeserializeObject<LibraryRequest>(payload) ?? new LibraryRequest();
                    await PublishLibrarySnapshotAsync(request.RequestId, applicationClosingCompletionSource.Token);
                }
                else if (string.Equals(messageTopic, commandTopic, StringComparison.Ordinal))
                {
                    var command = JsonConvert.DeserializeObject<LibraryCommand>(payload);
                    if (command == null)
                    {
                        throw new InvalidOperationException("Library command payload is empty.");
                    }

                    await HandleLibraryCommandAsync(command, applicationClosingCompletionSource.Token);
                }
                else if (string.Equals(messageTopic, coverRequestTopic, StringComparison.Ordinal))
                {
                    var request = JsonConvert.DeserializeObject<LibraryCoverRequest>(payload);
                    if (request == null)
                    {
                        throw new InvalidOperationException("Cover request payload is empty.");
                    }

                    await PublishCoverAsync(request, applicationClosingCompletionSource.Token);
                }
            }
            catch (Exception exception)
            {
                logger.Error(exception, "Failed to process a Playnite library MQTT message.");
            }
        }
        private async Task PublishLibrarySnapshotAsync(string requestId, CancellationToken cancellationToken)
        {
            if (!client.IsConnected ||
                !topicHelper.TryGetTopic(Topics.LibraryManifestSubTopic, out var manifestTopic) ||
                !topicHelper.TryGetTopic(Topics.LibraryChunkSubTopic, out var chunkTopic))
            {
                return;
            }

            await librarySyncLock.WaitAsync(cancellationToken);
            try
            {
                if (!client.IsConnected)
                {
                    return;
                }

                var revision = Guid.NewGuid().ToString("N");
                var games = PlayniteApi.Database.Games
                    .Select(game => new LibraryGameData(game))
                    .OrderBy(game => game.Name ?? string.Empty, StringComparer.OrdinalIgnoreCase)
                    .ToList();
                var chunks = games
                    .Select((game, index) => new { game, index })
                    .GroupBy(item => item.index / LibraryProtocol.GamesPerChunk)
                    .Select(group => group.Select(item => item.game).ToList())
                    .ToList();

                for (var index = 0; index < chunks.Count; index++)
                {
                    var chunk = new LibraryChunk
                    {
                        Revision = revision,
                        Index = index,
                        Games = chunks[index]
                    };
                    await client.PublishStringAsync(
                        $"{chunkTopic}/{index}",
                        serializer.Serialize(chunk),
                        MqttQualityOfServiceLevel.AtLeastOnce,
                        true,
                        cancellationToken);
                }

                // The retained manifest is the commit marker. A consumer only accepts
                // chunks with its revision, so older retained chunks are harmless.
                var manifest = new LibraryManifest
                {
                    Revision = revision,
                    GeneratedAt = DateTime.UtcNow,
                    GameCount = games.Count,
                    ChunkCount = chunks.Count,
                    RequestId = requestId
                };
                await client.PublishStringAsync(
                    manifestTopic,
                    serializer.Serialize(manifest),
                    MqttQualityOfServiceLevel.AtLeastOnce,
                    true,
                    cancellationToken);
                // Status is retained separately from the large snapshot. It powers the
                // native HA selected-game control and read-only active-view status.
                await PublishLibraryStatusAsync(cancellationToken);
                logger.Info($"Published Playnite library snapshot {revision} with {games.Count} games in {chunks.Count} chunks.");
            }
            finally
            {
                librarySyncLock.Release();
            }
        }

        private async Task PublishLibraryStatusAsync(CancellationToken cancellationToken)
        {
            if (!client.IsConnected || !topicHelper.TryGetTopic(Topics.LibraryStatusSubTopic, out var topic))
            {
                return;
            }

            var selectedGame = PlayniteApi.MainView.SelectedGames?.FirstOrDefault();
            var status = new LibraryStatus
            {
                SelectedGameId = selectedGame?.Id.ToString(),
                ActiveDesktopView = PlayniteApi.MainView.ActiveDesktopView.ToString(),
                PlayniteVersion = PlayniteApi.ApplicationInfo.ApplicationVersion.ToString()
            };
            await client.PublishStringAsync(
                topic,
                serializer.Serialize(status),
                MqttQualityOfServiceLevel.AtLeastOnce,
                true,
                cancellationToken);
        }
        private async Task PublishLibraryUpdateAsync(Game game, string eventName, CancellationToken cancellationToken)
        {
            if (!topicHelper.TryGetTopic(Topics.LibraryUpdateSubTopic, out var topic))
            {
                return;
            }

            await client.PublishStringAsync(
                topic,
                serializer.Serialize(new LibraryUpdate { Event = eventName, Game = new LibraryGameData(game) }),
                MqttQualityOfServiceLevel.AtLeastOnce,
                false,
                cancellationToken);
        }

        private async Task PublishCoverAsync(LibraryCoverRequest request, CancellationToken cancellationToken)
        {
            if (!Guid.TryParse(request.GameId, out var gameId))
            {
                await PublishCoverErrorAsync(request, "gameId must be a Playnite game GUID.", cancellationToken);
                return;
            }

            var game = PlayniteApi.Database.Games.FirstOrDefault(item => item.Id == gameId);
            if (game == null)
            {
                await PublishCoverErrorAsync(request, "The requested game no longer exists in Playnite.", cancellationToken);
                return;
            }

            var imageType = string.IsNullOrWhiteSpace(request.ImageType)
                ? LibraryProtocol.ImageTypeCover
                : request.ImageType.Trim().ToLowerInvariant();
            string imageReference;
            switch (imageType)
            {
                case LibraryProtocol.ImageTypeCover:
                    imageReference = game.CoverImage;
                    break;
                case LibraryProtocol.ImageTypeBackground:
                    imageReference = game.BackgroundImage;
                    break;
                case LibraryProtocol.ImageTypeIcon:
                    imageReference = game.Icon;
                    break;
                default:
                    await PublishCoverErrorAsync(request, "imageType must be cover, background, or icon.", cancellationToken);
                    return;
            }

            string imagePath;
            try
            {
                imagePath = PlayniteApi.Database.GetFullFilePath(imageReference);
            }
            catch (Exception exception)
            {
                logger.Error(exception, $"Failed to resolve Playnite {imageType} image for {game.Name}.");
                await PublishCoverErrorAsync(request, $"This game has no accessible {imageType} image.", cancellationToken);
                return;
            }

            if (string.IsNullOrWhiteSpace(imagePath) || !File.Exists(imagePath))
            {
                await PublishCoverErrorAsync(request, $"This game has no accessible {imageType} image.", cancellationToken);
                return;
            }

            var fileInfo = new FileInfo(imagePath);
            if (fileInfo.Length > LibraryProtocol.MaximumCoverBytes)
            {
                await PublishCoverErrorAsync(request, $"This Playnite {imageType} image exceeds the 10 MB transfer limit.", cancellationToken);
                return;
            }

            byte[] data;
            try
            {
                data = File.ReadAllBytes(imagePath);
            }
            catch (Exception exception)
            {
                logger.Error(exception, $"Failed to read Playnite {imageType} image for {game.Name}.");
                await PublishCoverErrorAsync(request, $"This game has no accessible {imageType} image.", cancellationToken);
                return;
            }
            var chunkCount = Math.Max(1, (int)Math.Ceiling((double)data.Length / LibraryProtocol.CoverBytesPerChunk));
            for (var index = 0; index < chunkCount; index++)
            {
                var offset = index * LibraryProtocol.CoverBytesPerChunk;
                var count = Math.Min(LibraryProtocol.CoverBytesPerChunk, data.Length - offset);
                var chunkData = new byte[count];
                Buffer.BlockCopy(data, offset, chunkData, 0, count);
                await PublishCoverChunkAsync(new LibraryCoverChunk
                {
                    RequestId = request.RequestId,
                    GameId = request.GameId,
                    ImageType = imageType,
                    Index = index,
                    ChunkCount = chunkCount,
                    ContentType = GetImageContentType(imagePath),
                    Data = Convert.ToBase64String(chunkData)
                }, cancellationToken);
            }
        }
        private async Task PublishCoverErrorAsync(LibraryCoverRequest request, string error, CancellationToken cancellationToken)
        {
            await PublishCoverChunkAsync(new LibraryCoverChunk
            {
                RequestId = request.RequestId,
                GameId = request.GameId,
                ImageType = request.ImageType,
                Error = error,
                ChunkCount = 0
            }, cancellationToken);
        }

        private async Task PublishCoverChunkAsync(LibraryCoverChunk chunk, CancellationToken cancellationToken)
        {
            if (!topicHelper.TryGetTopic(Topics.LibraryCoverChunkSubTopic, out var topic))
            {
                return;
            }

            await client.PublishStringAsync(
                $"{topic}/{chunk.RequestId}/{chunk.Index}",
                serializer.Serialize(chunk),
                MqttQualityOfServiceLevel.AtLeastOnce,
                false,
                cancellationToken);
        }

        private static string GetImageContentType(string path)
        {
            switch (Path.GetExtension(path).ToLowerInvariant())
            {
                case ".png": return "image/png";
                case ".webp": return "image/webp";
                case ".gif": return "image/gif";
                case ".bmp": return "image/bmp";
                default: return "image/jpeg";
            }
        }

        private async Task HandleLibraryCommandAsync(LibraryCommand command, CancellationToken cancellationToken)
        {
            var response = new LibraryResponse
            {
                RequestId = command.RequestId,
                Action = command.Action,
                GameId = command.GameId
            };

            try
            {
                if (!Guid.TryParse(command.GameId, out var gameId))
                {
                    throw new InvalidOperationException("gameId must be a Playnite game GUID.");
                }

                var game = PlayniteApi.Database.Games.FirstOrDefault(item => item.Id == gameId);
                if (game == null)
                {
                    throw new InvalidOperationException("The requested game no longer exists in Playnite.");
                }

                switch ((command.Action ?? string.Empty).Trim().ToLowerInvariant())
                {
                    case "select":
                        // SelectGame is a real public Playnite API call, unlike the
                        // legacy MQTT Discovery select topic which had no handler.
                        PlayniteApi.MainView.SelectGame(gameId);
                        response.Message = "Game selected in Playnite.";
                        break;
                    case "start":
                        if (game.IsInstalled)
                        {
                            PlayniteApi.StartGame(gameId);
                            response.Message = "Start requested.";
                        }
                        else
                        {
                            PlayniteApi.InstallGame(gameId);
                            response.Message = "Game is not installed; install requested.";
                        }
                        break;
                    case "install":
                        if (!game.IsInstalled)
                        {
                            PlayniteApi.InstallGame(gameId);
                            response.Message = "Install requested.";
                        }
                        else
                        {
                            response.Message = "Game is already installed.";
                        }
                        break;
                    case "uninstall":
                        if (game.IsInstalled)
                        {
                            PlayniteApi.UninstallGame(gameId);
                            response.Message = "Uninstall requested.";
                        }
                        else
                        {
                            response.Message = "Game is already uninstalled.";
                        }
                        break;
                    case "stop":
                        var stoppedCount = StopGameProcesses(game);
                        if (stoppedCount == 0)
                        {
                            throw new InvalidOperationException("No running process could be matched safely to this game's install directory.");
                        }
                        response.Message = $"Stopped {stoppedCount} game process(es).";
                        break;
                    case "restart":
                        var restartStoppedCount = StopGameProcesses(game);
                        if (restartStoppedCount == 0)
                        {
                            throw new InvalidOperationException("No running process could be matched safely to this game's install directory.");
                        }
                        await Task.Delay(TimeSpan.FromSeconds(3), cancellationToken);
                        PlayniteApi.StartGame(gameId);
                        response.Message = $"Stopped {restartStoppedCount} game process(es) and requested restart.";
                        break;
                    default:
                        throw new InvalidOperationException("Unsupported action. Use select, start, stop, restart, install, or uninstall.");
                }

                response.Success = true;
            }
            catch (Exception exception)
            {
                response.Success = false;
                response.Message = exception.Message;
                logger.Error(exception, $"Failed Playnite library command {command.Action} for {command.GameId}.");
            }

            await PublishLibraryResponseAsync(response, cancellationToken);
        }

        private async Task PublishLibraryResponseAsync(LibraryResponse response, CancellationToken cancellationToken)
        {
            if (!topicHelper.TryGetTopic(Topics.LibraryResponseSubTopic, out var topic))
            {
                return;
            }

            await client.PublishStringAsync(
                topic,
                serializer.Serialize(response),
                MqttQualityOfServiceLevel.AtLeastOnce,
                false,
                cancellationToken);
        }

        public async void ApplySettings()
        {
            // Settings are already persisted by the view model when this runs.
            // Awaiting here keeps a reconnect from racing an active disconnect.
            await StartDisconnect();
            StartConnection();
            RestartCoverApi();
        }
        public void RestartCoverApi()
        {
            coverApiServer.Restart(
                settings.Settings.CoverApiEnabled,
                settings.Settings.CoverApiNetworkAccess,
                settings.Settings.CoverApiPort);
        }

        public bool EnableCoverApiNetworkAccess(out string message)
        {
            if (!IPAddress.TryParse(settings.Settings.CoverApiHomeAssistantAddress, out var homeAssistantAddress) ||
                homeAssistantAddress.AddressFamily != System.Net.Sockets.AddressFamily.InterNetwork)
            {
                message = "Enter Home Assistant's IPv4 address first.";
                return false;
            }

            try
            {
                var port = settings.Settings.CoverApiPort;
                var processInfo = new ProcessStartInfo
                {
                    FileName = "cmd.exe",
                    // URL ACL permits the listener. The firewall rule admits only the
                    // configured HA host; the bearer token is still required per request.
                    Arguments = $"/c netsh http add urlacl url=http://+:{port}/ sddl=D:(A;;GX;;;S-1-1-0) & netsh advfirewall firewall delete rule name=\"Playnite Connect Covers\" & netsh advfirewall firewall add rule name=\"Playnite Connect Covers\" dir=in action=allow protocol=tcp localport={port} remoteip={homeAssistantAddress} profile=private",
                    Verb = "runas",
                    UseShellExecute = true,
                    CreateNoWindow = true
                };
                var process = Process.Start(processInfo);
                process?.WaitForExit(10000);
                settings.Settings.CoverApiEnabled = true;
                settings.Settings.CoverApiNetworkAccess = true;
                SavePluginSettings(settings.Settings);
                RestartCoverApi();
                message = coverApiServer.IsNetworkBound
                    ? $"Network access enabled. Home Assistant can use http://<this-pc-ip>:{port}/api/covers/<game-id>."
                    : "Windows completed the request, but the API could not bind to the network. Check the Playnite log.";
                return coverApiServer.IsNetworkBound;
            }
            catch (System.ComponentModel.Win32Exception)
            {
                message = "Cancelled — administrator permission is required to enable network access.";
                return false;
            }
            catch (Exception exception)
            {
                logger.Error(exception, "Failed to enable Playnite Connect cover API network access.");
                message = $"Failed to enable network access: {exception.Message}";
                return false;
            }
        }
        private static int StopGameProcesses(Game game)
        {
            if (string.IsNullOrWhiteSpace(game.InstallDirectory) || !Directory.Exists(game.InstallDirectory))
            {
                throw new InvalidOperationException("This game has no accessible install directory, so it cannot be stopped safely.");
            }

            var installDirectory = Path.GetFullPath(game.InstallDirectory)
                .TrimEnd(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar) + Path.DirectorySeparatorChar;
            var stoppedCount = 0;
            foreach (var process in Process.GetProcesses())
            {
                try
                {
                    var executablePath = process.MainModule?.FileName;
                    if (string.IsNullOrEmpty(executablePath) ||
                        !Path.GetFullPath(executablePath).StartsWith(installDirectory, StringComparison.OrdinalIgnoreCase))
                    {
                        continue;
                    }

                    process.Kill();
                    stoppedCount++;
                }
                catch (Exception)
                {
                    // Processes owned by another session or protected by Windows are
                    // ignored. The response reports failure if none can be stopped.
                }
                finally
                {
                    process.Dispose();
                }
            }

            return stoppedCount;
        }
        #region Overrides of Plugin

        public override Guid Id { get; } = Guid.Parse("b81e4a83-823d-4a83-89e8-aee39f17a483");

        public override void OnLibraryUpdated(OnLibraryUpdatedEventArgs args)
        {
            Task.Run(
                () => PublishLibrarySnapshotAsync(null, applicationClosingCompletionSource.Token),
                applicationClosingCompletionSource.Token);
        }

        public override void OnGameSelected(OnGameSelectedEventArgs args)
        {
            // Playnite supplies the actual selection change; publish it for the
            // native HA select entity instead of pretending MQTT Discovery owns it.
            Task.Run(
                () => PublishLibraryStatusAsync(applicationClosingCompletionSource.Token),
                applicationClosingCompletionSource.Token);
        }
        public override void Dispose()
        {
            coverApiServer.Dispose();
            client.Dispose();
            base.Dispose();
        }

        public override void OnGameInstalled(OnGameInstalledEventArgs args)
        {
            Task.Run(() => PublishLibraryUpdateAsync(args.Game, "game_installed", applicationClosingCompletionSource.Token), applicationClosingCompletionSource.Token);
        }

        public override void OnGameStarted(OnGameStartedEventArgs args)
        {
            Task.Run(() => PublishLibraryUpdateAsync(args.Game, "game_started", applicationClosingCompletionSource.Token), applicationClosingCompletionSource.Token);
        }

        public override void OnGameStarting(OnGameStartingEventArgs args)
        {
            Task.Run(() => PublishLibraryUpdateAsync(args.Game, "game_starting", applicationClosingCompletionSource.Token), applicationClosingCompletionSource.Token);
        }

        public override void OnGameStopped(OnGameStoppedEventArgs args)
        {
            Task.Run(() => PublishLibraryUpdateAsync(args.Game, "game_stopped", applicationClosingCompletionSource.Token), applicationClosingCompletionSource.Token);
        }

        public override void OnGameUninstalled(OnGameUninstalledEventArgs args)
        {
            Task.Run(() => PublishLibraryUpdateAsync(args.Game, "game_uninstalled", applicationClosingCompletionSource.Token), applicationClosingCompletionSource.Token);
        }

        public override void OnApplicationStarted(OnApplicationStartedEventArgs args)
        {
            client.ConnectedAsync += ClientOnConnectedAsync;
            client.ConnectingAsync += ClientOnConnectingAsync;
            client.DisconnectedAsync += ClientOnDisconnectedAsync;
            client.ApplicationMessageReceivedAsync += ClientOnApplicationMessageReceivedAsync;
            SystemEvents.PowerModeChanged += SystemEventsOnPowerModeChanged;
            RestartCoverApi();
            if (settings.Settings.ShowProgress)
            {
                StartConnection();
            }
            else
            {
                Task.Run(async () =>
                {
                    var sidebarItem = sidebarItems.First();
                    sidebarItem.ProgressMaximum = 1;
                    sidebarItem.ProgressValue = 0;
                    try
                    {
                        await StartConnectionTask(false, cancellationToken:applicationClosingCompletionSource.Token);
                    }
                    finally
                    {
                        sidebarItem.ProgressValue = 1;
                    }
                });
            }
        }

        private Task ClientOnConnectingAsync(MqttClientConnectingEventArgs arg)
        {
            sidebarProgress.Report(0.5f);
            return Task.CompletedTask;
        }

        private void SystemEventsOnPowerModeChanged(object sender, PowerModeChangedEventArgs e)
        {
            logger.Debug($"System power mode changed to: {e.Mode}");
            lastPowerMode = e.Mode;
            if (e.Mode == PowerModes.Resume && !client.IsConnected)
            {
                StartConnection();
            }
        }

        public override void OnApplicationStopped(OnApplicationStoppedEventArgs args)
        {
            client.ConnectedAsync -= ClientOnConnectedAsync;
            client.ConnectingAsync -= ClientOnConnectingAsync;
            client.DisconnectedAsync -= ClientOnDisconnectedAsync;
            client.ApplicationMessageReceivedAsync -= ClientOnApplicationMessageReceivedAsync;
            SystemEvents.PowerModeChanged -= SystemEventsOnPowerModeChanged;
            StartDisconnect().Wait();
            applicationClosingCompletionSource.Cancel();
            applicationClosingCompletionSource.Dispose();
        }


        public override IEnumerable<SidebarItem> GetSidebarItems()
        {
            return sidebarItems;
        }

        public override IEnumerable<MainMenuItem> GetMainMenuItems(GetMainMenuItemsArgs args)
        {
            return mainMenuItems;
        }

        public override ISettings GetSettings(bool firstRunSettings)
        {
            return settings;
        }

        public override UserControl GetSettingsView(bool firstRunSettings)
        {
            return new MQTTClientSettingsView();
        }

        #endregion

        private enum ConnectionState
        {
            Disconnected,
            Connected,
            Connecting
        }
    }
}
