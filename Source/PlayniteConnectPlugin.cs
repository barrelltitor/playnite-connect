
using Microsoft.Win32;
using PlayniteConnect.Helpers;
using MQTTnet;
using MQTTnet.Client;
using MQTTnet.Protocol;
using Newtonsoft.Json;
using Playnite.SDK;
using Playnite.SDK.Events;
using Playnite.SDK.Models;
using Playnite.SDK.Plugins;
using System;
using System.Collections.Concurrent;
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

namespace PlayniteConnect
{
    public class PlayniteConnectPlugin : GenericPlugin
    {
        private static readonly ILogger logger = LogManager.GetLogger();

        private readonly List<MainMenuItem> mainMenuItems;

        private readonly List<SidebarItem> sidebarItems;

        private readonly MqttClient client;

        private readonly PlayniteConnectSettingsViewModel settings;

        private readonly CoverApiServer coverApiServer;

        private readonly TopicHelper topicHelper;

        private readonly ObjectSerializer serializer;

        private readonly CancellationTokenSource applicationClosingCompletionSource;

        private readonly SidebarItem progressSidebarItem;

        private readonly SemaphoreSlim librarySyncLock = new SemaphoreSlim(1, 1);

        // MQTTnet dispatches received messages serially. Preserve that order in one
        // dedicated worker so command responses never block the receive callback and
        // start/stop operations cannot race each other.
        private readonly ConcurrentQueue<LibraryCommandWorkItem> libraryCommandQueue =
            new ConcurrentQueue<LibraryCommandWorkItem>();

        private readonly SemaphoreSlim libraryCommandSignal = new SemaphoreSlim(0);

        // MQTTnet allows concurrent publish/subscribe operations, but connection
        // lifecycle changes must not overlap them. A short state lock owns session
        // identity; this gate is used only by ConnectAsync and DisconnectAsync.
        private readonly object mqttSessionLock = new object();
        private readonly SemaphoreSlim mqttLifecycleGate = new SemaphoreSlim(1, 1);
        private MqttSession currentMqttSession;
        private Task activeDisconnectTask;

        private const int CoverTransferWorkerCount = 3;

        private const int ShutdownDisconnectTimeoutMilliseconds = 5000;

        // MQTTnet processes one received PUBLISH callback at a time. A cover can
        // comprise many QoS 1 chunks, so requests leave that callback immediately
        // and run through this small fixed worker pool instead.
        private readonly ConcurrentQueue<LibraryCoverWorkItem> coverTransferQueue =
            new ConcurrentQueue<LibraryCoverWorkItem>();

        private readonly SemaphoreSlim coverTransferSignal = new SemaphoreSlim(0);



        private int lastPowerMode;

        // MQTTnet does not expose a separate connecting state. Keep one local so
        // startup, a sidebar click, and a power-resume event cannot begin competing
        // ConnectAsync calls before IsConnected becomes true.
        private int isConnecting;

        private int isDisconnecting;

        private int isStopping;

        public PlayniteConnectPlugin(IPlayniteAPI api) : base(api)
        {
            serializer = new ObjectSerializer();
            settings = new PlayniteConnectSettingsViewModel(this);
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
            progressSidebarItem = progressSidebar;
            sidebarItems = new List<SidebarItem>
            {
                progressSidebar
            };
            progressSidebarItem.Title = "MQTT (disconnected)";
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

        // MQTTnet callbacks and cover workers run away from Playnite's UI thread.
        // Playnite's SDK is not fully thread-safe, so snapshot and command its API
        // through the dispatcher, then keep network and file I/O off that thread.
        private Task<T> RunOnPlayniteUiThreadAsync<T>(Func<T> action, CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var dispatcher = PlayniteApi.MainView.UIDispatcher;
            if (dispatcher.HasShutdownStarted || dispatcher.HasShutdownFinished)
            {
                return Task.FromCanceled<T>(new CancellationToken(true));
            }

            if (dispatcher.CheckAccess())
            {
                cancellationToken.ThrowIfCancellationRequested();
                return Task.FromResult(action());
            }

            return dispatcher.InvokeAsync(() =>
            {
                // The dispatcher may not run this item until after shutdown begins.
                // Recheck at execution time so a queued callback cannot touch the
                // Playnite database or UI once its owning operation was cancelled.
                cancellationToken.ThrowIfCancellationRequested();
                return action();
            }).Task;
        }
        private T RunOnPlayniteUiThread<T>(Func<T> action)
        {
            var dispatcher = PlayniteApi.MainView.UIDispatcher;
            if (dispatcher.HasShutdownStarted || dispatcher.HasShutdownFinished)
            {
                throw new InvalidOperationException("Playnite's UI is shutting down.");
            }

            return dispatcher.CheckAccess() ? action() : dispatcher.Invoke(action);
        }
        // MQTT callbacks can arrive on worker threads. Do not rely on Progress<T>
        // capturing a UI synchronization context; explicitly marshal sidebar changes.
        private void QueuePlayniteUiUpdate(Action action, string description)
        {
            if (Volatile.Read(ref isStopping) != 0)
            {
                return;
            }

            try
            {
                var uiUpdate = RunOnPlayniteUiThreadAsync(() =>
                {
                    action();
                    return true;
                }, applicationClosingCompletionSource.Token);
                uiUpdate.ContinueWith(
                    completed => logger.Warn(completed.Exception, $"Failed to {description}."),
                    CancellationToken.None,
                    TaskContinuationOptions.OnlyOnFaulted,
                    TaskScheduler.Default);
            }
            catch (OperationCanceledException)
            {
                // Playnite is shutting down, so a sidebar update is no longer useful.
            }
            catch (Exception exception)
            {
                logger.Warn(exception, $"Failed to schedule {description}.");
            }
        }

        private void ReportSidebarProgress(float progress)
        {
            QueuePlayniteUiUpdate(
                () => progressSidebarItem.ProgressValue = progress,
                "update the MQTT sidebar progress");
        }

        private void ReportConnectionState(ConnectionState state)
        {
            QueuePlayniteUiUpdate(() =>
            {
                switch (state)
                {
                    case ConnectionState.Disconnected:
                        progressSidebarItem.Title = "MQTT (disconnected)";
                        break;
                    case ConnectionState.Connected:
                        progressSidebarItem.Title = "MQTT (connected)";
                        break;
                    case ConnectionState.Connecting:
                        progressSidebarItem.Title = "MQTT (connecting)";
                        break;
                    default:
                        throw new ArgumentOutOfRangeException(nameof(state), state, null);
                }
            }, "update the MQTT sidebar connection state");
        }
        // Keep event-triggered network work observable. MQTTnet and Playnite do not
        // await these lifecycle callbacks, so an unobserved fault otherwise vanishes.
        private void RunBackground(Func<Task> action, string description)
        {
            var cancellationToken = applicationClosingCompletionSource.Token;
            if (cancellationToken.IsCancellationRequested)
            {
                return;
            }

            _ = Task.Run(async () =>
            {
                try
                {
                    await action().ConfigureAwait(false);
                }
                catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                {
                    // Expected while Playnite is closing.
                }
                catch (Exception exception)
                {
                    logger.Error(exception, $"Playnite Connect background task failed: {description}.");
                }
            }, cancellationToken);
        }

        private void RunUiOperation(Func<Task> action, string description)
        {
            try
            {
                var operation = action();
                operation.ContinueWith(
                    completed => logger.Error(completed.Exception, $"Playnite Connect UI operation failed: {description}."),
                    CancellationToken.None,
                    TaskContinuationOptions.OnlyOnFaulted,
                    TaskScheduler.Default);
            }
            catch (OperationCanceledException)
            {
                // Playnite is closing and the requested UI operation is no longer useful.
            }
            catch (Exception exception)
            {
                logger.Error(exception, $"Failed to start Playnite Connect UI operation: {description}.");
            }
        }
        private bool TryStartMqttSession(CancellationToken requestedCancellationToken, Task<MqttClientConnectResult> connectionTask, out MqttSession session)
        {
            lock (mqttSessionLock)
            {
                if (Volatile.Read(ref isStopping) != 0 ||
                    Volatile.Read(ref isDisconnecting) != 0 ||
                    Volatile.Read(ref isConnecting) != 0 ||
                    client.IsConnected)
                {
                    session = null;
                    return false;
                }

                // A previous failed or interrupted session cannot publish again once
                // a new attempt takes ownership of the client and its MQTT topic
                // identity. Drain any work that was already accepted before calling
                // ConnectAsync again, because MQTTnet does not permit that lifecycle
                // transition to overlap old publish work.
                var retiredSession = currentMqttSession;
                retiredSession?.CancellationSource.Cancel();
                RetireQueuedMqttWork(retiredSession);
                var retiredSessionWork = retiredSession == null
                    ? Task.CompletedTask
                    : WaitForMqttSessionWorkAsync(retiredSession);

                session = new MqttSession(
                    settings.Settings.DeviceId,
                    CancellationTokenSource.CreateLinkedTokenSource(
                        applicationClosingCompletionSource.Token,
                        requestedCancellationToken))
                {
                    RetiredSessionWork = retiredSessionWork
                };
                session.ConnectionTask = connectionTask;
                currentMqttSession = session;
                Volatile.Write(ref isConnecting, 1);
                return true;
            }
        }

        private MqttSession GetCurrentMqttSession()
        {
            lock (mqttSessionLock)
            {
                return currentMqttSession;
            }
        }

        private bool IsCurrentMqttSession(MqttSession session)
        {
            lock (mqttSessionLock)
            {
                return ReferenceEquals(currentMqttSession, session);
            }
        }

        private void MarkCurrentMqttSessionUnavailable()
        {
            lock (mqttSessionLock)
            {
                if (currentMqttSession != null)
                {
                    currentMqttSession.IsReady = false;
                    // A transport loss may happen while a cover or snapshot worker is
                    // between operations. Retire its token now so it cannot resume and
                    // publish into a later connection with different settings.
                    currentMqttSession.CancellationSource.Cancel();
                }
            }
        }
        private bool TryMarkMqttSessionReady(MqttSession session)
        {
            lock (mqttSessionLock)
            {
                if (!ReferenceEquals(currentMqttSession, session) ||
                    session.CancellationSource.IsCancellationRequested ||
                    Volatile.Read(ref isDisconnecting) != 0)
                {
                    return false;
                }

                session.IsReady = true;
                return true;
            }
        }

        private bool TryAcquireMqttSessionWork(out MqttSession session)
        {
            lock (mqttSessionLock)
            {
                session = currentMqttSession;
                if (session == null ||
                    !session.IsReady ||
                    session.CancellationSource.IsCancellationRequested ||
                    Volatile.Read(ref isDisconnecting) != 0)
                {
                    return false;
                }

                session.ActiveExternalOperations++;
                return true;
            }
        }

        private bool IsMqttSessionReady(MqttSession session)
        {
            lock (mqttSessionLock)
            {
                return ReferenceEquals(currentMqttSession, session) &&
                    session.IsReady &&
                    !session.CancellationSource.IsCancellationRequested &&
                    Volatile.Read(ref isDisconnecting) == 0;
            }
        }

        private void ReleaseMqttSessionWork(MqttSession session)
        {
            lock (mqttSessionLock)
            {
                session.ActiveExternalOperations--;
                if (session.ActiveExternalOperations == 0)
                {
                    session.ExternalOperationsDrained.TrySetResult(true);
                }
            }
        }

        private Task WaitForMqttSessionWorkAsync(MqttSession session)
        {
            lock (mqttSessionLock)
            {
                return session.ActiveExternalOperations == 0
                    ? Task.CompletedTask
                    : session.ExternalOperationsDrained.Task;
            }
        }

        private void RetireQueuedMqttWork(MqttSession session)
        {
            if (session == null)
            {
                return;
            }

            // This runs before another session can become ready. Every queued item
            // still belongs to this retiring session, so release its operation lease
            // rather than making DisconnectAsync wait for a worker that will no
            // longer be allowed to act on it. Extra semaphore signals are harmless:
            // the workers simply observe an empty queue and wait again.
            while (libraryCommandQueue.TryDequeue(out var commandWorkItem))
            {
                ReleaseMqttSessionWork(commandWorkItem.Session);
            }

            while (coverTransferQueue.TryDequeue(out var coverWorkItem))
            {
                ReleaseMqttSessionWork(coverWorkItem.Session);
            }
        }

        private static async Task WaitForMqttWorkOrCancellationAsync(Task work, CancellationToken cancellationToken)
        {
            if (work.IsCompleted)
            {
                await work.ConfigureAwait(false);
                return;
            }

            var cancellationCompletion = new TaskCompletionSource<bool>();
            using (cancellationToken.Register(() => cancellationCompletion.TrySetCanceled()))
            {
                var completed = await Task.WhenAny(work, cancellationCompletion.Task).ConfigureAwait(false);
                await completed.ConfigureAwait(false);
            }
        }
        private void RunSessionBackground(Func<CancellationToken, Task> action, string description)
        {
            if (!TryAcquireMqttSessionWork(out var session))
            {
                return;
            }

            // Do not pass the application token to Task.Run: even if shutdown wins
            // before the worker begins, its finally block must release this session's
            // operation lease so DisconnectAsync can safely drain it.
            _ = Task.Run(async () =>
            {
                try
                {
                    await action(session.CancellationSource.Token).ConfigureAwait(false);
                }
                catch (OperationCanceledException) when (session.CancellationSource.IsCancellationRequested)
                {
                    // Expected when a disconnect/reconfigure retires this session.
                }
                catch (Exception exception)
                {
                    logger.Error(exception, $"Playnite Connect session task failed: {description}.");
                }
                finally
                {
                    ReleaseMqttSessionWork(session);
                }
            });
        }

        // Called while mqttSessionLock is held. Once this marks the session retiring,
        // command and cover callbacks can no longer add a queue lease behind it.
        private MqttSession BeginMqttDisconnectNoLock()
        {
            Volatile.Write(ref isDisconnecting, 1);
            var session = currentMqttSession;
            if (session != null)
            {
                session.IsReady = false;
                session.CancellationSource.Cancel();
                RetireQueuedMqttWork(session);
            }

            return session;
        }

        private void CompleteMqttDisconnect(MqttSession session, Task disconnectTask)
        {
            lock (mqttSessionLock)
            {
                if (ReferenceEquals(currentMqttSession, session))
                {
                    currentMqttSession = null;
                }

                Volatile.Write(ref isDisconnecting, 0);
                if (ReferenceEquals(activeDisconnectTask, disconnectTask))
                {
                    activeDisconnectTask = null;
                }
            }
        }
        public Task StartDisconnect(bool notify = false)
        {
            Task disconnectTask;
            lock (mqttSessionLock)
            {
                // Coalesce simultaneous sidebar, menu, settings, and shutdown calls.
                // Only the owner of this task may operate the MQTT lifecycle or clear
                // the active session, so a delayed older request cannot disconnect a
                // freshly reconnected client.
                disconnectTask = activeDisconnectTask;
                if (disconnectTask == null)
                {
                    var completion = new TaskCompletionSource<bool>();
                    disconnectTask = completion.Task;
                    activeDisconnectTask = disconnectTask;
                    var session = BeginMqttDisconnectNoLock();
                    _ = Task.Run(() => CompleteMqttDisconnectAttemptAsync(session, completion));
                }
            }

            return notify
                ? NotifyWhenMqttDisconnectedAsync(disconnectTask)
                : disconnectTask;
        }

        private async Task CompleteMqttDisconnectAttemptAsync(MqttSession session, TaskCompletionSource<bool> completion)
        {
            Exception failure = null;
            try
            {
                await StartDisconnectCoreAsync(session).ConfigureAwait(false);
            }
            catch (Exception exception)
            {
                failure = exception;
                logger.Error(exception, "MQTT disconnect attempt ended unexpectedly.");
            }
            finally
            {
                CompleteMqttDisconnect(session, completion.Task);
            }

            if (failure == null)
            {
                completion.TrySetResult(true);
            }
            else
            {
                completion.TrySetException(failure);
            }
        }

        private async Task StartDisconnectCoreAsync(MqttSession session)
        {
            // A settings apply or user disconnect can arrive while ConnectAsync is
            // pending. Cancel and await that attempt before a second lifecycle
            // operation touches MQTTnet, otherwise the new settings are ignored.
            if (session?.ConnectionTask != null)
            {
                try
                {
                    await session.ConnectionTask.ConfigureAwait(false);
                }
                catch (OperationCanceledException) when (session.CancellationSource.IsCancellationRequested)
                {
                    // Disconnect intentionally retires a pending connection attempt.
                }
                catch (Exception exception)
                {
                    logger.Warn(exception, "MQTT connection attempt faulted while disconnecting.");
                }
            }

            if (session != null)
            {
                await WaitForMqttSessionWorkAsync(session).ConfigureAwait(false);
            }

            await mqttLifecycleGate.WaitAsync().ConfigureAwait(false);
            try
            {
                if (!client.IsConnected)
                {
                    return;
                }

                try
                {
                    var deviceId = session?.DeviceId;
                    if (!string.IsNullOrWhiteSpace(deviceId))
                    {
                        var connectionTopic = $"playnite/{deviceId}/{Topics.ConnectionSubTopic}";
                        await client.PublishStringAsync(
                            connectionTopic,
                            "offline",
                            MqttQualityOfServiceLevel.AtLeastOnce,
                            retain: true).ConfigureAwait(false);
                    }
                }
                catch (Exception exception)
                {
                    // An unavailable broker must not prevent a local disconnect.
                    logger.Warn(exception, "Failed to publish MQTT offline status before disconnecting.");
                }

                try
                {
                    await client.DisconnectAsync().ConfigureAwait(false);
                }
                catch (Exception exception)
                {
                    logger.Warn(exception, "Failed to disconnect MQTT client cleanly.");
                }
            }
            finally
            {
                mqttLifecycleGate.Release();
            }
        }

        private async Task NotifyWhenMqttDisconnectedAsync(Task disconnectTask)
        {
            await disconnectTask.ConfigureAwait(false);
            if (!client.IsConnected)
            {
                await RunOnPlayniteUiThreadAsync(() =>
                {
                    PlayniteApi.Dialogs.ShowMessage("MQTT Disconnected", "MQTT Status");
                    return true;
                }).ConfigureAwait(false);
            }
        }

        public Task<MqttClientConnectResult> StartConnectionTask(
            bool notifyCompletion,
            Action<float> reportProgress = null,
            CancellationToken cancellationToken = default)
        {
            // Install a task placeholder under the session lock before the
            // asynchronous core begins. A concurrent Disconnect can then cancel and
            // await this exact attempt instead of racing a second connection.
            var completion = new TaskCompletionSource<MqttClientConnectResult>();
            if (!TryStartMqttSession(cancellationToken, completion.Task, out var session))
            {
                logger.Debug("Ignoring MQTT connection request because the client is connected or its lifecycle is changing.");
                return Task.FromResult<MqttClientConnectResult>(null);
            }

            _ = CompleteMqttConnectionAttemptAsync(session, notifyCompletion, reportProgress, completion);
            return completion.Task;
        }

        private async Task CompleteMqttConnectionAttemptAsync(
            MqttSession session,
            bool notifyCompletion,
            Action<float> reportProgress,
            TaskCompletionSource<MqttClientConnectResult> completion)
        {
            try
            {
                completion.TrySetResult(await StartConnectionTaskCoreAsync(
                    session,
                    notifyCompletion,
                    reportProgress).ConfigureAwait(false));
            }
            catch (OperationCanceledException) when (session.CancellationSource.IsCancellationRequested)
            {
                completion.TrySetCanceled();
            }
            catch (Exception exception)
            {
                logger.Error(exception, "MQTT connection attempt ended unexpectedly.");
                completion.TrySetException(exception);
            }
        }

        private async Task<MqttClientConnectResult> StartConnectionTaskCoreAsync(
            MqttSession session,
            bool notifyCompletion,
            Action<float> reportProgress)
        {
            var lifecycleGateHeld = false;
            try
            {
                // If the broker dropped unexpectedly, a caller can request a new
                // connection before an older snapshot or cover operation has fully
                // unwound. Do not let that old publish overlap ConnectAsync.
                await WaitForMqttWorkOrCancellationAsync(
                    session.RetiredSessionWork,
                    session.CancellationSource.Token).ConfigureAwait(false);
                await mqttLifecycleGate.WaitAsync(session.CancellationSource.Token).ConfigureAwait(false);
                lifecycleGateHeld = true;
                if (!IsCurrentMqttSession(session) || Volatile.Read(ref isDisconnecting) != 0)
                {
                    return null;
                }

                // Build every broker option inside this try block. Password decryption and
                // certificate loading can fail before ConnectAsync begins, especially after
                // a user changes credentials or certificate files in the settings page.
                var optionsUnBuilt = new MqttClientOptionsBuilder().WithClientId(settings.Settings.ClientId)
                    .WithTcpServer(settings.Settings.ServerAddress, settings.Settings.Port)
                    .WithCredentials(settings.Settings.Username, LoadPassword())
                    .WithCleanSession().WithKeepAlivePeriod(TimeSpan.FromSeconds(5));

                // Publish retained offline if this Playnite process exits without a
                // clean MQTT disconnect (for example a crash, force-close, or lost
                // network connection). The Companion maps this into its
                // connectivity binary sensor for reliable automation conditions.
                if (!string.IsNullOrWhiteSpace(session.DeviceId))
                {
                    var connectionTopic = $"playnite/{session.DeviceId}/{Topics.ConnectionSubTopic}";
                    optionsUnBuilt = optionsUnBuilt
                        .WithWillTopic(connectionTopic)
                        .WithWillPayload("offline")
                        .WithWillQualityOfServiceLevel(MqttQualityOfServiceLevel.AtLeastOnce)
                        .WithWillRetain(true);
                }

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
                topicHelper.SetActiveDeviceId(session.DeviceId);
                reportProgress?.Invoke(0.1f);
                var connectionResult = await client.ConnectAsync(options, session.CancellationSource.Token).ConfigureAwait(false);
                session.CancellationSource.Token.ThrowIfCancellationRequested();
                if (session.ConnectionSetupException != null || !IsMqttSessionReady(session))
                {
                    // ConnectAsync completed, but the ConnectedAsync setup did not:
                    // do not leave a broker connection that has no subscriptions,
                    // initial library snapshot, or retained online availability.
                    try
                    {
                        if (client.IsConnected)
                        {
                            await client.DisconnectAsync().ConfigureAwait(false);
                        }
                    }
                    catch (Exception disconnectException)
                    {
                        logger.Warn(disconnectException, "Failed to clean up MQTT after connection setup failed.");
                    }

                    throw new InvalidOperationException(
                        "MQTT connected, but Playnite Connect could not finish its initial setup.",
                        session.ConnectionSetupException);
                }

                if (notifyCompletion && client.IsConnected)
                {
                    await RunOnPlayniteUiThreadAsync(() =>
                    {
                        PlayniteApi.Dialogs.ShowMessage("MQTT Connected", "MQTT Status");
                        return true;
                    }, session.CancellationSource.Token).ConfigureAwait(false);
                }

                var notificationsEnabled = await RunOnPlayniteUiThreadAsync(
                    () => settings.Settings.Notifications,
                    session.CancellationSource.Token).ConfigureAwait(false);
                if (notificationsEnabled && client.IsConnected)
                {
                    await RunOnPlayniteUiThreadAsync(() =>
                    {
                        PlayniteApi.Notifications.Add(
                            new NotificationMessage(
                                Guid.NewGuid().ToString(),
                                DateTime.Now.ToString("dd/MM/yyyy HH:mm:ss") + "\nMQTT Connected",
                                NotificationType.Info));
                        return true;
                    }, session.CancellationSource.Token).ConfigureAwait(false);
                }
                if (client.IsConnected)
                {
                    logger.Debug("MQTT Connected");
                }

                return connectionResult;
            }
            catch (OperationCanceledException) when (session.CancellationSource.IsCancellationRequested)
            {
                logger.Debug("MQTT connection attempt was cancelled.");
                ReportDisconnectedConnectionAttempt();
            }
            catch (Exception exception)
            {
                logger.Error(exception, "Failed to connect to MQTT.");
                ReportDisconnectedConnectionAttempt();
                if (!applicationClosingCompletionSource.IsCancellationRequested &&
                    Volatile.Read(ref isDisconnecting) == 0)
                {
                    try
                    {
                        await RunOnPlayniteUiThreadAsync(() =>
                        {
                            PlayniteApi.Dialogs.ShowErrorMessage($"MQTT: {exception.Message}", "MQTT Error");
                            return true;
                        }).ConfigureAwait(false);
                    }
                    catch (Exception uiException)
                    {
                        logger.Warn(uiException, "Failed to display the MQTT connection error.");
                    }
                }
            }
            finally
            {
                if (lifecycleGateHeld)
                {
                    mqttLifecycleGate.Release();
                }

                Volatile.Write(ref isConnecting, 0);
            }

            return null;
        }
        private void ReportDisconnectedConnectionAttempt()
        {
            if (Volatile.Read(ref isStopping) != 0)
            {
                return;
            }

            // ConnectAsync can fail or be cancelled before MQTTnet emits a
            // DisconnectedAsync event. Keep the sidebar and HA-facing state honest
            // for that pending-connection path as well as ordinary disconnects.
            ReportConnectionState(ConnectionState.Disconnected);
            ReportSidebarProgress(-1);
        }
        public GlobalProgressResult StartConnection(bool notifyCompletion = false)
        {
            // This method is also called after a power-resume event. Keep the
            // dialog and notification APIs on Playnite's dispatcher regardless of
            // which thread requested the reconnect.
            return RunOnPlayniteUiThread(() => StartConnectionOnUiThread(notifyCompletion));
        }

        private GlobalProgressResult StartConnectionOnUiThread(bool notifyCompletion)
        {
            if (client.IsConnected || Volatile.Read(ref isConnecting) != 0)
            {
                PlayniteApi.Notifications.Add(
                    new NotificationMessage(Guid.NewGuid().ToString(), "Connection to MQTT underway", NotificationType.Error));
                throw new Exception("Connection to MQTT underway");
            }

            ReportConnectionState(ConnectionState.Connecting);
            return PlayniteApi.Dialogs.ActivateGlobalProgress(
                async args =>
                {
                    args.ProgressMaxValue = 1;
                    args.CurrentProgressValue = 0;
                    await StartConnectionTask(notifyCompletion, ReportSidebarProgress, args.CancelToken);
                    args.CurrentProgressValue = 1;
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
            if (client.IsConnected || Volatile.Read(ref isConnecting) != 0)
            {
                RunUiOperation(() => DisconnectFromUiAsync(settings.Settings.ShowStatusChanged), "disconnect MQTT from the sidebar");
                return;
            }

            StartConnection(settings.Settings.ShowStatusChanged);
        }

        private async Task DisconnectFromUiAsync(bool notifyCompletion)
        {
            await StartDisconnect(notifyCompletion);
            if (settings.Settings.Notifications && !client.IsConnected)
            {
                await RunOnPlayniteUiThreadAsync(() =>
                {
                    PlayniteApi.Notifications.Add(
                        new NotificationMessage(
                            Guid.NewGuid().ToString(),
                            DateTime.Now.ToString("dd/MM/yyyy HH:mm:ss") + "\nMQTT Disconnected",
                            NotificationType.Info));
                    return true;
                });
            }
        }
        private async Task ClientOnConnectedAsync(EventArgs eventArgs)
        {
            var session = GetCurrentMqttSession();
            if (session == null ||
                Volatile.Read(ref isStopping) != 0 ||
                session.CancellationSource.IsCancellationRequested)
            {
                return;
            }

            var cancellationToken = session.CancellationSource.Token;
            try
            {
                ReportSidebarProgress(0.6f);
                // Subscribe before declaring Playnite available. With a clean MQTT
                // session, HA can react to the availability sensor immediately; an
                // earlier retained online message could otherwise make it send a
                // command before this client was listening for it.
                await SubscribeToLibraryProtocolAsync(cancellationToken).ConfigureAwait(false);
                await PublishLibrarySnapshotAsync(null, cancellationToken).ConfigureAwait(false);
                if (!TryMarkMqttSessionReady(session))
                {
                    return;
                }

                if (topicHelper.TryGetTopic(Topics.ConnectionSubTopic, out var connectionTopic))
                {
                    await client.PublishStringAsync(
                        connectionTopic,
                        "online",
                        MqttQualityOfServiceLevel.AtLeastOnce,
                        retain: true,
                        cancellationToken: cancellationToken).ConfigureAwait(false);
                }

                ReportSidebarProgress(1f);
                ReportConnectionState(ConnectionState.Connected);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                // A disconnect or shutdown won the race with connection setup.
            }
            catch (Exception exception)
            {
                // ConnectAsync awaits this handler. Preserve the cause so its caller
                // can cleanly tear the client down instead of leaving it connected
                // without its command subscriptions or availability state.
                session.ConnectionSetupException = exception;
                logger.Error(exception, "Failed to finish the MQTT connected callback.");
            }
        }
        private void DisconnectMenuAction(MainMenuItemActionArgs obj)
        {
            RunUiOperation(() => DisconnectFromUiAsync(settings.Settings.ShowStatusChanged), "disconnect MQTT from the menu");
        }

        private void ReconnectMenuAction(MainMenuItemActionArgs obj)
        {
            RunUiOperation(ReconnectAsync, "reconnect MQTT from the menu");
        }

        private async Task ReconnectAsync()
        {
            await StartDisconnect();
            StartConnection(true);
        }
        private Task ClientOnDisconnectedAsync(EventArgs eventArgs)
        {
            MarkCurrentMqttSessionUnavailable();
            ReportConnectionState(ConnectionState.Disconnected);
            ReportSidebarProgress(-1);

            logger.Debug("MQTT client disconnected.");
            if (Volatile.Read(ref isStopping) == 0 &&
                Volatile.Read(ref isDisconnecting) == 0 &&
                Volatile.Read(ref lastPowerMode) == (int)PowerModes.Resume)
            {
                Volatile.Write(ref lastPowerMode, 0);
                logger.Debug("Last power mode is Resume. Connecting...");
                RunBackground(async () =>
                {
                    ReportSidebarProgress(0);
                    try
                    {
                        await StartConnectionTask(false, cancellationToken: applicationClosingCompletionSource.Token).ConfigureAwait(false);
                        if (client.IsConnected)
                        {
                            logger.Debug("MQTT client reconnected after disconnect on power resume.");
                        }
                    }
                    finally
                    {
                        ReportSidebarProgress(1);
                    }
                }, "reconnect MQTT after a power-resume event");
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

        private Task ClientOnApplicationMessageReceivedAsync(MqttApplicationMessageReceivedEventArgs args)
        {
            if (Volatile.Read(ref isStopping) != 0 || applicationClosingCompletionSource.IsCancellationRequested)
            {
                return Task.CompletedTask;
            }
            if (!topicHelper.TryGetTopic(Topics.LibraryRequestSubTopic, out var requestTopic) ||
                !topicHelper.TryGetTopic(Topics.LibraryCommandSubTopic, out var commandTopic) ||
                !topicHelper.TryGetTopic(Topics.LibraryCoverRequestSubTopic, out var coverRequestTopic))
            {
                return Task.CompletedTask;
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
                    // MQTTnet awaits this callback before acknowledging and processing
                    // another PUBLISH. Snapshot chunks use QoS 1, so publish them from
                    // a worker rather than blocking the receive loop on their ACKs.
                    RunSessionBackground(
                        cancellationToken => PublishLibrarySnapshotAsync(request.RequestId, cancellationToken),
                        "publish a requested Playnite library snapshot");
                }
                else if (string.Equals(messageTopic, commandTopic, StringComparison.Ordinal))
                {
                    var command = JsonConvert.DeserializeObject<LibraryCommand>(payload);
                    if (command == null)
                    {
                        throw new InvalidOperationException("Library command payload is empty.");
                    }

                    // Commands can publish QoS 1 responses and may wait for a game
                    // process to close. Keep that work outside MQTTnet's serialized
                    // receive callback; the single FIFO worker preserves command order.
                    QueueLibraryCommand(command);
                }
                else if (string.Equals(messageTopic, coverRequestTopic, StringComparison.Ordinal))
                {
                    var request = JsonConvert.DeserializeObject<LibraryCoverRequest>(payload);
                    if (request == null)
                    {
                        throw new InvalidOperationException("Cover request payload is empty.");
                    }

                    QueueCoverPublish(request);
                }
            }
            catch (Exception exception)
            {
                logger.Error(exception, "Failed to process a Playnite library MQTT message.");
            }

            return Task.CompletedTask;
        }
        private void StartCoverTransferWorkers()
        {
            var cancellationToken = applicationClosingCompletionSource.Token;
            for (var index = 0; index < CoverTransferWorkerCount; index++)
            {
                RunBackground(() => ProcessCoverTransferQueueAsync(cancellationToken), "process queued cover transfers");
            }
        }

        private void StartLibraryCommandWorker()
        {
            RunBackground(
                () => ProcessLibraryCommandQueueAsync(applicationClosingCompletionSource.Token),
                "process queued Playnite library commands");
        }

        private void QueueLibraryCommand(LibraryCommand command)
        {
            lock (mqttSessionLock)
            {
                var session = currentMqttSession;
                if (session == null ||
                    !session.IsReady ||
                    session.CancellationSource.IsCancellationRequested ||
                    Volatile.Read(ref isDisconnecting) != 0 ||
                    Volatile.Read(ref isStopping) != 0)
                {
                    return;
                }

                // Queue admission and its operation lease share the retirement lock.
                // Shutdown can therefore either drain this item or reject it, never
                // strand a lease after the workers have been cancelled.
                session.ActiveExternalOperations++;
                libraryCommandQueue.Enqueue(new LibraryCommandWorkItem
                {
                    Command = command,
                    Session = session
                });
                libraryCommandSignal.Release();
            }
        }

        private async Task ProcessLibraryCommandQueueAsync(CancellationToken cancellationToken)
        {
            try
            {
                while (true)
                {
                    await libraryCommandSignal.WaitAsync(cancellationToken).ConfigureAwait(false);
                    if (!libraryCommandQueue.TryDequeue(out var workItem))
                    {
                        continue;
                    }

                    try
                    {
                        if (!IsMqttSessionReady(workItem.Session))
                        {
                            continue;
                        }

                        await HandleLibraryCommandAsync(
                            workItem.Command,
                            workItem.Session.CancellationSource.Token).ConfigureAwait(false);
                    }
                    catch (OperationCanceledException) when (workItem.Session.CancellationSource.IsCancellationRequested)
                    {
                        // This queued command belongs to a retired MQTT session.
                    }
                    catch (Exception exception)
                    {
                        logger.Error(exception, "Failed to process a queued Playnite library command.");
                    }
                    finally
                    {
                        ReleaseMqttSessionWork(workItem.Session);
                    }
                }
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                // Playnite is stopping; queued commands are no longer actionable.
            }
        }

        private void QueueCoverPublish(LibraryCoverRequest request)
        {
            lock (mqttSessionLock)
            {
                var session = currentMqttSession;
                if (session == null ||
                    !session.IsReady ||
                    session.CancellationSource.IsCancellationRequested ||
                    Volatile.Read(ref isDisconnecting) != 0 ||
                    Volatile.Read(ref isStopping) != 0)
                {
                    return;
                }

                // Finish MQTTnet's receive callback before publishing QoS 1 chunks,
                // but make the queued request atomic with session retirement so it
                // cannot outlive shutdown without releasing its lease.
                session.ActiveExternalOperations++;
                coverTransferQueue.Enqueue(new LibraryCoverWorkItem
                {
                    Request = request,
                    Session = session
                });
                coverTransferSignal.Release();
            }
        }

        private async Task ProcessCoverTransferQueueAsync(CancellationToken cancellationToken)
        {
            try
            {
                while (true)
                {
                    await coverTransferSignal.WaitAsync(cancellationToken).ConfigureAwait(false);
                    if (!coverTransferQueue.TryDequeue(out var workItem))
                    {
                        continue;
                    }

                    try
                    {
                        if (!IsMqttSessionReady(workItem.Session))
                        {
                            continue;
                        }

                        await PublishCoverAsync(
                            workItem.Request,
                            workItem.Session.CancellationSource.Token).ConfigureAwait(false);
                    }
                    catch (OperationCanceledException) when (workItem.Session.CancellationSource.IsCancellationRequested)
                    {
                        // This queued cover belongs to a retired MQTT session.
                    }
                    catch (Exception exception)
                    {
                        logger.Error(exception, "Failed to publish a requested Playnite cover.");
                        try
                        {
                            await PublishCoverErrorAsync(
                                workItem.Request,
                                "Playnite could not transfer this image.",
                                workItem.Session.CancellationSource.Token).ConfigureAwait(false);
                        }
                        catch (OperationCanceledException) when (workItem.Session.CancellationSource.IsCancellationRequested)
                        {
                            // The session ended before its error response could be sent.
                        }
                        catch (Exception responseException)
                        {
                            logger.Error(responseException, "Failed to report a Playnite cover transfer error.");
                        }
                    }
                    finally
                    {
                        ReleaseMqttSessionWork(workItem.Session);
                    }
                }
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                // Playnite is stopping; no broker response is expected at shutdown.
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
                var games = await RunOnPlayniteUiThreadAsync(
                    () => PlayniteApi.Database.Games
                        .Select(game => new LibraryGameData(game))
                        .OrderBy(game => game.Name ?? string.Empty, StringComparer.OrdinalIgnoreCase)
                        .ToList(),
                    cancellationToken);
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

            var status = await RunOnPlayniteUiThreadAsync(() =>
            {
                var selectedGame = PlayniteApi.MainView.SelectedGames?.FirstOrDefault();
                return new LibraryStatus
                {
                    SelectedGameId = selectedGame?.Id.ToString(),
                    ActiveDesktopView = PlayniteApi.MainView.ActiveDesktopView.ToString(),
                    PlayniteVersion = PlayniteApi.ApplicationInfo.ApplicationVersion.ToString()
                };
            }, cancellationToken);
            await client.PublishStringAsync(
                topic,
                serializer.Serialize(status),
                MqttQualityOfServiceLevel.AtLeastOnce,
                true,
                cancellationToken);
        }
        private async Task PublishLibraryUpdateAsync(LibraryGameData game, string eventName, CancellationToken cancellationToken)
        {
            if (!topicHelper.TryGetTopic(Topics.LibraryUpdateSubTopic, out var topic))
            {
                return;
            }

            await client.PublishStringAsync(
                topic,
                serializer.Serialize(new LibraryUpdate { Event = eventName, Game = game }),
                MqttQualityOfServiceLevel.AtLeastOnce,
                false,
                cancellationToken);
        }

        private void QueueLibraryUpdate(Game game, string eventName)
        {
            if (game == null || applicationClosingCompletionSource.IsCancellationRequested)
            {
                return;
            }

            LibraryGameData gameData;
            try
            {
                // Copy the event game while on Playnite's dispatcher. The MQTT work
                // below only serializes this detached protocol object.
                gameData = RunOnPlayniteUiThread(() => new LibraryGameData(game));
            }
            catch (Exception exception)
            {
                logger.Error(exception, $"Failed to snapshot the game for Playnite event {eventName}.");
                return;
            }

            RunSessionBackground(
                cancellationToken => PublishLibraryUpdateAsync(gameData, eventName, cancellationToken),
                "publish a Playnite library update");
        }
        private async Task PublishCoverAsync(LibraryCoverRequest request, CancellationToken cancellationToken)
        {
            if (!Guid.TryParse(request.GameId, out var gameId))
            {
                await PublishCoverErrorAsync(request, "gameId must be a Playnite game GUID.", cancellationToken);
                return;
            }

            var imageType = string.IsNullOrWhiteSpace(request.ImageType)
                ? LibraryProtocol.ImageTypeCover
                : request.ImageType.Trim().ToLowerInvariant();
            if (imageType != LibraryProtocol.ImageTypeCover &&
                imageType != LibraryProtocol.ImageTypeBackground &&
                imageType != LibraryProtocol.ImageTypeIcon)
            {
                await PublishCoverErrorAsync(request, "imageType must be cover, background, or icon.", cancellationToken);
                return;
            }

            LibraryImageResolution image;
            try
            {
                // Look up Playnite-owned game and image metadata on its dispatcher.
                // Disk access and MQTT chunk publishing remain in this worker.
                image = await RunOnPlayniteUiThreadAsync(
                    () => ResolveLibraryImageOnUiThread(gameId, imageType),
                    cancellationToken);
            }
            catch (Exception exception)
            {
                logger.Error(exception, $"Failed to resolve Playnite {imageType} image.");
                await PublishCoverErrorAsync(request, $"This game has no accessible {imageType} image.", cancellationToken);
                return;
            }

            if (!image.GameFound)
            {
                await PublishCoverErrorAsync(request, "The requested game no longer exists in Playnite.", cancellationToken);
                return;
            }

            if (string.IsNullOrWhiteSpace(image.ImagePath) || !File.Exists(image.ImagePath))
            {
                await PublishCoverErrorAsync(request, $"This game has no accessible {imageType} image.", cancellationToken);
                return;
            }

            var fileInfo = new FileInfo(image.ImagePath);
            if (fileInfo.Length > LibraryProtocol.MaximumCoverBytes)
            {
                await PublishCoverErrorAsync(request, $"This Playnite {imageType} image exceeds the 10 MB transfer limit.", cancellationToken);
                return;
            }

            byte[] data;
            try
            {
                data = File.ReadAllBytes(image.ImagePath);
            }
            catch (Exception exception)
            {
                logger.Error(exception, $"Failed to read Playnite {imageType} image for {image.GameName}.");
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
                    ContentType = GetImageContentType(image.ImagePath),
                    Data = Convert.ToBase64String(chunkData)
                }, cancellationToken);
            }
        }

        private LibraryImageResolution ResolveLibraryImageOnUiThread(Guid gameId, string imageType)
        {
            var game = PlayniteApi.Database.Games.FirstOrDefault(item => item.Id == gameId);
            if (game == null)
            {
                return new LibraryImageResolution();
            }

            string imageReference;
            switch (imageType)
            {
                case LibraryProtocol.ImageTypeBackground:
                    imageReference = game.BackgroundImage;
                    break;
                case LibraryProtocol.ImageTypeIcon:
                    imageReference = game.Icon;
                    break;
                default:
                    imageReference = game.CoverImage;
                    break;
            }

            return new LibraryImageResolution
            {
                GameFound = true,
                GameName = game.Name,
                ImagePath = string.IsNullOrWhiteSpace(imageReference)
                    ? null
                    : PlayniteApi.Database.GetFullFilePath(imageReference)
            };
        }

        private sealed class LibraryImageResolution
        {
            public bool GameFound { get; set; }
            public string GameName { get; set; }
            public string ImagePath { get; set; }
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

                var action = (command.Action ?? string.Empty).Trim().ToLowerInvariant();
                switch (action)
                {
                    case "stop":
                        var stopTarget = await RunOnPlayniteUiThreadAsync(
                            () => CreateGameStopTargetOnUiThread(gameId),
                            cancellationToken);
                        var stopResult = await StopGameAsync(stopTarget, cancellationToken);
                        if (stopResult.ProcessCount == 0)
                        {
                            throw new InvalidOperationException(stopResult.FailureMessage);
                        }
                        response.Message = stopResult.Message;
                        break;
                    case "restart":
                        var restartTarget = await RunOnPlayniteUiThreadAsync(
                            () => CreateGameStopTargetOnUiThread(gameId),
                            cancellationToken);
                        var restartStopResult = await StopGameAsync(restartTarget, cancellationToken);
                        if (restartStopResult.ProcessCount == 0)
                        {
                            throw new InvalidOperationException(restartStopResult.FailureMessage);
                        }
                        await Task.Delay(TimeSpan.FromSeconds(3), cancellationToken);
                        await RunOnPlayniteUiThreadAsync(() =>
                        {
                            PlayniteApi.StartGame(gameId);
                            return true;
                        }, cancellationToken);
                        response.Message = $"{restartStopResult.Message} Restart requested.";
                        break;
                    default:
                        response.Message = await RunOnPlayniteUiThreadAsync(
                            () => ExecuteLibraryCommandOnUiThread(gameId, action),
                            cancellationToken);
                        break;
                }

                response.Success = true;
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                // The session is closing, so no command response should be published.
                return;
            }
            catch (Exception exception)
            {
                response.Success = false;
                response.Message = exception.Message;
                logger.Error(exception, $"Failed Playnite library command {command.Action} for {command.GameId}.");
            }

            await PublishLibraryResponseAsync(response, cancellationToken);
        }

        private string ExecuteLibraryCommandOnUiThread(Guid gameId, string action)
        {
            var game = PlayniteApi.Database.Games.FirstOrDefault(item => item.Id == gameId);
            if (game == null)
            {
                throw new InvalidOperationException("The requested game no longer exists in Playnite.");
            }

            switch (action)
            {
                case "select":
                    // SelectGame is a real public Playnite API call, unlike the
                    // legacy MQTT Discovery select topic which had no handler.
                    PlayniteApi.MainView.SelectGame(gameId);
                    return "Game selected in Playnite.";
                case "start":
                    if (game.IsInstalled)
                    {
                        PlayniteApi.StartGame(gameId);
                        return "Start requested.";
                    }

                    PlayniteApi.InstallGame(gameId);
                    return "Game is not installed; install requested.";
                case "install":
                    if (!game.IsInstalled)
                    {
                        PlayniteApi.InstallGame(gameId);
                        return "Install requested.";
                    }

                    return "Game is already installed.";
                case "uninstall":
                    if (game.IsInstalled)
                    {
                        PlayniteApi.UninstallGame(gameId);
                        return "Uninstall requested.";
                    }

                    return "Game is already uninstalled.";
                default:
                    throw new InvalidOperationException("Unsupported action. Use select, start, stop, restart, install, or uninstall.");
            }
        }

        private GameStopTarget CreateGameStopTargetOnUiThread(Guid gameId)
        {
            var game = PlayniteApi.Database.Games.FirstOrDefault(item => item.Id == gameId);
            if (game == null)
            {
                throw new InvalidOperationException("The requested game no longer exists in Playnite.");
            }

            return new GameStopTarget
            {
                InstallDirectory = game.InstallDirectory,
                EmulatorExecutablePath = GetEmulatorExecutablePathOnUiThread(game)
            };
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

        public void ApplySettings()
        {
            RunUiOperation(ApplySettingsAsync, "apply Playnite Connect settings");
        }

        private async Task ApplySettingsAsync()
        {
            // Settings are already persisted by the view model when this runs.
            // Awaiting here keeps a reconnect from racing an active disconnect.
            await StartDisconnect().ConfigureAwait(false);
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
        private async Task<StopGameResult> StopGameAsync(GameStopTarget game, CancellationToken cancellationToken)
        {
            var emulatorExecutablePath = game.EmulatorExecutablePath;
            if (!string.IsNullOrEmpty(emulatorExecutablePath))
            {
                var gracefulStopCount = RequestGracefulProcessStop(emulatorExecutablePath);
                if (gracefulStopCount == 0)
                {
                    return StopGameResult.Failed("No running emulator process could be matched safely to this game's configured emulator.");
                }

                // Windows has no portable SIGTERM equivalent. CloseMainWindow sends the
                // emulator a normal WM_CLOSE request so it can save state and run its
                // own cleanup instead of being forcibly terminated.
                if (!await WaitForProcessesToExitAsync(emulatorExecutablePath, cancellationToken))
                {
                    return StopGameResult.Failed("The emulator received a graceful stop request but was still running after 10 seconds.");
                }

                return StopGameResult.Succeeded(gracefulStopCount, "Stopped the emulator cleanly.");
            }

            var stoppedCount = StopGameProcesses(game.InstallDirectory);
            return stoppedCount > 0
                ? StopGameResult.Succeeded(stoppedCount, $"Stopped {stoppedCount} game process(es).")
                : StopGameResult.Failed("No running process could be matched safely to this game's install directory.");
        }

        private string GetEmulatorExecutablePathOnUiThread(Game game)
        {
            var action = game.GameActions?.FirstOrDefault(item =>
                item.IsPlayAction && item.Type == GameActionType.Emulator);
            if (action == null)
            {
                return null;
            }

            var emulator = PlayniteApi.Database.Emulators.FirstOrDefault(item => item.Id == action.EmulatorId);
            if (emulator == null)
            {
                logger.Warn($"Unable to resolve the emulator configured for {game.Name}.");
                return null;
            }

            var profile = emulator.GetProfile(action.EmulatorProfileId);
            string executable = null;
            if (profile is CustomEmulatorProfile customProfile)
            {
                executable = customProfile.Executable;
            }
            else if (profile is BuiltInEmulatorProfile builtInProfile)
            {
                var definition = PlayniteApi.Emulation.GetEmulator(emulator.BuiltInConfigId);
                executable = definition?.Profiles
                    .FirstOrDefault(item => item.Name == builtInProfile.BuiltInProfileName)
                    ?.StartupExecutable;
            }

            if (string.IsNullOrWhiteSpace(executable))
            {
                logger.Warn($"The emulator profile configured for {game.Name} does not expose an executable path.");
                return null;
            }

            executable = PlayniteApi.ExpandGameVariables(game, executable, emulator.InstallDir);
            if (!Path.IsPathRooted(executable))
            {
                executable = Path.Combine(emulator.InstallDir ?? string.Empty, executable);
            }

            return File.Exists(executable) ? Path.GetFullPath(executable) : null;
        }

        private static int RequestGracefulProcessStop(string executablePath)
        {
            var stoppedCount = 0;
            foreach (var process in Process.GetProcesses())
            {
                try
                {
                    if (!string.Equals(process.MainModule?.FileName, executablePath, StringComparison.OrdinalIgnoreCase))
                    {
                        continue;
                    }

                    if (process.CloseMainWindow())
                    {
                        stoppedCount++;
                    }
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

        private static async Task<bool> WaitForProcessesToExitAsync(string executablePath, CancellationToken cancellationToken)
        {
            var deadline = DateTime.UtcNow.AddSeconds(10);
            while (DateTime.UtcNow < deadline)
            {
                var isStillRunning = Process.GetProcesses().Any(process =>
                {
                    try
                    {
                        return string.Equals(process.MainModule?.FileName, executablePath, StringComparison.OrdinalIgnoreCase);
                    }
                    catch (Exception)
                    {
                        return false;
                    }
                    finally
                    {
                        process.Dispose();
                    }
                });

                if (!isStillRunning)
                {
                    return true;
                }

                await Task.Delay(TimeSpan.FromMilliseconds(250), cancellationToken);
            }

            return false;
        }

        private static int StopGameProcesses(string installDirectory)
        {
            if (string.IsNullOrWhiteSpace(installDirectory) || !Directory.Exists(installDirectory))
            {
                throw new InvalidOperationException("This game has no accessible install directory, so it cannot be stopped safely.");
            }

            installDirectory = Path.GetFullPath(installDirectory)
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

        private sealed class MqttSession
        {
            public MqttSession(string deviceId, CancellationTokenSource cancellationSource)
            {
                DeviceId = deviceId;
                CancellationSource = cancellationSource;
            }

            public string DeviceId { get; }
            public CancellationTokenSource CancellationSource { get; }
            public Task<MqttClientConnectResult> ConnectionTask { get; set; }
            public Task RetiredSessionWork { get; set; } = Task.CompletedTask;
            public Exception ConnectionSetupException { get; set; }
            public bool IsReady { get; set; }
            public int ActiveExternalOperations { get; set; }
            public TaskCompletionSource<bool> ExternalOperationsDrained { get; } =
                new TaskCompletionSource<bool>();
        }
        private sealed class LibraryCommandWorkItem
        {
            public LibraryCommand Command { get; set; }
            public MqttSession Session { get; set; }
        }

        private sealed class LibraryCoverWorkItem
        {
            public LibraryCoverRequest Request { get; set; }
            public MqttSession Session { get; set; }
        }
        private sealed class GameStopTarget
        {
            public string InstallDirectory { get; set; }
            public string EmulatorExecutablePath { get; set; }
        }
        private sealed class StopGameResult
        {
            public int ProcessCount { get; }
            public string Message { get; }
            public string FailureMessage { get; }

            private StopGameResult(int processCount, string message, string failureMessage)
            {
                ProcessCount = processCount;
                Message = message;
                FailureMessage = failureMessage;
            }

            public static StopGameResult Succeeded(int processCount, string message)
            {
                return new StopGameResult(processCount, message, null);
            }

            public static StopGameResult Failed(string failureMessage)
            {
                return new StopGameResult(0, null, failureMessage);
            }
        }
        #region Overrides of Plugin

        public override Guid Id { get; } = Guid.Parse("b81e4a83-823d-4a83-89e8-aee39f17a483");

        public override void OnLibraryUpdated(OnLibraryUpdatedEventArgs args)
        {
            RunSessionBackground(
                cancellationToken => PublishLibrarySnapshotAsync(null, cancellationToken),
                "publish the updated Playnite library snapshot");
        }

        public override void OnGameSelected(OnGameSelectedEventArgs args)
        {
            // Playnite supplies the actual selection change; publish it for the
            // native HA select entity instead of pretending MQTT Discovery owns it.
            RunSessionBackground(
                cancellationToken => PublishLibraryStatusAsync(cancellationToken),
                "publish the selected-game status");
        }
        public override void Dispose()
        {
            if (BeginShutdown())
            {
                DisconnectDuringShutdown();
            }

            coverApiServer.Dispose();
            client.Dispose();
            base.Dispose();
        }

        public override void OnGameInstalled(OnGameInstalledEventArgs args)
        {
            QueueLibraryUpdate(args.Game, "game_installed");
        }

        public override void OnGameStarted(OnGameStartedEventArgs args)
        {
            QueueLibraryUpdate(args.Game, "game_started");
        }

        public override void OnGameStarting(OnGameStartingEventArgs args)
        {
            QueueLibraryUpdate(args.Game, "game_starting");
        }

        public override void OnGameStopped(OnGameStoppedEventArgs args)
        {
            QueueLibraryUpdate(args.Game, "game_stopped");
        }

        public override void OnGameUninstalled(OnGameUninstalledEventArgs args)
        {
            QueueLibraryUpdate(args.Game, "game_uninstalled");
        }

        public override void OnApplicationStarted(OnApplicationStartedEventArgs args)
        {
            client.ConnectedAsync += ClientOnConnectedAsync;
            client.ConnectingAsync += ClientOnConnectingAsync;
            client.DisconnectedAsync += ClientOnDisconnectedAsync;
            client.ApplicationMessageReceivedAsync += ClientOnApplicationMessageReceivedAsync;
            StartCoverTransferWorkers();
            StartLibraryCommandWorker();
            SystemEvents.PowerModeChanged += SystemEventsOnPowerModeChanged;
            RestartCoverApi();
            if (settings.Settings.ShowProgress)
            {
                StartConnection();
            }
            else
            {
                ReportSidebarProgress(0);
                RunBackground(async () =>
                {
                    try
                    {
                        await StartConnectionTask(false, cancellationToken: applicationClosingCompletionSource.Token).ConfigureAwait(false);
                    }
                    finally
                    {
                        ReportSidebarProgress(1);
                    }
                }, "connect MQTT when Playnite starts");
            }
        }

        private Task ClientOnConnectingAsync(MqttClientConnectingEventArgs arg)
        {
            ReportSidebarProgress(0.5f);
            return Task.CompletedTask;
        }

        private void SystemEventsOnPowerModeChanged(object sender, PowerModeChangedEventArgs e)
        {
            logger.Debug($"System power mode changed to: {e.Mode}");
            Volatile.Write(ref lastPowerMode, (int)e.Mode);
            if (e.Mode == PowerModes.Resume &&
                !client.IsConnected &&
                Volatile.Read(ref isStopping) == 0)
            {
                RequestConnectionOnPlayniteUiThread();
            }
        }

        private void RequestConnectionOnPlayniteUiThread()
        {
            try
            {
                var task = RunOnPlayniteUiThreadAsync(() =>
                {
                    if (!client.IsConnected && Volatile.Read(ref isStopping) == 0 && Volatile.Read(ref isDisconnecting) == 0)
                    {
                        StartConnection();
                    }

                    return true;
                });
                task.ContinueWith(
                    completed => logger.Error(completed.Exception, "Failed to start the MQTT reconnect from a power-resume event."),
                    TaskContinuationOptions.OnlyOnFaulted);
            }
            catch (Exception exception)
            {
                logger.Error(exception, "Failed to schedule the MQTT reconnect from a power-resume event.");
            }
        }

        private bool BeginShutdown()
        {
            if (Interlocked.Exchange(ref isStopping, 1) != 0)
            {
                return false;
            }

            // Stop accepting fresh broker work before shutting down the network
            // endpoints. StartDisconnect intentionally has no cancellation token so it
            // can still publish the retained offline state once during exit.
            applicationClosingCompletionSource.Cancel();
            client.ConnectedAsync -= ClientOnConnectedAsync;
            client.ConnectingAsync -= ClientOnConnectingAsync;
            client.DisconnectedAsync -= ClientOnDisconnectedAsync;
            client.ApplicationMessageReceivedAsync -= ClientOnApplicationMessageReceivedAsync;
            SystemEvents.PowerModeChanged -= SystemEventsOnPowerModeChanged;
            coverApiServer.Stop();
            return true;
        }

        private void DisconnectDuringShutdown()
        {
            // OnApplicationStopped is called on Playnite's UI thread. Run the MQTT
            // round-trip on a worker and wait only a bounded amount of time, otherwise
            // a broker outage could prevent Playnite itself from closing.
            var disconnectTask = Task.Run(() => StartDisconnect());
            try
            {
                var completedTask = Task.WhenAny(
                        disconnectTask,
                        Task.Delay(ShutdownDisconnectTimeoutMilliseconds))
                    .GetAwaiter()
                    .GetResult();
                if (completedTask != disconnectTask)
                {
                    logger.Warn("Timed out waiting for MQTT to disconnect during Playnite shutdown.");
                    disconnectTask.ContinueWith(
                        completed => logger.Warn(completed.Exception, "MQTT disconnect failed after the shutdown timeout."),
                        CancellationToken.None,
                        TaskContinuationOptions.OnlyOnFaulted,
                        TaskScheduler.Default);
                    return;
                }

                disconnectTask.GetAwaiter().GetResult();
            }
            catch (Exception exception)
            {
                logger.Warn(exception, "Failed to finish MQTT disconnect during Playnite shutdown.");
            }
        }
        public override void OnApplicationStopped(OnApplicationStoppedEventArgs args)
        {
            if (BeginShutdown())
            {
                DisconnectDuringShutdown();
            }
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
            return new PlayniteConnectSettingsView();
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
