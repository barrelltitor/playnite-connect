using Playnite.SDK;
using Playnite.SDK.Models;
using System;
using System.IO;
using System.Linq;
using System.Net;
using System.Threading;
using System.Text;

namespace PlayniteConnect
{
    /// <summary>
    /// Serves local Playnite cover files to Home Assistant. This listener is
    /// localhost-only unless the user explicitly enables network access.
    /// </summary>
    internal sealed class CoverApiServer : IDisposable
    {
        private const long MaximumCoverBytes = 25L * 1024 * 1024;
        private static readonly ILogger Logger = LogManager.GetLogger();
        private readonly IPlayniteAPI playniteApi;
        private readonly Func<string> getToken;
        private readonly object lifecycleLock = new object();
        private HttpListener listener;
        private CancellationTokenSource listenerCancellationSource;

        public bool IsNetworkBound { get; private set; }
        public int Port { get; private set; }

        public CoverApiServer(IPlayniteAPI playniteApi, Func<string> getToken)
        {
            this.playniteApi = playniteApi;
            this.getToken = getToken;
        }

        public void Start(bool enabled, bool enableNetworkAccess, int port)
        {
            lock (lifecycleLock)
            {
                StopListenerNoLock();
                Port = port;
                if (!enabled)
                {
                    Logger.Info("Playnite Connect cover API is disabled.");
                    return;
                }

                // A wildcard prefix is only used after the user has approved the
                // matching URL ACL and firewall rule in Enable Network Access.
                var prefix = enableNetworkAccess
                    ? $"http://+:{port}/"
                    : $"http://localhost:{port}/";
                var newListener = new HttpListener();
                var newListenerCancellationSource = new CancellationTokenSource();
                try
                {
                    newListener.Prefixes.Add(prefix);
                    newListener.Start();
                    listener = newListener;
                    listenerCancellationSource = newListenerCancellationSource;
                    IsNetworkBound = enableNetworkAccess;
                    var serverThread = new Thread(() => ServerLoop(newListener, newListenerCancellationSource.Token))
                    {
                        IsBackground = true,
                        Name = "Playnite Connect Cover API"
                    };
                    serverThread.Start();
                    Logger.Info($"Playnite Connect cover API listening on {prefix}");
                }
                catch (Exception exception)
                {
                    Logger.Error(exception, $"Failed to start Playnite Connect cover API on {prefix}.");
                    if (ReferenceEquals(listener, newListener))
                    {
                        listener = null;
                        listenerCancellationSource = null;
                    }

                    IsNetworkBound = false;
                    try { newListenerCancellationSource.Cancel(); } catch { }
                    try { newListener.Stop(); } catch { }
                    try { newListener.Close(); } catch { }
                }
            }
        }

        public void Restart(bool enabled, bool enableNetworkAccess, int port)
        {
            Start(enabled, enableNetworkAccess, port);
        }

        public void Stop()
        {
            lock (lifecycleLock)
            {
                StopListenerNoLock();
            }
        }

        // Each server loop owns the listener instance and cancellation lifetime it
        // started with. Restarting must not let an old loop serve the new listener,
        // and stale worker requests must not reach Playnite after Stop has begun.
        private void StopListenerNoLock()
        {
            IsNetworkBound = false;
            var activeListener = listener;
            var activeCancellationSource = listenerCancellationSource;
            listener = null;
            listenerCancellationSource = null;
            try { activeCancellationSource?.Cancel(); } catch { }
            try { activeListener?.Stop(); } catch { }
            try { activeListener?.Close(); } catch { }
        }

        private void ServerLoop(HttpListener activeListener, CancellationToken cancellationToken)
        {
            while (!cancellationToken.IsCancellationRequested && activeListener.IsListening)
            {
                try
                {
                    var context = activeListener.GetContext();
                    ThreadPool.QueueUserWorkItem(_ => HandleRequest(context, cancellationToken));
                }
                catch (HttpListenerException) { break; }
                catch (ObjectDisposedException) { break; }
                catch (Exception exception) { Logger.Error(exception, "Playnite Connect cover API listener error."); }
            }
        }

        private void HandleRequest(HttpListenerContext context, CancellationToken cancellationToken)
        {
            var request = context.Request;
            var response = context.Response;
            try
            {
                cancellationToken.ThrowIfCancellationRequested();
                if (!string.Equals(request.HttpMethod, "GET", StringComparison.OrdinalIgnoreCase))
                {
                    response.StatusCode = (int)HttpStatusCode.MethodNotAllowed;
                    return;
                }

                if (!HasValidToken(request.Headers["Authorization"]))
                {
                    response.StatusCode = (int)HttpStatusCode.Unauthorized;
                    response.Headers.Add("WWW-Authenticate", "Bearer");
                    return;
                }

                var imageRequest = GetImageRequest(request.Url?.AbsolutePath);
                if (imageRequest == null)
                {
                    response.StatusCode = (int)HttpStatusCode.NotFound;
                    return;
                }

                // The listener runs on its own thread. Playnite documents its SDK as
                // not fully thread-safe, so resolve the database-backed image path
                // on the UI dispatcher before streaming the file in this worker.
                var imagePath = ResolveImagePathOnPlayniteUiThread(imageRequest, cancellationToken);
                cancellationToken.ThrowIfCancellationRequested();
                if (string.IsNullOrWhiteSpace(imagePath) || !File.Exists(imagePath))
                {
                    response.StatusCode = (int)HttpStatusCode.NotFound;
                    return;
                }

                var imageInfo = new FileInfo(imagePath);
                if (imageInfo.Length > MaximumCoverBytes)
                {
                    response.StatusCode = (int)HttpStatusCode.RequestEntityTooLarge;
                    return;
                }

                response.StatusCode = (int)HttpStatusCode.OK;
                response.ContentType = GetImageContentType(imagePath);
                response.ContentLength64 = imageInfo.Length;
                response.Headers.Add("Cache-Control", "private, max-age=86400");
                using (var input = File.OpenRead(imagePath))
                {
                    input.CopyTo(response.OutputStream);
                }
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                // The API lifetime ended while this request waited on Playnite's UI.
            }
            catch (Exception exception)
            {
                Logger.Error(exception, $"Failed to serve Playnite cover {request.RawUrl}.");
                if (!response.OutputStream.CanWrite)
                {
                    return;
                }
                response.StatusCode = (int)HttpStatusCode.InternalServerError;
            }
            finally
            {
                try { response.OutputStream.Close(); } catch { }
            }
        }

        private bool HasValidToken(string authorizationHeader)
        {
            var token = getToken();
            if (string.IsNullOrWhiteSpace(token) ||
                string.IsNullOrWhiteSpace(authorizationHeader) ||
                !authorizationHeader.StartsWith("Bearer ", StringComparison.OrdinalIgnoreCase))
            {
                return false;
            }

            return FixedTimeEquals(authorizationHeader.Substring(7).Trim(), token);
        }

        private string ResolveImagePathOnPlayniteUiThread(ImageRequest imageRequest, CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var dispatcher = playniteApi.MainView.UIDispatcher;
            if (dispatcher.HasShutdownStarted || dispatcher.HasShutdownFinished)
            {
                return null;
            }

            if (dispatcher.CheckAccess())
            {
                cancellationToken.ThrowIfCancellationRequested();
                return ResolveImagePath(imageRequest);
            }

            return dispatcher.Invoke(() =>
            {
                // A queued HTTP request can outlive a Stop or Playnite shutdown.
                // Check the listener lifetime again before accessing Playnite data.
                cancellationToken.ThrowIfCancellationRequested();
                return ResolveImagePath(imageRequest);
            });
        }
        private string ResolveImagePath(ImageRequest imageRequest)
        {
            var game = playniteApi.Database.Games.FirstOrDefault(item => item.Id == imageRequest.GameId);
            if (game == null)
            {
                return null;
            }

            var imageReference = GetImageReference(game, imageRequest.ImageType);
            return string.IsNullOrWhiteSpace(imageReference)
                ? null
                : playniteApi.Database.GetFullFilePath(imageReference);
        }

        private static bool FixedTimeEquals(string provided, string expected)
        {
            var providedBytes = Encoding.UTF8.GetBytes(provided);
            var expectedBytes = Encoding.UTF8.GetBytes(expected);
            var difference = providedBytes.Length ^ expectedBytes.Length;
            var longestLength = Math.Max(providedBytes.Length, expectedBytes.Length);

            for (var index = 0; index < longestLength; index++)
            {
                var providedByte = index < providedBytes.Length ? providedBytes[index] : (byte)0;
                var expectedByte = index < expectedBytes.Length ? expectedBytes[index] : (byte)0;
                difference |= providedByte ^ expectedByte;
            }

            return difference == 0;
        }

        private sealed class ImageRequest
        {
            public Guid GameId { get; set; }
            public string ImageType { get; set; }
        }

        private static ImageRequest GetImageRequest(string path)
        {
            const string coverPrefix = "/api/covers/";
            if (!string.IsNullOrEmpty(path) && path.StartsWith(coverPrefix, StringComparison.OrdinalIgnoreCase) &&
                Guid.TryParse(path.Substring(coverPrefix.Length), out var coverGameId))
            {
                // Keep the original cover URL stable for existing HA entries.
                return new ImageRequest { GameId = coverGameId, ImageType = LibraryProtocol.ImageTypeCover };
            }

            const string imagePrefix = "/api/images/";
            if (string.IsNullOrEmpty(path) || !path.StartsWith(imagePrefix, StringComparison.OrdinalIgnoreCase))
            {
                return null;
            }
            var parts = path.Substring(imagePrefix.Length).Split('/');
            if (parts.Length != 2 || !Guid.TryParse(parts[1], out var gameId))
            {
                return null;
            }
            var imageType = parts[0].Trim().ToLowerInvariant();
            if (imageType != LibraryProtocol.ImageTypeCover &&
                imageType != LibraryProtocol.ImageTypeBackground &&
                imageType != LibraryProtocol.ImageTypeIcon)
            {
                return null;
            }
            return new ImageRequest { GameId = gameId, ImageType = imageType };
        }

        private static string GetImageReference(Game game, string imageType)
        {
            switch (imageType)
            {
                case LibraryProtocol.ImageTypeBackground:
                    return game.BackgroundImage;
                case LibraryProtocol.ImageTypeIcon:
                    return game.Icon;
                default:
                    return game.CoverImage;
            }
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

        public void Dispose()
        {
            Stop();
        }
    }
}