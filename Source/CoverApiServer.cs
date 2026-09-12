using Playnite.SDK;
using Playnite.SDK.Models;
using System;
using System.IO;
using System.Linq;
using System.Net;
using System.Threading;

namespace MQTTClient
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
        private HttpListener listener;
        private Thread serverThread;

        public bool IsNetworkBound { get; private set; }
        public int Port { get; private set; }

        public CoverApiServer(IPlayniteAPI playniteApi, Func<string> getToken)
        {
            this.playniteApi = playniteApi;
            this.getToken = getToken;
        }

        public void Start(bool enabled, bool enableNetworkAccess, int port)
        {
            Stop();
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
            try
            {
                listener = new HttpListener();
                listener.Prefixes.Add(prefix);
                listener.Start();
                IsNetworkBound = enableNetworkAccess;
                serverThread = new Thread(ServerLoop) { IsBackground = true, Name = "Playnite Connect Cover API" };
                serverThread.Start();
                Logger.Info($"Playnite Connect cover API listening on {prefix}");
            }
            catch (Exception exception)
            {
                Logger.Error(exception, $"Failed to start Playnite Connect cover API on {prefix}.");
                Stop();
            }
        }

        public void Restart(bool enabled, bool enableNetworkAccess, int port)
        {
            Start(enabled, enableNetworkAccess, port);
        }

        public void Stop()
        {
            IsNetworkBound = false;
            try { listener?.Stop(); } catch { }
            try { listener?.Close(); } catch { }
            listener = null;
        }

        private void ServerLoop()
        {
            while (listener?.IsListening == true)
            {
                try
                {
                    var context = listener.GetContext();
                    ThreadPool.QueueUserWorkItem(_ => HandleRequest(context));
                }
                catch (HttpListenerException) { break; }
                catch (ObjectDisposedException) { break; }
                catch (Exception exception) { Logger.Error(exception, "Playnite Connect cover API listener error."); }
            }
        }

        private void HandleRequest(HttpListenerContext context)
        {
            var request = context.Request;
            var response = context.Response;
            try
            {
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

                var game = playniteApi.Database.Games.FirstOrDefault(item => item.Id == imageRequest.GameId);
                if (game == null)
                {
                    response.StatusCode = (int)HttpStatusCode.NotFound;
                    return;
                }

                var imageReference = GetImageReference(game, imageRequest.ImageType);
                if (string.IsNullOrWhiteSpace(imageReference))
                {
                    response.StatusCode = (int)HttpStatusCode.NotFound;
                    return;
                }

                var imagePath = playniteApi.Database.GetFullFilePath(imageReference);
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
            return !string.IsNullOrWhiteSpace(token)
                && !string.IsNullOrWhiteSpace(authorizationHeader)
                && authorizationHeader.StartsWith("Bearer ", StringComparison.OrdinalIgnoreCase)
                && string.Equals(authorizationHeader.Substring(7).Trim(), token, StringComparison.Ordinal);
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