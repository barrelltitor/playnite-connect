using Newtonsoft.Json;
using Playnite.SDK.Models;
using System;
using System.Collections.Generic;
using System.Linq;

namespace MQTTClient
{
    /// <summary>
    /// Versioned messages used to synchronize a browseable Playnite library.
    /// Cover and background bytes are deliberately excluded from library sync.
    /// </summary>
    public static class LibraryProtocol
    {
        public const int Version = 1;
        public const int GamesPerChunk = 25;
        // Kept below conservative MQTT broker message-size limits after base64
        // encoding, while avoiding hundreds of tiny messages per cover.
        public const int CoverBytesPerChunk = 48 * 1024;
        public const int MaximumCoverBytes = 10 * 1024 * 1024;
        public const string ImageTypeCover = "cover";
        public const string ImageTypeBackground = "background";
        public const string ImageTypeIcon = "icon";
    }

    public class LibraryPlatformData
    {
        [JsonProperty("id")]
        public string Id { get; set; }

        [JsonProperty("name")]
        public string Name { get; set; }
    }

    public class LibraryGameData
    {
        public LibraryGameData()
        {
        }

        public LibraryGameData(Game game)
        {
            Id = game.Id.ToString();
            Name = game.Name;
            Hidden = game.Hidden;
            Favorite = game.Favorite;
            IsInstalled = game.IsInstalled;
            IsInstalling = game.IsInstalling;
            IsUninstalling = game.IsUninstalling;
            IsRunning = game.IsRunning;
            IsLaunching = game.IsLaunching;
            InstallSize = game.InstallSize;
            Playtime = game.Playtime.ToString();
            PlayCount = game.PlayCount;
            LastActivity = game.LastActivity;
            Added = game.Added;
            Modified = game.Modified;
            Version = game.Version;
            Source = game.Source?.Name;
            Platforms = game.Platforms?.Where(platform => platform != null).Select(platform => new LibraryPlatformData
            {
                Id = platform.Id.ToString(),
                Name = platform.Name
            }).ToList() ?? new List<LibraryPlatformData>();
            Categories = game.Categories?.Where(category => category != null).Select(category => category.Name).Where(name => !string.IsNullOrWhiteSpace(name)).ToList() ?? new List<string>();
            Tags = game.Tags?.Where(tag => tag != null).Select(tag => tag.Name).Where(name => !string.IsNullOrWhiteSpace(name)).ToList() ?? new List<string>();
            Features = game.Features?.Where(feature => feature != null).Select(feature => feature.Name).Where(name => !string.IsNullOrWhiteSpace(name)).ToList() ?? new List<string>();
            Genres = game.Genres?.Where(genre => genre != null).Select(genre => genre.Name).Where(name => !string.IsNullOrWhiteSpace(name)).ToList() ?? new List<string>();
        }

        [JsonProperty("id")]
        public string Id { get; set; }

        [JsonProperty("name")]
        public string Name { get; set; }

        [JsonProperty("hidden")]
        public bool Hidden { get; set; }

        [JsonProperty("favorite")]
        public bool Favorite { get; set; }

        [JsonProperty("isInstalled")]
        public bool IsInstalled { get; set; }

        [JsonProperty("isInstalling")]
        public bool IsInstalling { get; set; }

        [JsonProperty("isUninstalling")]
        public bool IsUninstalling { get; set; }

        [JsonProperty("isRunning")]
        public bool IsRunning { get; set; }

        [JsonProperty("isLaunching")]
        public bool IsLaunching { get; set; }

        [JsonProperty("installSize")]
        public ulong? InstallSize { get; set; }

        [JsonProperty("playtime")]
        public string Playtime { get; set; }

        [JsonProperty("playCount")]
        public ulong PlayCount { get; set; }

        [JsonProperty("lastActivity")]
        public DateTime? LastActivity { get; set; }

        [JsonProperty("added")]
        public DateTime? Added { get; set; }

        [JsonProperty("modified")]
        public DateTime? Modified { get; set; }

        [JsonProperty("version")]
        public string Version { get; set; }

        [JsonProperty("source")]
        public string Source { get; set; }

        [JsonProperty("platforms")]
        public List<LibraryPlatformData> Platforms { get; set; }

        [JsonProperty("categories")]
        public List<string> Categories { get; set; }

        [JsonProperty("tags")]
        public List<string> Tags { get; set; }

        [JsonProperty("features")]
        public List<string> Features { get; set; }

        [JsonProperty("genres")]
        public List<string> Genres { get; set; }
    }

    public class LibraryManifest
    {
        [JsonProperty("protocolVersion")]
        public int ProtocolVersion { get; set; } = LibraryProtocol.Version;

        [JsonProperty("revision")]
        public string Revision { get; set; }

        [JsonProperty("generatedAt")]
        public DateTime GeneratedAt { get; set; }

        [JsonProperty("gameCount")]
        public int GameCount { get; set; }

        [JsonProperty("chunkCount")]
        public int ChunkCount { get; set; }

        [JsonProperty("requestId")]
        public string RequestId { get; set; }
    }

    public class LibraryChunk
    {
        [JsonProperty("protocolVersion")]
        public int ProtocolVersion { get; set; } = LibraryProtocol.Version;

        [JsonProperty("revision")]
        public string Revision { get; set; }

        [JsonProperty("index")]
        public int Index { get; set; }

        [JsonProperty("games")]
        public List<LibraryGameData> Games { get; set; }
    }

    public class LibraryUpdate
    {
        [JsonProperty("protocolVersion")]
        public int ProtocolVersion { get; set; } = LibraryProtocol.Version;

        // Present only for a real Playnite lifecycle callback, never for snapshots.
        [JsonProperty("event")]
        public string Event { get; set; }

        [JsonProperty("game")]
        public LibraryGameData Game { get; set; }
    }

    public class LibraryStatus
    {
        [JsonProperty("protocolVersion")]
        public int ProtocolVersion { get; set; } = LibraryProtocol.Version;

        // The first selected game is exposed because HA's select entity chooses one game.
        [JsonProperty("selectedGameId")]
        public string SelectedGameId { get; set; }

        // Playnite exposes the desktop view as read-only through its public API.
        [JsonProperty("activeDesktopView")]
        public string ActiveDesktopView { get; set; }

        [JsonProperty("playniteVersion")]
        public string PlayniteVersion { get; set; }
    }

    public class LibraryRequest
    {
        [JsonProperty("requestId")]
        public string RequestId { get; set; }
    }

    public class LibraryCoverRequest
    {
        [JsonProperty("requestId")]
        public string RequestId { get; set; }

        [JsonProperty("gameId")]
        public string GameId { get; set; }

        // Omitted by older clients, where it is treated as "cover" for compatibility.
        [JsonProperty("imageType")]
        public string ImageType { get; set; }
    }

    public class LibraryCoverChunk
    {
        [JsonProperty("protocolVersion")]
        public int ProtocolVersion { get; set; } = LibraryProtocol.Version;

        [JsonProperty("requestId")]
        public string RequestId { get; set; }

        [JsonProperty("gameId")]
        public string GameId { get; set; }

        [JsonProperty("imageType")]
        public string ImageType { get; set; }

        [JsonProperty("index")]
        public int Index { get; set; }

        [JsonProperty("chunkCount")]
        public int ChunkCount { get; set; }

        [JsonProperty("contentType")]
        public string ContentType { get; set; }

        [JsonProperty("data")]
        public string Data { get; set; }

        [JsonProperty("error")]
        public string Error { get; set; }
    }

    public class LibraryCommand
    {
        [JsonProperty("requestId")]
        public string RequestId { get; set; }

        [JsonProperty("action")]
        public string Action { get; set; }

        [JsonProperty("gameId")]
        public string GameId { get; set; }
    }

    public class LibraryResponse
    {
        [JsonProperty("requestId")]
        public string RequestId { get; set; }

        [JsonProperty("action")]
        public string Action { get; set; }

        [JsonProperty("gameId")]
        public string GameId { get; set; }

        [JsonProperty("success")]
        public bool Success { get; set; }

        [JsonProperty("message")]
        public string Message { get; set; }
    }
}
