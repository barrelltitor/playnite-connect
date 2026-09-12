namespace MQTTClient
{
    public static class Topics
    {
        public const string ConnectionSubTopic = "connection";

        // Versioned Playnite library synchronization, control, and lifecycle protocol.
        public const string LibraryRequestSubTopic = "library/request";
        public const string LibraryCommandSubTopic = "library/command";
        public const string LibraryResponseSubTopic = "library/response";
        public const string LibraryManifestSubTopic = "library/manifest";
        public const string LibraryChunkSubTopic = "library/chunk";
        public const string LibraryUpdateSubTopic = "library/update";
        public const string LibraryStatusSubTopic = "library/status";

        // Covers are requested only when Home Assistant needs one. They are never
        // included in a normal library snapshot.
        public const string LibraryCoverRequestSubTopic = "library/cover/request";
        public const string LibraryCoverChunkSubTopic = "library/cover/chunk";
    }
}