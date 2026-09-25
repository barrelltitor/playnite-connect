# Changelog

## 1.0.8 - 2026-09-17

### Changed

- Improved MQTT availability reporting for Home Assistant with retained QoS 1 status messages and a last-will offline status.
- Moved on-demand cover transfers to a small worker pool, keeping MQTT message handling responsive during image transfers.
- Updated the Playnite Connect icon.

### Fixed

- Returns an error response when a requested cover cannot be transferred.


## 1.0.7 - 2026-09-12

### Fixed

- Stops emulated games by requesting the configured emulator close cleanly, rather than force-killing a process in the game's directory.

## 1.0.6 - 2026-09-12

### Changed

- Assigned this independent fork its own Playnite add-on ID, package identity, and COM GUID.
- The extension can now install beside the upstream MQTT Client without claiming its identity.


## 1.0.3 - 2026-09-12

### Added

- Authenticated local Cover API at `GET /api/covers/{game-id}`.
- 256-bit bearer token, generated locally and rotatable from extension settings.
- Explicit Home Assistant network-access action with Windows URL ACL and a private firewall rule restricted to the configured HA IPv4 address.

### Changed

- Home Assistant now retrieves cover files through HTTP; MQTT remains for library sync, events, and commands.
## 1.0.2 - 2026-09-12

### Fixed

- Uses the distinct `PlayniteConnect.dll` assembly identity, allowing this fork to load beside Simeon Radivoev’s upstream MQTT Client.
- Renamed the reconnect/disconnect main-menu section to Playnite Connect.
## 1.0.1 - 2026-09-11

### Added

- On-demand, chunked MQTT transfer of the actual local Playnite cover image.
- Optional Home Assistant disk caching for transferred covers.

### Changed

- Renamed the visible fork to Playnite Connect.
## 1.0.0 - 2026-09-10

### Added

- Explicit real Playnite lifecycle names on live library updates: installed, starting, started, stopped, and uninstalled.
- Home Assistant event-bus support in the companion integration.

### Changed

- Removed legacy selected/current-game, image, active-view, and MQTT-discovery publishing from this fork.
## Fork development history - 2026-09-10

### Added

- Versioned, retained, chunked Playnite library snapshots for direct Home Assistant browsing.
- Live per-game install and running-state updates.
- MQTT request/response commands for start, install, uninstall, and safe install-directory-based stop.
- True Playnite installation state and install size in library metadata.
