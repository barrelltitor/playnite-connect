# Changelog

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
# Changelog

## 06/01/2024

### New

- MQTTClient: handles reconnection after disconnection

### Change

- Upgrade packages:
  - **MQTTNet** from *4.0.0-preview3* to *4.3.0.858*
  - **Newtonsoft.Json** from *10.0.3* to *13.0.3*
  - **PlayniteSDK** from *6.2.0* to *6.9.0*
- Metadata: add link to **simeonradivoev** GitHub project
- Settings: 
  - declare variable with the right type
  - add options for notification and show status changed
  - group options
- MQTTClient: use new options to show or not notifications


### Fix

- MQTTClient: `WithTls()` is deprecated. Replaced by `WithTlsOptions`
- MQTTClient: Add an explicit cast for the client declaration

### Log

- *151da59* - feat(MQTTClient): use new settings for displaying message or notifications. Fix can't disconnect after a power resume.
- *663121c* - feat(Settings): group settings in view. Add settings for display (QoL)
- *dc16494* - chore(metdata): add link to GitHub
- *2e32297* - chore csproj file change
- *d3756e8* - chore: restore code in PowerChange method
- *558e3dc* - chore: remove unecessary using
- *2b364d0* - fix: add an explicit cast for CreateMqttClient()
- *892cf6c* - chore: replace deprecated WIthTLS() method by WithTlsOptions()
- *17526d8* - chore: update plugin version
- *98c8a24* - feat: update MQTTnet, Newtonsoft.json and PlayniteSDK packages
- *f92d85f* - feat: handle disconnection after power resume
