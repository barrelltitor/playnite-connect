# Playnite Connect

Playnite Connect is a Playnite extension that makes your game library and game
controls available to Home Assistant through MQTT. It is designed to work with
the [Playnite Connect Companion Home Assistant integration](https://github.com/barrelltitor/playnite-connect-companion).

## Features

- Browse and launch the Playnite library from Home Assistant.
- Start, stop, restart, install, and uninstall games.
- Publish game lifecycle events: installed, starting, started, stopped, and
  uninstalled.
- Send game metadata, including installation state, play time, tags, genres,
  platforms, categories, and sources.
- Deliver game artwork on demand through MQTT, with an optional authenticated
  local HTTP cover API.

## Installation

1. Download the latest `.pext` file from this repository's Releases page.
2. Open the file with Playnite to install it.
3. Restart Playnite if prompted.
4. Open **Add-ons settings → Generic → Playnite Connect**.
5. Configure your MQTT broker, credentials, and Device ID. The default Device
   ID is `playnite`.
6. Add **Playnite Connect Companion** in Home Assistant using
   the same Device ID.

The extension connects to the configured broker when Playnite starts. Use its
sidebar icon or menu entry to reconnect or disconnect.

## Artwork

MQTT artwork transfer is enabled by default: Home Assistant requests covers
only when it needs them. You can instead enable the optional **Cover API** in
Playnite Connect settings and provide its URL and token in the Home Assistant
integration. The API is disabled by default and requires a bearer token.

## Security

MQTT commands can control Playnite. Use broker authentication and limit access
to the Playnite Connect topics to trusted clients.

## Based on Playnite MQTT Client

Playnite Connect is based on [Playnite MQTT Client](https://github.com/simeonradivoev/PlayniteMQTTClient)
by Simeon Radivoev. This fork keeps the original MIT license and attribution.