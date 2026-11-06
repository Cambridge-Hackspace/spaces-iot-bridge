# spaces-iot-bridge

Makes devices on the hackspace's IoT broker appear to a
[Cooperative Systems: Spaces](https://github.com/Cambridge-Hackspace/cooperative-systems-spaces)
edge as **modules**, so that a Zigbee plug can switch a tool on the same lease a
ToolGuard relay does.

**Status: implementation, from a settled design.** The design, and the basis
for each of its decisions, is
[#1](https://github.com/Cambridge-Hackspace/spaces-iot-bridge/issues/1) -- the
decisions themselves are in
[its design comment](https://github.com/Cambridge-Hackspace/spaces-iot-bridge/issues/1#issuecomment-6697291588).
The work is broken out as #2 (scaffold and reaper tenant), #3 (the pure core),
#4 (Zigbee2MQTT adapter), #5 (edge adapter), #6 (`probe`), #7 (stop-on-loss),
#8 (deployment) and #9 (what only hardware can answer). The core comes first;
its API is the contract the adapters are written against.

`reaper test` in this directory builds and tests the crate in a pinned Rust
container; `config.sample.toml` is the committed shape of the real
`/config.toml`, which never is.

## Why

A Spaces edge talks to the modules in its building over the `resource/*` topics
on its local broker: card readers ask about badges, power modules report in and
hold a lease, and a module whose lease stops being renewed switches itself off.
Purpose-built firmware such as
[spaces-toolguard-firmware](https://github.com/Cambridge-Hackspace/spaces-toolguard-firmware)
speaks that wire natively.

An off-the-shelf device cannot. A Zigbee plug only understands
Zigbee2MQTT's `{"state": "ON"}` on another broker, and has no identity on the
platform, no way to pick out its own lease, and no watchdog. This service speaks
the module wire on its behalf, and translates in both directions.

The platform already models these devices (`countdown` and
`holds_last_on_disconnect` in CS:S's `css_lib/src/capabilities.rs`); it has no
code that drives them. This is that code, kept outside the edge so the edge does
not change.

```
IoT broker                                         Spaces edge broker
zigbee2mqtt/<friendly name>       <-->  iot-bridge  <-->  resource/lease
zigbee2mqtt/<friendly name>/set                          resource/request/power
                                                         resource/request/alive
```

## Failing safe

A plug keeps its last state when its controller goes quiet. Left alone, that
would let a tool stay energized after the bridge, its host or the network died.

Where the plug supports Zigbee's on-with-timed-off, every lease renewal is sent
as `{"state": "ON", "on_time": N}`: the plug switches *itself* off N seconds
after the last renewal, whatever happened upstream. The bridge declares
`countdown: true` for such a plug only after it has been tested. A plug that
cannot do this is declared as such, and the platform then reports the tool as
not fail-safe, honestly.

`relay_on` is always what the plug reports, never what the bridge asked for. The
edge waits for a `true` from an earlier start stage (an extractor, say) before it
leases the next (the laser), so a bridge that echoed its own command could start
a laser with no extraction.

## Scope

1. **`power`**: plugs, starting with the laser's fume extractor.
2. Later: `sensor` (contact, leak or flow sensors feeding interlocks), once the
   edge has a wire path for sensor conditions; a stop from a button; other
   backends than Zigbee2MQTT.

## Licence

AGPL-3.0, the same as Spaces. See [LICENSE](LICENSE).

## Running it

The bridge runs on the edge VM, next to `css-edge`, as a systemd user unit
(`deploy/spaces-iot-bridge.service`). Three files hold secrets and are never in
this repository; all are `0600`:

| file | holds | written by |
|---|---|---|
| `~/.config/spaces-iot-bridge/config.toml` | the shape of `config.sample.toml` | you |
| `…/iot.toml`, `…/edge.toml` | `username` / `password` for each broker | you |
| `…/identities/<device>.toml` | `device_id` / `auth_token` | `register`, once |

### Commissioning a plug, in order

1. **Pair it in Zigbee2MQTT** and note its friendly name; put the device in
   `config.toml` with `countdown = false` for now.
2. **Measure it** — the bridge stopped, nothing using the plug:
   ```
   spaces-iot-bridge --config config.toml probe --device extractor
   ```
   Both halves must pass before you set `countdown = true`. The second half —
   re-sending `on_time` while ON extends the deadline — is the one the design
   depends on; a plug that passes only the first cannot be re-armed and runs on
   the bridge's watchdog alone, which the platform then reports honestly as
   `power_fails_safe = false`.
3. **Register it** — an administrator mints an invite on the platform:
   ```
   spaces-iot-bridge --config config.toml register --device extractor --invite <code>
   ```
   This writes the identity file and refuses to run again while it exists.
   Nothing re-registers on its own; replacing a live identity would orphan every
   binding to the old `device_id`.
4. **Bind it** on the platform, in Resource Wiring, as `power` on its resource.
   For the extractor: start stage 0, `fail_off` on disconnect; the laser's own
   box at stage 1 with a spin-up delay. The edge will not lease the laser until
   the extractor has *reported* `relay_on: true`.
5. **Start the service** and watch the log for `set ON on_time=60`, then the
   plug's own `ON` report, then `relay_on` appearing in the power reports.

### Reading the log

Every state transition is `INFO`; every anomaly `WARN`; and **every time
`relay_on` is omitted, the log says why** — a retained replay discarded, the
plug unavailable, a command unanswered. The silent failure is the one that
hides for weeks; this service is built not to have one.

| line | meaning |
|---|---|
| `retained state ignored; asking for a live report` | reconnect; resolves within a second |
| `plug unavailable; state unknown` | Zigbee2MQTT says the plug left the mesh |
| `no report within 5000 ms of ON; state unknown` | the plug did not answer a command |
| `leased but not On for 10000 ms; stopping <resource>` | stop-on-loss fired |
| `WebSocket`/`connection error … retrying` | a broker is unreachable; the plug's countdown is holding the line |
