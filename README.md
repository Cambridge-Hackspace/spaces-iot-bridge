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
[#20](https://github.com/Cambridge-Hackspace/spaces-iot-bridge/issues/20) added
Home Assistant as a second backend -- a lock as a power module on a door -- and
is documented in its own section below.

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
2. **Home Assistant** as a second backend (#20): a lock, switch or light HA
   owns, driven as a `power` module -- the lock case is a door.
3. **`sensor`** (#25): a Home Assistant `binary_sensor` -- a door contact, a
   flow switch, a leak sensor -- feeding an interlock condition over the wire
   CS:S #213 added. Zigbee2MQTT sensors and a stop from a button: later.

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
| `…/ha-token` | a Home Assistant long-lived access token, alone (#20) | you |
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

## Home Assistant (#20)

Some things a space wants behind a lease are reachable only through Home
Assistant — a door lock whose only integration is an HA one is the case this
was built for. HA is an **on-prem hub** here: the bridge talks to it over the
LAN on its WebSocket API, and whatever HA uses to reach the lock is HA's
business. The loop is the one above — watch the lease, drive a thing, report
what the thing says — with HA as the thing's owner instead of Zigbee2MQTT:

```
Home Assistant                                   Spaces edge broker
lock.workshop_door  (state_changed)  <-->  iot-bridge  <-->  resource/lease
lock.unlock / lock.lock (call_service)                       resource/request/power
```

**A lock is a `power` module on a door resource, and unlocked is "on".** The
edge leases a door's power bindings for exactly its strike pulse (or for the
session, on an `until_stopped` room): lease granted → `lock.unlock`; withdrawn
→ `lock.lock`. The `strike` role is the latch a reader's own box pulses from
its scan reply — the bridge is not on that path. But a door with no `strike`
binding has a pulse of zero and never leases anything, so the bridge registers
a lock as **`power` and `strike`**, and you bind it as both: the strike binding
gives the door its pulse, the power binding carries it out. Set
`hold_open_access = false` on the strike binding — open-access holds travel the
scan-reply path.

Domains the bridge drives as `power`: `lock` (unlocked = on), `switch`,
`light`, `input_boolean`.

**A `binary_sensor` is bridged as a `sensor` module** (#25), which the edge
*reads* rather than drives: the bridge publishes the contact's reading as one
named interlock condition (`condition = "door_open"` — the server's vocabulary)
and the edge gates and trips on it (CS:S #213). Two rules, both about silence:
an **unknown** sensor says nothing at all — no reading and no alive pulse — so a
contact Home Assistant has lost reads to the edge as a sensor gone quiet, which
denies the tool; and a change is reported **at once**, a steady state every few
seconds, because a condition the edge has never observed denies until the first
report. `invert = true` for a contact whose `on` is the safe state. A sensor
ignores its lease; there is nothing to switch. Zigbee2MQTT sensors are not
wired yet: their reading lives in a device-specific field nothing here names.

### What HA needs

Nothing bridge-specific is installed in HA; the WebSocket API is built in.

1. **A dedicated user**: *Settings → People → Users → Add user* — not an
   administrator, with **"Can only log in from the local network"** ticked.
2. **A long-lived access token** from that user: *Profile → Security →
   Long-lived access tokens → Create token*. Alone in a `0600` file; the
   bridge refuses a token file holding anything else.
3. **Stable entity IDs** (*entity → cog → Entity ID*), so a rename in HA does
   not silently move what the bridge drives. Audit HA automations on the same
   entity: an auto-relock is fine; an automation that *unlocks* on some other
   trigger is a second authority over a door Spaces thinks it controls.
4. **Plain `ws://` on the LAN.** Behind a TLS reverse proxy, `wss://` with a
   certificate the public roots trust (a private CA is not wired). Never the
   Nabu Casa remote URL — that puts the cloud in the path.

**HA has no per-entity permissions.** The token can call `lock.unlock` on every
lock in that HA, not only the configured one; a non-admin, local-only user is
the strongest scoping HA offers. Treat the token as you would an admin's.

### What it does and does not promise

- **HA's state is believed as HA's current belief**, `unavailable` included.
  A lock HA cannot see is reported as unknown, and a *leased* lock unknown
  for 10 s gets a `stop` on its door — the session ends and the lease is
  withdrawn. How fast an integration notices a lost device is the
  integration's: Z-Wave JS and ESPHome are prompt; ZHA defaults to hours for
  mains-powered devices.
- **Losing HA itself** reads as every bridged entity becoming unavailable, so
  any live lease on one is stopped after the debounce. The lock then holds its
  last state, which is what `holds_last_on_disconnect: true` has declared all
  along.
- **No countdown is claimed** for an HA device (`countdown` must be `false`;
  `probe` refuses one): nothing measures it, and an unmeasured countdown is
  the fail-safe lie the platform now refuses to badge.
- **A cloud-backed integration inside HA** (August, Yale, Nuki cloud) puts a
  vendor's cloud in an access-control path. Fine for a convenience unlock; not
  something a door's security should rest on. LAN-local integrations (Z-Wave,
  Zigbee, Matter, ESPHome, Shelly) are the intended case, and the bridge
  cannot tell the difference from its side.
- **Relock latency is the lease's.** Withdrawal is explicit and prompt; the
  relock lands within a lease tick of the pulse ending.

### Commissioning a lock

1. Make the HA user and token (above); put the lock in `config.toml` as the
   sample shows (`backend = "homeassistant"`, `entity`, `role = "power"`,
   `stop_resource_on_loss = true`).
2. **Register it** — `register --device workshop-door --invite <code>` — once.
3. **Bind it** in Resource Wiring, on the door: as `power` (`fail_off`), and as
   `strike` with the `pulse_ms` you want the door open for and
   `hold_open_access = false`. Bind the edge as the door's coordinator and
   give the door a rule admitting the members who may enter.
4. **Start the service**; badge at the door. The log shows the lease, then
   `set ON (homeassistant lock.workshop_door)`, HA's `unlocked`, `relay_on`
   in the power report, and the relock when the pulse ends.

| line | meaning |
|---|---|
| `ha: connected and authenticated` | the token works |
| `ha: token rejected (...); retrying in 60 s` | the token does not; check the user and file |
| `ha: connection lost (...)` | HA unreachable; every bridged entity is now unknown |
| `ha: <entity>: <state> (transitional, not a report)` | `locking`/`unlocking`; the previous state stands |
| `ha: 1 of 2 configured entities are not in Home Assistant's state list` | an entity id in the config does not exist in HA |
