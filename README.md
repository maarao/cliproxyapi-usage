# cliproxyapi-usage

Usage history and a dashboard for [CLIProxyAPI](https://github.com/router-for-me/CLIProxyAPI).

CLIProxyAPI (v7 and later) no longer keeps usage totals. It publishes one record per request
to a queue that readers drain destructively and that expires after
`redis-usage-queue-retention-seconds`. This service drains that queue into
SQLite every few seconds and serves:

- `/`: a dashboard of tokens or requests over time by client, plus totals by
  client, upstream account and model, and recent failures (last 24h/7d/30d/90d).
  Providers count input differently (Codex includes cache reads in input,
  Claude does not and reports cache writes only in the total), so the tables
  show uncached input as total - output - cached.
- an "Upstream accounts" table when reset-aware priority is on (below)
- `/api/summary?range=24h|7d|30d|90d&metric=tokens|requests`: the same data as JSON
- `/healthz`

It is one Python file with no dependencies beyond the standard library.

## Requirements on the proxy

In CLIProxyAPI's config:

- `remote-management.secret-key` set (and `allow-remote: true` if the
  collector connects from another address)
- `usage-statistics-enabled: true`
- `redis-usage-queue-retention-seconds` long enough to cover collector
  downtime, e.g. `3600`

The collector reads `/v0/management/usage-queue` and `/v0/management/auth-files`;
reset-aware priority also uses `/v0/management/api-call` and
`PATCH /v0/management/auth-files/fields`.
Nothing else should drain the usage queue, or records are split between readers.

## Reset-aware priority (optional)

CLIProxyAPI has no routing strategy that looks at rate-limit resets, but it
prefers higher-`priority` credentials for new sessions. With `--prioritize`,
every few minutes the service reads each Claude and Codex account's windows
(through the proxy's `/api-call`, the same calls its web panel makes) and sets
priorities so that, per provider:

1. accounts that are usable come first: not limited, under
   `--short-window-limit` (default 90%) of the short window (e.g. Claude's
   5-hour window), with weekly allowance left;
2. among those, the soonest weekly reset wins, so allowance that would expire
   unused gets spent first;
3. the rest follow, soonest to become usable first.

Only changed priorities are written. If any account of a provider cannot be
read, that provider is left alone for the round. Established sessions keep
their account (session affinity), and exhausted accounts still fail over. This
overwrites hand-set priorities on Claude and Codex credentials.

## Privacy

Raw client API keys are never stored: each record keeps only the SHA-256 of its
key. Give clients names by mapping that hash (or a prefix of it) to a label:

```sh
printf %s "$CLIENT_KEY" | sha256sum
```

## NixOS

```nix
{
  inputs.cliproxyapi-usage.url = "github:maarao/cliproxyapi-usage";
  # in your nixosSystem modules:
  #   cliproxyapi-usage.nixosModules.default
}
```

```nix
services.cliproxyapi-usage = {
  enable = true;
  user = "cliproxyapi";                      # any user that can read the key file
  managementKeyFile = "/var/lib/cliproxyapi/management.key";
  proxyUrl = "http://127.0.0.1:8317";
  labels = { "48ff0700cad2" = "Alice"; "6a8f36647d26" = "Bob"; };
  listenAddress = "100.64.0.1";              # e.g. a Tailscale address
  openFirewallInterfaces = [ "tailscale0" ];
  prioritize.enable = true;                  # optional, see above
};
```

The dashboard has no authentication of its own; bind it to loopback or a
private network.

## Running directly

```sh
./cliproxyapi_usage.py --management-key-file key.txt --proxy-url http://127.0.0.1:8317 \
  --db usage.db --labels-file labels.json --listen 127.0.0.1 --port 8318
python3 -m unittest -v test_cliproxyapi_usage
```
