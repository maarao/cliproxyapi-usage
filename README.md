# cliproxyapi-usage

Usage history and a dashboard for [CLIProxyAPI](https://github.com/router-for-me/CLIProxyAPI).

CLIProxyAPI v7 no longer keeps usage totals. It publishes one record per request
to a queue that readers drain destructively and that expires after
`redis-usage-queue-retention-seconds`. This service drains that queue into
SQLite every few seconds and serves:

- `/`: a dashboard of tokens or requests over time by client, plus totals by
  client, upstream account and model, and recent failures (last 24h/7d/30d/90d)
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

The collector reads `/v0/management/usage-queue` and `/v0/management/auth-files`.
Nothing else should drain the usage queue, or records are split between readers.

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
