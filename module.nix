self:
{ config, lib, pkgs, ... }:
let
  cfg = config.services.cliproxyapi-usage;
  inherit (lib) mkOption types;
  labelsFile = pkgs.writeText "cliproxyapi-usage-labels.json" (builtins.toJSON cfg.labels);
in
{
  options.services.cliproxyapi-usage = {
    enable = lib.mkEnableOption "the CLIProxyAPI usage collector and dashboard";
    package = mkOption {
      type = types.package;
      default = self.packages.${pkgs.stdenv.hostPlatform.system}.default;
      defaultText = lib.literalExpression "cliproxyapi-usage.packages.\${system}.default";
    };
    user = mkOption {
      type = types.str;
      default = "cliproxyapi-usage";
      description = "User to run as; it must be able to read managementKeyFile. Created if left at the default.";
    };
    group = mkOption {
      type = types.str;
      default = "cliproxyapi-usage";
    };
    proxyUrl = mkOption {
      type = types.str;
      default = "http://127.0.0.1:8317";
      description = "Base URL of CLIProxyAPI, whose management API must be enabled.";
    };
    managementKeyFile = mkOption {
      type = types.str;
      description = "File holding the plaintext CLIProxyAPI management key. Kept out of the Nix store.";
    };
    labels = mkOption {
      type = types.attrsOf types.str;
      default = { };
      example = { "48ff0700cad2" = "Alice"; };
      description = ''
        Display names for clients, keyed by the SHA-256 hex of their CLIProxyAPI
        API key or a prefix of it. Order here sets each client's chart color.
      '';
    };
    listenAddress = mkOption {
      type = types.str;
      default = "127.0.0.1";
    };
    port = mkOption {
      type = types.port;
      default = 8318;
    };
    pollInterval = mkOption {
      type = types.ints.positive;
      default = 5;
      description = "Seconds between usage-queue drains.";
    };
    title = mkOption {
      type = types.str;
      default = "CLIProxyAPI usage";
    };
    openFirewallInterfaces = mkOption {
      type = types.listOf types.str;
      default = [ ];
      example = [ "tailscale0" ];
      description = "Interfaces on which to open the dashboard port.";
    };
  };

  config = lib.mkIf cfg.enable {
    users.users = lib.mkIf (cfg.user == "cliproxyapi-usage") {
      cliproxyapi-usage = {
        isSystemUser = true;
        group = cfg.group;
      };
    };
    users.groups = lib.mkIf (cfg.group == "cliproxyapi-usage") { cliproxyapi-usage = { }; };

    systemd.services.cliproxyapi-usage = {
      description = "CLIProxyAPI usage collector and dashboard";
      wantedBy = [ "multi-user.target" ];
      wants = [ "network-online.target" ];
      after = [ "network-online.target" ];
      serviceConfig = {
        User = cfg.user;
        Group = cfg.group;
        StateDirectory = "cliproxyapi-usage";
        StateDirectoryMode = "0700";
        ExecStart = lib.escapeShellArgs [
          (lib.getExe cfg.package)
          "--proxy-url" cfg.proxyUrl
          "--management-key-file" cfg.managementKeyFile
          "--db" "/var/lib/cliproxyapi-usage/usage.db"
          "--labels-file" labelsFile
          "--listen" cfg.listenAddress
          "--port" (toString cfg.port)
          "--poll-interval" (toString cfg.pollInterval)
          "--title" cfg.title
        ];
        # The listen address may not exist yet (e.g. Tailscale still starting); keep retrying.
        Restart = "always";
        RestartSec = 5;
        UMask = "0077";
        NoNewPrivileges = true;
        PrivateTmp = true;
        ProtectSystem = "strict";
        ProtectHome = true;
        ProtectKernelTunables = true;
        ProtectKernelModules = true;
        ProtectControlGroups = true;
        RestrictSUIDSGID = true;
        CapabilityBoundingSet = "";
      };
    };

    networking.firewall.interfaces = lib.genAttrs cfg.openFirewallInterfaces (_: {
      allowedTCPPorts = [ cfg.port ];
    });
  };
}
