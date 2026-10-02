{
  description = "Usage history and dashboard for CLIProxyAPI";

  inputs.nixpkgs.url = "github:NixOS/nixpkgs/nixos-26.05";

  outputs = { self, nixpkgs }:
    let
      systems = [ "x86_64-linux" "aarch64-linux" "x86_64-darwin" "aarch64-darwin" ];
      forAll = f: nixpkgs.lib.genAttrs systems (system: f nixpkgs.legacyPackages.${system});
    in
    {
      packages = forAll (pkgs: { default = pkgs.callPackage ./package.nix { }; });
      checks = forAll (pkgs: { default = self.packages.${pkgs.stdenv.hostPlatform.system}.default; });
      nixosModules.default = import ./module.nix self;
    };
}
