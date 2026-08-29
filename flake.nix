{
  description = "BlitzDB development environment";

  inputs.nixpkgs.url = "github:NixOS/nixpkgs/24.11";

  outputs = { self, nixpkgs }: let
    forAllSystems = nixpkgs.lib.genAttrs [
      "x86_64-linux"
      "aarch64-linux"
      "x86_64-darwin"
      "aarch64-darwin"
    ];
  in {
    devShells = forAllSystems (system: let
      pkgs = import nixpkgs { inherit system; };
    in {
      default = pkgs.mkShell {
        nativeBuildInputs = with pkgs; [
          pkgconf
          libfabric
        ];
      };
    });
  };
}
