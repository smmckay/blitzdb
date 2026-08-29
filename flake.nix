{
  description = "BlitzDB development environment";

  inputs.nixpkgs.url = "github:NixOS/nixpkgs/release-26.05";

  outputs = { self, nixpkgs }: let
    forAllSystems = nixpkgs.lib.genAttrs [
        "x86_64-linux"
        "aarch64-linux"
        "aarch64-darwin"
      ];
  in {
    devShells = forAllSystems (system: let
      pkgs = import nixpkgs { inherit system; };
    in {
      default = pkgs.mkShell {
        defaultShell = pkgs.zsh;
        nativeBuildInputs = with pkgs; [
          pkgconf
          libfabric
         ];
       };
     });
   };
}
