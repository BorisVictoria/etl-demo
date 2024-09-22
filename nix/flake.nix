{
  description = "Nix flake for STADVDB";

  # Change this to small if packages are not updated in the unstable branch!
  inputs = { nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable"; };

  outputs = { self, nixpkgs }:
    let
      system = "x86_64-linux";
      pkgs = nixpkgs.legacyPackages.${system};
    in {
      devShells.${system}.default = pkgs.mkShell {
        packages = with pkgs; [
          # Python Libraries
          (python3.withPackages (ps: [
            ps.streamlit
            ps.pymysql
            ps.pymongo
            ps.pandas
            ps.polars
            ps.matplotlib
          ]))

          # MySQL
          mysql
          mysql-shell-innovation

          # MongoDB
          mongosh
          mongodb-tools
        ];

        shellHook = ''
          nu
        '';

      };
    };

}
