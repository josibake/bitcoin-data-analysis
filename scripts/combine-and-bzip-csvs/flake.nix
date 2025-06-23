{
  description = "pull csv files from GCS; combine them into single bzip2 file; upload back to GCS";

  inputs = {
    nixpkgs.url = "nixpkgs/nixos-24.05";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs = { self, nixpkgs, flake-utils }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        pkgs = import nixpkgs { inherit system; };
        python = pkgs.python311;
        pythonEnv = python.withPackages (ps: with ps; [
          gcsfs
          pandas
          google-cloud-storage
          python-dotenv
        ]);
      in {
        packages.default = pkgs.writeShellApplication {
          name = "combine-csvs";
          runtimeInputs = [ pythonEnv ];
          text = ''
            exec python ${./main.py} "$@"
          '';
        };

        apps.default = flake-utils.lib.mkApp {
          drv = self.packages.${system}.default;
        };
      });
}
