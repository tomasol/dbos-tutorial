#!/usr/bin/env bash

# Collect versions of binaries installed by `nix develop` producing file `dev-deps.txt`.
# This script should be executed after every `nix flake update`.

set -exuo pipefail
cd "$(dirname "$0")/.."

rm -f dev-deps.txt
echo "gradle --version" >> dev-deps.txt
gradle --version >> dev-deps.txt
echo "gradle -q javaToolchains" >> dev-deps.txt
gradle -q javaToolchains >> dev-deps.txt
javac --version >> dev-deps.txt
