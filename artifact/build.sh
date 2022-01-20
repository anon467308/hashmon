#!/bin/bash
set -e

if [[ ! ( -d amazon && -d formulas && -d tools ) ]]; then
  info "Error: this script must be run from the archive's root directory"
  exit 2
fi

echo "Building evaluation tools ..."
pushd tools > /dev/null
dune build --profile=release
popd > /dev/null

echo "Building MonPoly ..."
pushd monpoly > /dev/null
dune build --profile=release
popd > /dev/null
pushd monpoly_no_errprobs > /dev/null
dune build --profile=release
popd > /dev/null

echo "Building DejaVu monitors ..."
./dejavu_wrapper.sh compile e1
./dejavu_wrapper.sh compile e2
./dejavu_wrapper.sh compile e4

echo
echo "Build complete."
