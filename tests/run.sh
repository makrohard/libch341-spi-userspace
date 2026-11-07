#!/bin/sh
# The library's unit tests against a fake libusb (no hardware, no libusb runtime). Needs the libusb-1.0 headers.
set -eu
here=$(cd "$(dirname "$0")" && pwd)
out=$(mktemp -d)
trap 'rm -rf "$out"' EXIT
for t in "$here"/test_*.c; do
  name=$(basename "$t" .c)
  ${CC:-cc} -std=c11 -Wall -Werror=implicit-function-declaration -g -I"$here/.." $(pkg-config --cflags libusb-1.0) \
    "$t" -o "$out/$name" -lpthread
  "$out/$name"
done
