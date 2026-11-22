#!/bin/sh
# Tests of this library against a fake libusb (no hardware): each test includes ../libpinedio-usb.c and fakes the
# libusb calls it makes. Needs a C compiler and the libusb-1.0 headers. Prints each test's result; exit 1 if any
# failed. test_poll_rearm fails on a library whose poll thread is re-created by a re-attach from the callback.
here=$(cd "$(dirname "$0")" && pwd)
out=$(mktemp -d); trap 'rm -rf "$out"' EXIT
rc=0
for t in "$here"/test_*.c; do
  n=$(basename "$t" .c)
  if ! ${CC:-cc} -std=c11 -w -g -ftrivial-auto-var-init=pattern -I"$here/.." $(pkg-config --cflags libusb-1.0) \
       "$t" -o "$out/$n" -lpthread; then echo "$n: BUILD FAILED"; rc=1; continue; fi
  (cd "$out" && timeout 20 "./$n" </dev/null >"$n.txt" 2>&1); r=$?
  grep -E "^$n:|callbacks|one second" "$out/$n.txt"; [ $r -eq 0 ] || { echo "$n: rc=$r"; rc=1; }
done
exit $rc
