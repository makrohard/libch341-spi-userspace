# libpinedio-usb

Userspace driver for [PINE64 LoRa USB Adapter](https://pine64.com/product/pinedio-usb-lora-adapter/). Work in progress, do not use yet, API and stuff can change anytime!

# This fork: the poll thread

- One poll thread per opened device, from `pinedio_init` to `pinedio_deinit`. `pinedio_attach_interrupt` /
  `pinedio_deattach_interrupt` only enable or disable a pin. Unlike pine64's library, detaching the last pin does
  not stop or join the thread; it waits idle until `pinedio_deinit`.
- `pinedio_deinit` returns `int32_t`. 0: the thread has terminated, every counted call has made its last access,
  the transfers, handle, mutex and condvar are gone (a second call returns 0). -1: nothing released, call again
  (also while a USB transfer whose callback did not come within the drain deadline is outstanding, or a call or
  a poll callback has not returned, or the poll thread has not ended, within that deadline: deinit never waits
  longer than it).
- A USB request without progress for `PINEDIO_XFER_MS` (3 s) fails with -1 instead of waiting for ever.
- The libusb callbacks only store a transfer's result; the failing call reports it after its wait
  (`<function>: IN|OUT transfer: ...` instead of `cb_in:` / `cb_out:` lines). Nothing is written while the USB
  mutex is held: lines are collected and written after the unlock, on a private dup of stderr, by a write that
  cannot wait for a reader and cannot end or stop the process (a pipe or terminal through a private non-blocking
  open of it, a socket with `MSG_DONTWAIT | MSG_NOSIGNAL`, a regular file appended as stderr is; SIGPIPE, SIGXFSZ
  and SIGTTOU blocked around the write). A line stderr cannot take whole is dropped and counted (a short write
  ends with a newline before the next line); the count, for the whole library, precedes the next line. errno is
  kept. So a full, closed or redirected stderr, a log file at its size limit or a background terminal never holds
  a call, the poll thread or `pinedio_deinit`. Not bounded: a log file on a stuck filesystem. libusb's own log is
  off (set `LIBUSB_DEBUG` to see it). "Failed to set CS pin." now goes to stderr, not stdout.
- One `pinedio_write_read` / `pinedio_transceive` moves at most `PINEDIO_SPI_MAX` (65536) bytes; a longer one
  returns -1 without a transfer. An empty one returns 0.
- A USB read error no longer ends the thread: `in_error` stays set until `pinedio_deinit`.
- The pin mode and state are per device; `pinedio_d_mode` / `pinedio_d_state` are the template an open starts
  from (`pinedio_seed_pin_state`).

# TODO

- Use clangformat for proper consistent formatting
- Add MSVC support
