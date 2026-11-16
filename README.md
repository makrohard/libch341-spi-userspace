# libpinedio-usb

Userspace driver for [PINE64 LoRa USB Adapter](https://pine64.com/product/pinedio-usb-lora-adapter/). Work in progress, do not use yet, API and stuff can change anytime!

# This fork: the poll thread

- One poll thread per opened device, from `pinedio_init` to `pinedio_deinit`. `pinedio_attach_interrupt` /
  `pinedio_deattach_interrupt` only enable or disable a pin. Unlike pine64's library, detaching the last pin does
  not stop or join the thread; it waits idle until `pinedio_deinit`.
- `pinedio_deinit` returns `int32_t`. 0: the thread has terminated, every counted call has made its last access,
  the transfers, handle, mutex and condvar are gone (a second call returns 0). -1: nothing released, call again
  (also while a USB transfer whose callback did not come within the drain deadline is outstanding, or a call or
  a poll callback has not returned within that deadline: deinit never waits longer than it).
- A USB request without progress for `PINEDIO_XFER_MS` (3 s) fails with -1 instead of waiting for ever.
- A USB read error no longer ends the thread: `in_error` stays set until `pinedio_deinit`.
- The pin mode and state are per device; `pinedio_d_mode` / `pinedio_d_state` are the template an open starts
  from (`pinedio_seed_pin_state`).

# TODO

- Use clangformat for proper consistent formatting
- Add MSVC support
