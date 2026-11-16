// SPDX-License-Identifier: GPL-2.0-or-later
// Copyright 2024 Marek Kraus (@gamelaster)

#ifndef PINEDIO_USB_LORA_ADAPTER_SDK_LIBPINEDIO_USB_H
#define PINEDIO_USB_LORA_ADAPTER_SDK_LIBPINEDIO_USB_H

#ifdef __cplusplus
extern "C" {
#endif

#include <stdint.h>
#include <stdbool.h>
#include <libusb-1.0/libusb.h>
#include <pthread.h>

#define USB_IN_TRANSFERS 32



enum pinedio_int_pin {
  PINEDIO_PIN_D0,
  PINEDIO_PIN_D1,
  PINEDIO_PIN_D2,
  PINEDIO_PIN_D3,
  PINEDIO_PIN_D4,
  PINEDIO_PIN_D5,
  PINEDIO_PIN_D6,
  PINEDIO_PIN_D7,
  PINEDIO_PIN_ERR,
  PINEDIO_PIN_PEMP,
  PINEDIO_PIN_INT,
  PINEDIO_INT_PIN_MAX
};

enum pinedio_int_mode {
  PINEDIO_INT_MODE_RISING = 0x01,
  PINEDIO_INT_MODE_FALLING = 0x02,
};

enum pinedio_option {
    PINEDIO_OPTION_AUTO_CS,
    PINEDIO_OPTION_SEARCH_SERIAL,
    PINEDIO_OPTION_VID,
    PINEDIO_OPTION_PID,
    PINEDIO_OPTION_MAX
};

struct pinedio_inst_int {
    uint8_t previous_state;
    enum pinedio_int_mode mode;
    void (*callback)(void);
};

/* Even this will work mostly on desktop (so malloc is available), I still prefer static allocation.
 * That's why structure definition is in header. */

/* One USB request's transfer state (see usb_transfer): owned by the instance, not by a call's stack frame, so a
 * callback that comes late (after its call gave up waiting) writes into memory that still exists. */
struct pinedio_xfer_state {
  int st;
  bool quiet;
};

struct pinedio_inst {
  struct libusb_device_handle *handle;

  /* We need to use many queued IN transfers for any resemblance of performance (especially on Windows)
   * because USB spec says that transfers end on non-full packets and the device sends the 31 reply
   * data bytes to each 32-byte packet with command + 31 bytes of data... */
  struct libusb_transfer *transfer_out;
  struct libusb_transfer *transfer_ins[USB_IN_TRANSFERS];
  uint8_t int_running_cnt;          /* pins with a callback (enabled) */
  pthread_mutex_t usb_access_mutex;  /* every USB transfer, the pin table and the fields below */
  pthread_t pin_poll_thread;         /* the poll worker: started by init, joined by deinit */
  bool in_error;                     /* set by the first failed poll; stays set until deinit; read with __atomic_load_n */
  uint16_t d_mode, d_state;          /* this device's pin mode and state (D0-D7), from the template at init */
  /* The one outstanding USB request (calls are serialised by the mutex): its transfer states and its bounce
   * buffers. abandoned: a transfer of it did not end within the drain deadline; nothing of it is reused or
   * freed until its callback has run. */
  struct pinedio_xfer_state req_out, req_in[USB_IN_TRANSFERS];
  uint8_t *req_buf_out, *req_buf_in;
  uint32_t req_cap_out, req_cap_in;
  bool req_abandoned;
  pthread_cond_t poll_cond;          /* the worker's waits (CLOCK_MONOTONIC) and deinit's caller wait */
  bool worker_started;
  bool worker_done;                  /* the worker has left its loop (under the mutex; deinit waits for it) */
  bool closing;                      /* deinit has begun: every later call is refused */
  bool closed;                       /* deinit ran to the end: the sync objects are gone; deinit again returns 0 */
  int api_active;                    /* calls between their entry and their last unlock (atomic) */
  struct pinedio_inst_int interrupts[PINEDIO_INT_PIN_MAX];
  uint32_t options[PINEDIO_OPTION_MAX];
  char serial_number[9];
  char product_string[97];
};

int32_t pinedio_init(struct pinedio_inst* inst, void* driver);
// As pinedio_init, but only the device on USB port port_path ("<bus>-<port>[.<port>...]", as in sysfs);
// NULL = any port. PINEDIO_HAS_INIT_AT_PORT tells a caller that this function exists.
#define PINEDIO_HAS_INIT_AT_PORT 1
int32_t pinedio_init_at_port(struct pinedio_inst* inst, void* driver, const char *port_path);
int32_t pinedio_set_option(struct pinedio_inst* inst, enum pinedio_option option, uint32_t value);
int32_t pinedio_set_pin_mode(struct pinedio_inst *inst, uint32_t pin, uint32_t mode);
int32_t pinedio_digital_write(struct pinedio_inst *inst, uint32_t pin, bool active);
int32_t pinedio_set_cs(struct pinedio_inst *inst, bool active);
int32_t pinedio_write_read(struct pinedio_inst* inst, uint8_t *writearr, uint32_t writecnt, uint8_t* readarr, uint32_t readcnt);
int32_t pinedio_transceive(struct pinedio_inst* inst, uint8_t *write_buf, uint8_t* read_buf, uint32_t count);
int32_t pinedio_digital_read(struct pinedio_inst *inst, uint32_t pin);
int32_t pinedio_get_irq_state(struct pinedio_inst *inst, uint32_t pin);
/* Interrupts are polled by one worker thread per instance, started by init and joined by deinit. attach and
 * detach only enable or disable a pin; they never start, stop or wait for a thread. Behaviour change against
 * pine64's library: detaching the last pin no longer stops and joins the poll thread; it stays (idle on a
 * condvar) until deinit. A callback runs on the worker without the library's mutex; it may attach and detach,
 * it must not deinit (refused, -1). A callback the worker took before a detach may still run once after that
 * detach returned. After deinit has begun, every call is refused (-1). attach refuses a NULL callback.
 *
 * What pinedio_deinit returning 0 guarantees, exactly: the worker has terminated (joined); every call that was
 * counted (from its first statement) has made its last access to the instance; no callback runs any more; the
 * transfers are freed, the handle is closed and the mutex and condvar are destroyed. It does not cover a call
 * that starts after deinit began: the owner must not start one (a call that has not reached its first statement
 * is outside any library's reach). After 0 the instance may be freed; a second deinit returns 0 and touches
 * nothing. -1 (from a callback; a worker that could not be joined; or, within one deadline of PINEDIO_DRAIN_MS,
 * a call or the worker (a callback) that has not returned, or a USB transfer whose callback has not come):
 * nothing was released, deinit may be called again. deinit never blocks longer than that deadline (on glibc;
 * elsewhere its first lock of the mutex is unbounded). A USB request whose transfers make no progress for
 * PINEDIO_XFER_MS fails (-1) and is cancelled. While such a transfer is
 * outstanding every call is refused (-1). */
int32_t pinedio_attach_interrupt(struct pinedio_inst* inst, enum pinedio_int_pin int_pin, enum pinedio_int_mode int_mode, void (*callback)(void));
int32_t pinedio_deattach_interrupt(struct pinedio_inst* inst, enum pinedio_int_pin int_pin);
/* 0: closed, the instance may be freed (see above). -1: refused (see above): nothing released, retryable. */
int32_t pinedio_deinit(struct pinedio_inst* inst);
/* OR bits into the pin mode and state every later open starts with (e.g. to keep RESET high on an open);
 * an open device keeps its own copy. pinedio_d_mode / pinedio_d_state are that template. */
void pinedio_seed_pin_state(uint16_t mode_bits, uint16_t state_bits);

#ifdef __cplusplus
}
#endif

#endif //PINEDIO_USB_LORA_ADAPTER_SDK_LIBPINEDIO_USB_H
