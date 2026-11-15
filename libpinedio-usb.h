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
  bool pin_poll_thread_exit;         /* unused since the worker lives from init to deinit */
  bool in_error;                     /* set by the first failed poll; stays set until deinit */
  pthread_cond_t poll_cond;          /* the worker's waits (CLOCK_MONOTONIC) and deinit's caller wait */
  bool worker_started;
  bool closing;                      /* deinit has begun: every later call is refused */
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
 * detach only enable or disable a pin; they never start, stop or wait for a thread. A callback runs on the
 * worker without the library's mutex; it may attach and detach, it must not deinit (refused). A callback the
 * worker took before a detach may still run once after that detach returned; none runs after deinit returns.
 * After deinit has begun, attach is refused (-1). */
int32_t pinedio_attach_interrupt(struct pinedio_inst* inst, enum pinedio_int_pin int_pin, enum pinedio_int_mode int_mode, void (*callback)(void));
int32_t pinedio_deattach_interrupt(struct pinedio_inst* inst, enum pinedio_int_pin int_pin);
void pinedio_deinit(struct pinedio_inst* inst);

#ifdef __cplusplus
}
#endif

#endif //PINEDIO_USB_LORA_ADAPTER_SDK_LIBPINEDIO_USB_H
