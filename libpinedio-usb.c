// SPDX-License-Identifier: GPL-2.0-or-later
/**
 * Copyright (C) 2024 Marek Kraus <gamelaster@outlook.com>
 *
 * This code is heavily based on ch341a_spi.c from the flashrom project.
 * The plan is to rework parts of code, but until that, original developers deserves to be mentioned.
 * Copyright (C) 2011 asbokid <ballymunboy@gmail.com>
 * Copyright (C) 2014 Pluto Yang <yangyj.ee@gmail.com>
 * Copyright (C) 2015-2016 Stefan Tauner
 * Copyright (C) 2015 Urja Rannikko <urjaman@gmail.com>
 */

#include <errno.h>
#include <stdio.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include "libpinedio-usb.h"

/* Test seam: a test defines PINEDIO_TEST_HOOK before including this file to hold or record a thread at a named
 * point (tests/test_poll_worker.c). Empty otherwise. */
#ifndef PINEDIO_TEST_HOOK
#define PINEDIO_TEST_HOOK(name) do { } while (0)
#endif

#define PINEDIO_POLL_INTERVAL_NS (1000000000L / 30)

#if 0
#define pinedio_mutex_lock(...) { printf("Locking %s\n", __func__); pthread_mutex_lock(__VA_ARGS__); }
#define pinedio_mutex_unlock(...) { printf("Unlocking %s\n", __func__); pthread_mutex_unlock(__VA_ARGS__); }
#else
#define pinedio_mutex_lock(...) pthread_mutex_lock(__VA_ARGS__);
#define pinedio_mutex_unlock(...) pthread_mutex_unlock(__VA_ARGS__);
#endif

#define CH341_USB_TIMEOUT 1000
#define CH341_WRITE_EP 0x02
#define CH341_READ_EP 0x82
#define	CH341_PACKET_LENGTH	0x20

#define CH341_CMD_SPI_STREAM 0xA8

#define CH341_CMD_UIO_STREAM 0xAB
#define CH341_CMD_UIO_STM_OUT 0x80
#define CH341_CMD_UIO_STM_DIR 0x40
#define CH341_CMD_UIO_STM_END 0x20

#define MIN(x, y) (((x) < (y)) ? (x) : (y))

/* Every call that takes the mutex is counted from before its lock to its last unlock, so deinit can wait until
 * no caller is left before it destroys the mutex and the condvar. A call that finds the instance closing leaves
 * at once (-1). Returns 0 with the mutex held. */
static int api_enter(struct pinedio_inst *inst) {
  __atomic_fetch_add(&inst->api_active, 1, __ATOMIC_SEQ_CST);
  PINEDIO_TEST_HOOK(api_entered);
  pthread_mutex_lock(&inst->usb_access_mutex);
  if (inst->closing) {
    __atomic_fetch_sub(&inst->api_active, 1, __ATOMIC_SEQ_CST);
    pthread_cond_broadcast(&inst->poll_cond);
    pthread_mutex_unlock(&inst->usb_access_mutex);
    return -1;
  }
  return 0;
}

/* The caller's last access to the instance is this unlock. */
static void api_exit(struct pinedio_inst *inst) {
  PINEDIO_TEST_HOOK(api_exit);
  __atomic_fetch_sub(&inst->api_active, 1, __ATOMIC_SEQ_CST);
  if (inst->closing)
    pthread_cond_broadcast(&inst->poll_cond);
  pthread_mutex_unlock(&inst->usb_access_mutex);
}

// store mode and state of d0-d7
uint16_t pinedio_d_mode = 0;
uint16_t pinedio_d_state = 0;

enum trans_state {TRANS_ACTIVE = -2, TRANS_ERR = -1, TRANS_IDLE = 0};

static void platform_sleep(uint32_t msecs) {
#ifdef __WIN32
  Sleep(msecs);
#else
  usleep(msecs * 1000);
#endif
}

static void cb_common(const char* func, struct libusb_transfer *transfer) {
  int* transfer_cnt = (int *) transfer->user_data;

  if (transfer->status == LIBUSB_TRANSFER_CANCELLED) {
    // Silently ACK and exit.
    *transfer_cnt = TRANS_IDLE;
    return;
  }

  if (transfer->status != LIBUSB_TRANSFER_COMPLETED) {
    fprintf(stderr, "%s: error: %s\n", func, libusb_error_name(transfer->status));
    *transfer_cnt = TRANS_ERR;
  } else {
    *transfer_cnt = transfer->actual_length;
  }
}

// callback for bulk out async transfer
static void LIBUSB_CALL cb_out(struct libusb_transfer *transfer) {
  cb_common(__func__, transfer);
}

// callback for bulk in async transfer
static void LIBUSB_CALL cb_in(struct libusb_transfer *transfer) {
  cb_common(__func__, transfer);
}

static int32_t usb_transfer(struct pinedio_inst *inst, const char *func, unsigned int writecnt, unsigned int readcnt,
                            const uint8_t *writearr, uint8_t *readarr, bool lock)
{
  int state_out = TRANS_IDLE;

  if (lock && api_enter(inst) != 0) {
    return -1;   /* closing */
  }

  inst->transfer_out->buffer = (uint8_t*)writearr;
  inst->transfer_out->length = writecnt;
  inst->transfer_out->user_data = &state_out;

  /* Schedule write first */
  if (writecnt > 0) {
    state_out = TRANS_ACTIVE;
    int ret = libusb_submit_transfer(inst->transfer_out);
    if (ret) {
      if (func) fprintf(stderr, "%s: failed to submit OUT transfer: %s\n", func, libusb_error_name(ret));
      state_out = TRANS_ERR;
      goto err;
    }
  }

  /* Handle all asynchronous packets as long as we have stuff to write or read. The write(s) simply need
   * to complete but we need to scheduling reads as long as we are not done. */
  unsigned int free_idx = 0; /* The IN transfer we expect to be free next. */
  unsigned int in_idx = 0; /* The IN transfer we expect to be completed next. */
  unsigned int in_done = 0;
  unsigned int in_active = 0;
  unsigned int out_done = 0;
  uint8_t *in_buf = readarr;
  int state_in[USB_IN_TRANSFERS] = {0};
  do {
    /* Schedule new reads as long as there are free transfers and unscheduled bytes to read. */
    while ((in_done + in_active) < readcnt && state_in[free_idx] == TRANS_IDLE) {
      unsigned int cur_todo = MIN(CH341_PACKET_LENGTH - 1, readcnt - in_done - in_active);
      inst->transfer_ins[free_idx]->length = cur_todo;
      inst->transfer_ins[free_idx]->buffer = in_buf;
      inst->transfer_ins[free_idx]->user_data = &state_in[free_idx];
      int ret = libusb_submit_transfer(inst->transfer_ins[free_idx]);
      if (ret) {
        state_in[free_idx] = TRANS_ERR;
        if (func) fprintf(stderr, "%s: failed to submit IN transfer: %s\n",
                 func, libusb_error_name(ret));
        goto err;
      }
      in_buf += cur_todo;
      in_active += cur_todo;
      state_in[free_idx] = TRANS_ACTIVE;
      free_idx = (free_idx + 1) % USB_IN_TRANSFERS; /* Increment (and wrap around). */
    }

    /* Actually get some work done. */
    libusb_handle_events_timeout(NULL, &(struct timeval){1, 0});

    /* Check for the write */
    if (out_done < writecnt) {
      if (state_out == TRANS_ERR) {
        goto err;
      } else if (state_out > 0) {
        out_done += state_out;
        state_out = TRANS_IDLE;
      }
    }
    /* Check for completed transfers. */
    while (state_in[in_idx] != TRANS_IDLE && state_in[in_idx] != TRANS_ACTIVE) {
      if (state_in[in_idx] == TRANS_ERR) {
        goto err;
      }
      /* If a transfer is done, record the number of bytes read and reuse it later. */
      in_done += state_in[in_idx];
      in_active -= state_in[in_idx];
      state_in[in_idx] = TRANS_IDLE;
      in_idx = (in_idx + 1) % USB_IN_TRANSFERS; /* Increment (and wrap around). */
    }
  } while ((out_done < writecnt) || (in_done < readcnt));

  if (lock) {
    api_exit(inst);
  }
  return 0;
err:
  /* Clean up on errors. */
  if (func) fprintf(stderr, "%s: Failed to %s %d bytes\n", func, (state_out == TRANS_ERR) ? "write" : "read",
           (state_out == TRANS_ERR) ? writecnt : readcnt);
  /* First, we must cancel any ongoing requests and wait for them to be canceled. */
  if ((writecnt > 0) && (state_out == TRANS_ACTIVE)) {
    if (libusb_cancel_transfer(inst->transfer_out) != 0)
      state_out = TRANS_ERR;
  }
  if (readcnt > 0) {
    unsigned int i;
    for (i = 0; i < USB_IN_TRANSFERS; i++) {
      if (state_in[i] == TRANS_ACTIVE)
        if (libusb_cancel_transfer(inst->transfer_ins[i]) != 0)
          state_in[i] = TRANS_ERR;
    }
  }

  /* Wait for cancellations to complete. */
  while (1) {
    bool finished = true;
    if ((writecnt > 0) && (state_out == TRANS_ACTIVE))
      finished = false;
    if (readcnt > 0) {
      unsigned int i;
      for (i = 0; i < USB_IN_TRANSFERS; i++) {
        if (state_in[i] == TRANS_ACTIVE)
          finished = false;
      }
    }
    if (finished)
      break;
    libusb_handle_events_timeout(NULL, &(struct timeval){1, 0});
  }
  if (lock) {
    api_exit(inst);
  }
  return -1;
}

static uint8_t reverse_byte(uint8_t x) {
  x = ((x >> 1) & 0x55) | ((x << 1) & 0xaa);
  x = ((x >> 2) & 0x33) | ((x << 2) & 0xcc);
  x = ((x >> 4) & 0x0f) | ((x << 4) & 0xf0);

  return x;
}

// The USB port path of a device as Linux names it in sysfs: "<bus>-<port>[.<port>...]" ("" when unknown)
static void pinedio_port_path(libusb_device *device, char *out, size_t n) {
  uint8_t ports[8];
  int cnt = libusb_get_port_numbers(device, ports, (int) sizeof(ports));
  out[0] = '\0';
  if (cnt <= 0) {
    return;
  }
  size_t len = (size_t) snprintf(out, n, "%u-%u", (unsigned) libusb_get_bus_number(device), (unsigned) ports[0]);
  for (int i = 1; i < cnt && len < n; i++) {
    len += (size_t) snprintf(out + len, n - len, ".%u", (unsigned) ports[i]);
  }
  if (len >= n) {
    out[0] = '\0';
  }
}

static void *pinedio_poll_worker(void *arg);

int32_t pinedio_init(struct pinedio_inst *inst, void *driver) {
  return pinedio_init_at_port(inst, driver, NULL);
}

int32_t pinedio_init_at_port(struct pinedio_inst *inst, void *driver, const char *port_path) {
  int32_t ret;
  inst->int_running_cnt = 0;
  inst->pin_poll_thread_exit = false;
  for (int i = 0; i < PINEDIO_INT_PIN_MAX; i++) {
    inst->interrupts[i].callback = NULL;
  }

  inst->options[PINEDIO_OPTION_AUTO_CS] = 1;

  ret = pthread_mutex_init(&inst->usb_access_mutex, NULL);
  if (ret != 0) {
    fprintf(stderr, "Failed to initialize mutex, res: %d.\n", ret);
    return -1;
  }
  pthread_condattr_t cond_attr;
  pthread_condattr_init(&cond_attr);
  pthread_condattr_setclock(&cond_attr, CLOCK_MONOTONIC);
  ret = pthread_cond_init(&inst->poll_cond, &cond_attr);
  pthread_condattr_destroy(&cond_attr);
  if (ret != 0) {
    fprintf(stderr, "Failed to initialize condvar, res: %d.\n", ret);
    pthread_mutex_destroy(&inst->usb_access_mutex);
    return -1;
  }
  inst->worker_started = false;
  inst->closing = false;
  inst->api_active = 0;
  inst->int_running_cnt = 0;
  inst->in_error = false;

  ret = libusb_init(NULL);
  if (ret < 0) {
    fprintf(stderr, "Couldn't initialize libusb!\n");
    return -1;
  }

  libusb_set_option(NULL, LIBUSB_OPTION_LOG_LEVEL, LIBUSB_LOG_LEVEL_INFO);
  if (inst->options[PINEDIO_OPTION_VID] == 0) {
    inst->options[PINEDIO_OPTION_VID] = 0x1A86;
  }
  if (inst->options[PINEDIO_OPTION_PID] == 0) {
    inst->options[PINEDIO_OPTION_PID] = 0x5512;
  }

  // discover devices
  libusb_device **list;
  libusb_device *found = NULL;
  ssize_t cnt = libusb_get_device_list(NULL, &list);
  ssize_t i = 0;

  for (i = 0; i < cnt; i++) {
    libusb_device *device = list[i];
    struct libusb_device_descriptor desc;
    ret = libusb_get_device_descriptor(device, &desc);

    if (desc.idVendor == inst->options[PINEDIO_OPTION_VID] && desc.idProduct == inst->options[PINEDIO_OPTION_PID] ) {
      if (port_path != NULL) {
        char port[32];
        pinedio_port_path(device, port, sizeof(port));
        if (strcmp(port, port_path) != 0) {
          continue;
        }
      }
      found = device;
      ret = libusb_open(found, &inst->handle);
      if (inst->handle != NULL) {
        // Compare the serial before detaching the driver or claiming the interface:
        // a device that is not the one searched for is left as it was
        char _serial[9] = {0};
        if (desc.iSerialNumber == 0
            || libusb_get_string_descriptor_ascii(inst->handle, desc.iSerialNumber, (unsigned char *) _serial, 9) <= 0) {
          _serial[0] = '\0'; // no serial, or it could not be read
        }
        if (inst->options[PINEDIO_OPTION_SEARCH_SERIAL]
            && (_serial[0] == '\0' || strncmp(_serial, inst->serial_number, 8) != 0)) {
          libusb_close(inst->handle);
          inst->handle = NULL;
          continue;
        }

#ifdef __linux__
        // On Windows, driver needs to be replaced manually by Zadig
        ret = libusb_detach_kernel_driver(inst->handle, 0);
        if (ret != 0 && ret != LIBUSB_ERROR_NOT_FOUND) {
          fprintf(stderr, "Cannot detach the existing USB driver. Claiming the interface may fail: %s\n",
                libusb_error_name(ret));
        }
#endif
        ret = libusb_claim_interface(inst->handle, 0);
        if (ret != 0) {
          fprintf(stderr, "Failed to claim interface 0: %s\n", libusb_error_name(ret));
          libusb_close(inst->handle);
          inst->handle = NULL;
        } else {
          libusb_get_string_descriptor(inst->handle, desc.iProduct, 0, inst->product_string, 96);
          strncpy(inst->serial_number, _serial, 9);
          break;
        }
      }
    }
  }
  libusb_free_device_list(list, 1);

  if (inst->handle == NULL) {
    // TODO: Rework this so we can receive error and print it.
    fprintf(stderr, "Couldn't open LoRa USB device.\n");
    return -2;
  }

  // Allocate and pre-fill transfer structures.
  inst->transfer_out = libusb_alloc_transfer(0);
  if (!inst->transfer_out) {
    fprintf(stderr, "Failed to alloc libusb OUT transfer.\n");
    goto deinit_on_error;
  }
  for (int i = 0; i < USB_IN_TRANSFERS; i++) {
    inst->transfer_ins[i] = libusb_alloc_transfer(0);
    if (inst->transfer_ins[i] == NULL) {
      fprintf(stderr, "Failed to alloc libusb IN transfer %d.\n", i);
      goto deinit_on_error;
    }
  }

  // We use these helpers but don't fill the actual buffer yet.
  libusb_fill_bulk_transfer(inst->transfer_out, inst->handle, CH341_WRITE_EP, NULL, 0, cb_out, NULL, CH341_USB_TIMEOUT);
  for (int i = 0; i < USB_IN_TRANSFERS; i++)
    libusb_fill_bulk_transfer(inst->transfer_ins[i], inst->handle, CH341_READ_EP, NULL, 0, cb_in, NULL,
                              CH341_USB_TIMEOUT);

  /**
   * We don't need to initialize SPI at all, as by default it's configured properly.
   * Only thing required is pinmux, what is anyway configured by CS change function.
   */

  pinedio_set_cs(inst, false);

  ret = pthread_create(&inst->pin_poll_thread, NULL, pinedio_poll_worker, inst);
  if (ret != 0) {
    fprintf(stderr, "Failed to create the poll thread, res: %d\n", ret);
    ret = -1;
    goto deinit_on_error;
  }
  inst->worker_started = true;
  return 0;

deinit_on_error:
  pinedio_deinit(inst);
  return ret;
}

int32_t pinedio_set_option(struct pinedio_inst *inst, enum pinedio_option option, uint32_t value) {
  inst->options[option] = value;
}

int32_t pinedio_set_pin_mode(struct pinedio_inst *inst, uint32_t pin, uint32_t mode) {
    if (mode == 1) { // output
    pinedio_d_mode |= (1 << pin);
  } else {
    pinedio_d_mode &= ~(1 << pin);
  }
  uint8_t buf[] = {
          CH341_CMD_UIO_STREAM,
          CH341_CMD_UIO_STM_DIR | pinedio_d_mode, // enable output on d0-d5
          CH341_CMD_UIO_STM_END
  };

  int32_t ret = 0; //usb_transfer(inst, __func__, sizeof(buf), 0, buf, NULL, true);
  if (ret < 0) {
    printf("Failed to set CS pin.\n");
  }
  return ret;
}

int32_t pinedio_digital_write(struct pinedio_inst *inst, uint32_t pin, bool active) {
  if (active) {
    pinedio_d_state |= (1 << pin);
  } else {
    pinedio_d_state &= ~(1 << pin);
  }
    uint8_t buf[] = {
          CH341_CMD_UIO_STREAM,
          CH341_CMD_UIO_STM_OUT | pinedio_d_state,  // bitfield controlling value of d0-d7 where the rightmost bit is d0
          CH341_CMD_UIO_STM_DIR | pinedio_d_mode,
          CH341_CMD_UIO_STM_END
  };

  int32_t ret = usb_transfer(inst, __func__, sizeof(buf), 0, buf, NULL, true);
  if (ret < 0) {
    printf("Failed to set CS pin.\n");
  }
  return ret;

}

int32_t pinedio_set_cs(struct pinedio_inst *inst, bool active) {
  return pinedio_digital_write(inst, 0, active);
  
}

int32_t pinedio_write_read(struct pinedio_inst* inst, uint8_t *writearr, uint32_t writecnt, uint8_t* readarr, uint32_t readcnt) {
  /* How many packets ... */
  const size_t packets = (writecnt + readcnt + CH341_PACKET_LENGTH - 2) / (CH341_PACKET_LENGTH - 1);

  /* We pluck CS/timeout handling into the first packet thus we need to allocate one extra package. */
  uint8_t wbuf[packets*CH341_PACKET_LENGTH];
  uint8_t rbuf[writecnt + readcnt];
  /* Initialize the write buffer to zero to prevent writing random stack contents to device. */
  memset(wbuf, 0, CH341_PACKET_LENGTH);

  uint8_t *ptr = wbuf;
  /* CS usage is optimized by doing both transitions in one packet.
   * Final transition to deselected state is in the pin disable. */
//  pluck_cs(ptr, &data->stored_delay_us);
  if (inst->options[PINEDIO_OPTION_AUTO_CS]) {
    pinedio_set_cs(inst, true);
  }
  unsigned int write_left = writecnt;
  unsigned int read_left = readcnt;
  unsigned int p;
  for (p = 0; p < packets; p++) {
    unsigned int write_now = MIN(CH341_PACKET_LENGTH - 1, write_left);
    unsigned int read_now = MIN((CH341_PACKET_LENGTH - 1) - write_now, read_left);
    ptr = &wbuf[p*CH341_PACKET_LENGTH];
    *ptr++ = CH341_CMD_SPI_STREAM;
    unsigned int i;
    for (i = 0; i < write_now; ++i)
      *ptr++ = reverse_byte(*writearr++);
    if (read_now) {
      memset(ptr, 0xFF, read_now);
      read_left -= read_now;
    }
    write_left -= write_now;
  }

  int32_t ret = usb_transfer(inst, __func__, packets + writecnt + readcnt,
                             writecnt + readcnt, wbuf, rbuf, true);
  if (inst->options[PINEDIO_OPTION_AUTO_CS]) {
    pinedio_set_cs(inst, false);
  }
  if (ret < 0)
    return -1;

  unsigned int i;
  for (i = 0; i < readcnt; i++) {
    *readarr++ = reverse_byte(rbuf[writecnt + i]);
  }

  return 0;
}

int32_t pinedio_transceive(struct pinedio_inst* inst, uint8_t *write_buf, uint8_t* read_buf, uint32_t count) {
  const size_t packets = (count + CH341_PACKET_LENGTH - 2) / (CH341_PACKET_LENGTH - 1);

  uint8_t wbuf[packets*CH341_PACKET_LENGTH];


  uint8_t *ptr = wbuf;
  if (inst->options[PINEDIO_OPTION_AUTO_CS]) {
    pinedio_set_cs(inst, true);
  }
  unsigned int write_left = count;
  unsigned int read_left = count;
  unsigned int p;
  for (p = 0; p < packets; p++) {
    unsigned int write_now = MIN(CH341_PACKET_LENGTH - 1, write_left);
    unsigned int read_now = MIN((CH341_PACKET_LENGTH - 1) - write_now, read_left);
    ptr = &wbuf[p*CH341_PACKET_LENGTH];
    *ptr++ = CH341_CMD_SPI_STREAM;
    unsigned int i;
    for (i = 0; i < write_now; ++i)
      *ptr++ = reverse_byte(*write_buf++);
    if (read_now) {
      memset(ptr, 0xFF, read_now);
      read_left -= read_now;
    }
    write_left -= write_now;
  }

  int32_t ret = usb_transfer(inst, __func__, packets + count,
                             count, wbuf, read_buf, true);
  if (inst->options[PINEDIO_OPTION_AUTO_CS]) {
    pinedio_set_cs(inst, false);
  }
  if (ret < 0)
    return -1;

  unsigned int i;
  for (i = 0; i < count; i++) {
    *read_buf++ = reverse_byte(*read_buf);
  }

  return 0;
}

int32_t pinedio_digital_read(struct pinedio_inst *inst, uint32_t pin) {
  uint8_t buf[] = {
    0xA0,
  };
  uint8_t output[6];

  int32_t ret = usb_transfer(inst, __func__, sizeof(buf), sizeof(output), buf, output, true);
  if (ret < 0) {
    fprintf(stderr, "Could not get input pins.\n");
    return ret;
  }
  // *input = ((output[2] & 0x80) << 16) | ((output[1] & 0xef) << 8) | output[0];
  return output[0] & 1 << pin; // maybe?
}

static int32_t pinedio_get_input(struct pinedio_inst *inst, uint32_t* input)
{
  uint8_t buf[] = {
    0xA0,
  };
  uint8_t output[6];

  int32_t ret = usb_transfer(inst, __func__, sizeof(buf), sizeof(output), buf, output, true);
  if (ret < 0) {
    fprintf(stderr, "Could not get input pins.\n");
  }
  *input = ((output[2] & 0x80) << 16) | ((output[1] & 0xef) << 8) | output[0];
  return ret;
}

int32_t pinedio_get_irq_state(struct pinedio_inst *inst, uint32_t pin) {
  uint32_t input;
  int32_t ret = pinedio_get_input(inst, &input);
  if (ret != 0) {
    return ret;
  }
  return (input & (1 << pin)) != 0 ? 1 : 0;
}

/* The worker's read: the mutex is held by the caller (the worker). Quiet once in_error is set, so a gone device
 * logs one failure, not one every poll. */
static int32_t pinedio_poll_read(struct pinedio_inst *inst, uint32_t *input) {
  uint8_t buf[] = {
    0xA0,
  };
  uint8_t output[6] = {0};

  PINEDIO_TEST_HOOK(in_read);
  int32_t ret = usb_transfer(inst, inst->in_error ? NULL : "pinedio_poll_read", sizeof(buf), sizeof(output), buf,
                             output, false);
  *input = ((output[2] & 0x80) << 16) | ((output[1] & 0xef) << 8) | output[0];
  return ret;
}

/* One worker per instance, from init to deinit. It polls while a pin is enabled, waits on poll_cond otherwise,
 * and calls the callbacks it took under the mutex without it. A failed read sets in_error (sticky) and the
 * worker keeps going; only deinit (closing) ends it. */
static void *pinedio_poll_worker(void *arg) {
  struct pinedio_inst *inst = arg;
  void (*calls[PINEDIO_INT_PIN_MAX])(void);

  pthread_mutex_lock(&inst->usb_access_mutex);
  while (!inst->closing) {
    if (inst->int_running_cnt == 0) {
      pthread_cond_wait(&inst->poll_cond, &inst->usb_access_mutex);   /* nothing enabled */
      continue;
    }
    uint32_t input;
    int n = 0;
    int32_t ret = pinedio_poll_read(inst, &input);
    if (ret < 0) {
      if (!inst->in_error)
        fprintf(stderr, "Failed to get input, res: %d\n", ret);
      inst->in_error = true;
    } else {
      for (uint8_t int_pin = 0; int_pin < PINEDIO_INT_PIN_MAX; int_pin++) {
        struct pinedio_inst_int *inst_int = &inst->interrupts[int_pin];
        if (inst_int->callback == NULL) continue;
        uint8_t state = (input & (1 << int_pin)) != 0;
        if (inst_int->previous_state != 255 && inst_int->previous_state != state) {
          enum pinedio_int_mode mode =
                  inst_int->previous_state == false && state == true ? PINEDIO_INT_MODE_RISING : PINEDIO_INT_MODE_FALLING;
          if (inst_int->mode & mode)
            calls[n++] = inst_int->callback;   /* the copy is called, never the field */
        }
        inst_int->previous_state = state;
      }
    }
    if (n > 0) {
      pthread_mutex_unlock(&inst->usb_access_mutex);
      PINEDIO_TEST_HOOK(before_callbacks);
      for (int i = 0; i < n; i++)
        calls[i]();
      pthread_mutex_lock(&inst->usb_access_mutex);
      if (inst->closing)
        break;
    }
    /* One absolute deadline per interval: a wakeup (spurious, or attach/detach) waits again for the same one. */
    struct timespec deadline;
    clock_gettime(CLOCK_MONOTONIC, &deadline);
    deadline.tv_nsec += PINEDIO_POLL_INTERVAL_NS;
    if (deadline.tv_nsec >= 1000000000L) {
      deadline.tv_sec++;
      deadline.tv_nsec -= 1000000000L;
    }
    while (!inst->closing && inst->int_running_cnt > 0) {
      if (pthread_cond_timedwait(&inst->poll_cond, &inst->usb_access_mutex, &deadline) == ETIMEDOUT)
        break;
    }
  }
  pthread_mutex_unlock(&inst->usb_access_mutex);
  PINEDIO_TEST_HOOK(worker_exit);
  return NULL;
}

/* Enable a pin: mode and callback only. Never starts, stops or waits for a thread. */
int32_t
pinedio_attach_interrupt(struct pinedio_inst *inst, enum pinedio_int_pin int_pin, enum pinedio_int_mode int_mode,
                         void (*callback)(void)) {
  if ((unsigned) int_pin >= PINEDIO_INT_PIN_MAX) {
    return -1;
  }
  if (api_enter(inst) != 0) {
    fprintf(stderr, "pinedio_attach_interrupt: instance is closing, refused\n");
    return -1;
  }
  if (inst->interrupts[int_pin].callback == NULL)
    inst->int_running_cnt++;
  inst->interrupts[int_pin].previous_state = 255;
  inst->interrupts[int_pin].mode = int_mode;
  inst->interrupts[int_pin].callback = callback;
  pthread_cond_broadcast(&inst->poll_cond);   /* an idle worker starts polling */
  api_exit(inst);
  return 0;
}

/* Disable a pin. -1 when it was not enabled or the instance is closing. */
int32_t pinedio_deattach_interrupt(struct pinedio_inst *inst, enum pinedio_int_pin int_pin) {
  if ((unsigned) int_pin >= PINEDIO_INT_PIN_MAX) {
    return -1;
  }
  if (api_enter(inst) != 0) {
    return -1;
  }
  int32_t res = -1;
  if (inst->interrupts[int_pin].callback != NULL) {
    inst->interrupts[int_pin].callback = NULL;
    inst->int_running_cnt--;
    res = 0;
  }
  pthread_cond_broadcast(&inst->poll_cond);
  api_exit(inst);
  return res;
}

void pinedio_deinit(struct pinedio_inst *inst) {
  if (inst->worker_started && pthread_equal(pthread_self(), inst->pin_poll_thread)) {
    fprintf(stderr, "pinedio_deinit: called from a poll callback, refused\n");
    return;
  }
  PINEDIO_TEST_HOOK(deinit_entered);
  pthread_mutex_lock(&inst->usb_access_mutex);
  inst->closing = true;
  for (int i = 0; i < PINEDIO_INT_PIN_MAX; i++)
    inst->interrupts[i].callback = NULL;
  inst->int_running_cnt = 0;
  pthread_cond_broadcast(&inst->poll_cond);
  pthread_mutex_unlock(&inst->usb_access_mutex);

  if (inst->worker_started) {
    pthread_join(inst->pin_poll_thread, NULL);   /* the worker has terminated */
    inst->worker_started = false;
  }

  /* Every counted caller has passed its last unlock before anything is freed or destroyed. */
  pthread_mutex_lock(&inst->usb_access_mutex);
  while (__atomic_load_n(&inst->api_active, __ATOMIC_SEQ_CST) > 0)
    pthread_cond_wait(&inst->poll_cond, &inst->usb_access_mutex);
  pthread_mutex_unlock(&inst->usb_access_mutex);
  PINEDIO_TEST_HOOK(deinit_past_wait);

  for (int i = 0; i < USB_IN_TRANSFERS; i++) {
    if (inst->transfer_ins[i] != NULL) {
      libusb_free_transfer(inst->transfer_ins[i]);
      inst->transfer_ins[i] = NULL;
    }
  }
  if (inst->transfer_out != NULL) {
    libusb_free_transfer(inst->transfer_out);
    inst->transfer_out = NULL;
  }

  if (inst->handle != NULL) {
    // We don't know if claim of interface was successful, but libusb handles this.
    libusb_release_interface(inst->handle, 0);
#ifdef __linux__
    libusb_attach_kernel_driver(inst->handle, 0);
#endif
    libusb_close(inst->handle);
    inst->handle = NULL;
  }
  pthread_cond_destroy(&inst->poll_cond);
  pthread_mutex_destroy(&inst->usb_access_mutex);
}
