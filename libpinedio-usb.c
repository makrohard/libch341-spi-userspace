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

#ifndef _GNU_SOURCE
#define _GNU_SOURCE   /* pthread_mutex_clocklock (glibc) */
#endif
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
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
    PINEDIO_TEST_HOOK(api_exit);
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

/* A public device call: counted from its first statement, the whole operation under the mutex, refused (-1)
 * once the instance is closing. */
#define PINEDIO_API(inst, call) do {          \
    if (api_enter(inst) != 0) return -1;      \
    int32_t api_ret_ = (call);                \
    api_exit(inst);                           \
    return api_ret_;                          \
  } while (0)

// store mode and state of d0-d7
uint16_t pinedio_d_mode = 0;
uint16_t pinedio_d_state = 0;
/* The pin mode and state each device starts with (a template). init copies it into the device's own
 * d_mode/d_state; every later write changes only that device's copy, under its mutex. A caller may seed the
 * template before an open (pinedio_seed_pin_state), e.g. to keep RESET high. This lock guards the template. */
static pthread_mutex_t pin_state_mutex = PTHREAD_MUTEX_INITIALIZER;

void pinedio_seed_pin_state(uint16_t mode_bits, uint16_t state_bits) {
  pthread_mutex_lock(&pin_state_mutex);
  pinedio_d_mode |= mode_bits;
  pinedio_d_state |= state_bits;
  pthread_mutex_unlock(&pin_state_mutex);
}

enum trans_state {TRANS_ACTIVE = -2, TRANS_ERR = -1, TRANS_IDLE = 0};

/* A transfer's state (struct pinedio_xfer_state, in the instance): st, and what the callback saw (status,
 * actual length; status -1 until a callback ran) for the caller's report. */
#define xfer_state pinedio_xfer_state

#ifndef PINEDIO_DRAIN_MS
#define PINEDIO_DRAIN_MS 3000   /* the cleanup, and deinit in all, wait at most this long */
#endif
#ifndef PINEDIO_XFER_MS
#define PINEDIO_XFER_MS 3000    /* a request whose transfers make no progress this long fails (each has 1 s) */
#endif

/* Monotonic deadlines. */
static void deadline_after(struct timespec *end, long ms) {
  clock_gettime(CLOCK_MONOTONIC, end);
  end->tv_sec += ms / 1000;
  end->tv_nsec += (ms % 1000) * 1000000L;
  if (end->tv_nsec >= 1000000000L) { end->tv_sec++; end->tv_nsec -= 1000000000L; }
}
/* false once end has passed; else tv = the time left, at most cap_us. */
static bool time_left(const struct timespec *end, struct timeval *tv, long cap_us) {
  struct timespec now;
  clock_gettime(CLOCK_MONOTONIC, &now);
  long long us = (long long) (end->tv_sec - now.tv_sec) * 1000000LL + (end->tv_nsec - now.tv_nsec) / 1000;
  if (us <= 0)
    return false;
  if (us > cap_us)
    us = cap_us;
  tv->tv_sec = (time_t) (us / 1000000);
  tv->tv_usec = (suseconds_t) (us % 1000000);
  return true;
}
/* Lock m, waiting at most until end (glibc's monotonic timed lock; elsewhere an unbounded lock). */
static int lock_until(pthread_mutex_t *m, const struct timespec *end) {
#if defined(__GLIBC__) && (__GLIBC__ > 2 || (__GLIBC__ == 2 && __GLIBC_MINOR__ >= 30))
  return pthread_mutex_clocklock(m, CLOCK_MONOTONIC, end);
#else
  (void) end;
  return pthread_mutex_lock(m);
#endif
}

/* st is written by whichever thread handles the libusb events (another instance's worker, too) and read by the
 * usb_transfer call that owns it: every access is atomic. */
#define ST_GET(ts) __atomic_load_n(&(ts).st, __ATOMIC_SEQ_CST)
#define ST_SET(ts, v) __atomic_store_n(&(ts).st, (v), __ATOMIC_SEQ_CST)

/* A callback only stores. It runs on whichever thread handles libusb events (another instance's worker, too),
 * and nothing in it may block that thread: a diagnostic on a stalled stderr held the event handler, so neither
 * the transfer loop's nor the drain's deadline could end the wait. The caller reports after its wait. The
 * terminal store is the callback's last access to its state. */
static void cb_common(struct libusb_transfer *transfer) {
  struct xfer_state *ts = (struct xfer_state *) transfer->user_data;
  int st;
  if (transfer->status != LIBUSB_TRANSFER_COMPLETED)
    st = TRANS_ERR;   /* CANCELLED too: as TRANS_IDLE it read "nothing to process" and hid a later slot's error */
  else if (transfer->actual_length != transfer->length)
    st = TRANS_ERR;   /* short or zero-byte: usb_transfer counts whole transfers and would wait for the rest */
  else
    st = transfer->actual_length;
  ts->status = transfer->status;
  ts->actual = transfer->actual_length;
  ST_SET(*ts, st);
}

// callback for bulk out async transfer
static void LIBUSB_CALL cb_out(struct libusb_transfer *transfer) {
  cb_common(transfer);
}

// callback for bulk in async transfer
static void LIBUSB_CALL cb_in(struct libusb_transfer *transfer) {
  cb_common(transfer);
}

/* The first transfer of the request that failed by itself (not cancelled by the cleanup), for the report. */
static void report_failed(const char *func, struct pinedio_inst *inst) {
  for (int i = -1; i < USB_IN_TRANSFERS; i++) {
    struct xfer_state *ts = i < 0 ? &inst->req_out : &inst->req_in[i];
    struct libusb_transfer *t = i < 0 ? inst->transfer_out : inst->transfer_ins[i];
    if (ST_GET(*ts) != TRANS_ERR || ts->status < 0 || ts->status == LIBUSB_TRANSFER_CANCELLED)
      continue;
    if (ts->status != LIBUSB_TRANSFER_COMPLETED)
      fprintf(stderr, "%s: %s transfer: error: %s\n", func, i < 0 ? "OUT" : "IN", libusb_error_name(ts->status));
    else
      fprintf(stderr, "%s: %s transfer: short: %d of %d bytes\n", func, i < 0 ? "OUT" : "IN", ts->actual, t->length);
    return;
  }
}

/* The caller holds usb_access_mutex (a public call through PINEDIO_API, or the poll worker). func NULL: quiet. */
/* The request has no transfer in flight: every state is past ACTIVE. */
static bool req_settled(struct pinedio_inst *inst) {
  if (ST_GET(inst->req_out) == TRANS_ACTIVE)
    return false;
  for (int i = 0; i < USB_IN_TRANSFERS; i++)
    if (ST_GET(inst->req_in[i]) == TRANS_ACTIVE)
      return false;
  return true;
}

/* Handle libusb events until the request is settled or end (monotonic) has passed; no event wait goes past end. */
static bool req_drain(struct pinedio_inst *inst, const struct timespec *end) {
  struct timeval tv;
  while (!req_settled(inst)) {
    if (!time_left(end, &tv, 100000))
      return false;
    libusb_handle_events_timeout(NULL, &tv);
  }
  return true;
}

static int32_t usb_transfer(struct pinedio_inst *inst, const char *func, unsigned int writecnt, unsigned int readcnt,
                            const uint8_t *writearr, uint8_t *readarr)
{
  bool stalled = false;   /* no transfer progressed within PINEDIO_XFER_MS */
  int submit_err = 0;      /* a failed submit: its error, for the report */
  const char *submit_dir = "";
  /* An abandoned request (a transfer whose callback did not come within the drain deadline) blocks every new one
   * until that callback has run: its states and buffers may still be written. */
  if (inst->req_abandoned) {
    if (!req_settled(inst))
      return -1;
    inst->req_abandoned = false;
  }
  /* The bounce buffers: a late callback writes into the instance, never into a caller's frame. */
  if (writecnt > inst->req_cap_out) {
    uint8_t *b = realloc(inst->req_buf_out, writecnt);
    if (!b) return -1;
    inst->req_buf_out = b;
    inst->req_cap_out = writecnt;
  }
  if (readcnt > inst->req_cap_in) {
    uint8_t *b = realloc(inst->req_buf_in, readcnt);
    if (!b) return -1;
    inst->req_buf_in = b;
    inst->req_cap_in = readcnt;
  }
  if (writecnt > 0)
    memcpy(inst->req_buf_out, writearr, writecnt);

  struct xfer_state *ts_outp = &inst->req_out, *ts_in = inst->req_in;
  ST_SET(*ts_outp, TRANS_IDLE);
  ts_outp->status = -1;
  inst->transfer_out->buffer = inst->req_buf_out;
  inst->transfer_out->length = writecnt;
  inst->transfer_out->user_data = ts_outp;

  /* Every IN state is set before the first submit: a failed OUT submit goes to the cleanup, which reads them. */
  for (unsigned int k = 0; k < USB_IN_TRANSFERS; k++) {
    ST_SET(ts_in[k], TRANS_IDLE);
    ts_in[k].status = -1;
  }

  /* Schedule write first */
  if (writecnt > 0) {
    ST_SET((*ts_outp), TRANS_ACTIVE);
    int ret = libusb_submit_transfer(inst->transfer_out);
    if (ret) {
      submit_err = ret;
      submit_dir = "OUT";
      ST_SET((*ts_outp), TRANS_ERR);
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
  uint8_t *in_buf = inst->req_buf_in;
  /* A callback that never comes (a failed timeout cancellation, a broken backend) must not hold the mutex for
   * ever: without progress for PINEDIO_XFER_MS the request fails through the cleanup below (cancel, bounded
   * drain, abandonment). Every completion restarts the deadline. */
  struct timespec end;
  deadline_after(&end, PINEDIO_XFER_MS);
  do {
    /* Schedule new reads as long as there are free transfers and unscheduled bytes to read. */
    while ((in_done + in_active) < readcnt && ST_GET(ts_in[free_idx]) == TRANS_IDLE) {
      unsigned int cur_todo = MIN(CH341_PACKET_LENGTH - 1, readcnt - in_done - in_active);
      inst->transfer_ins[free_idx]->length = cur_todo;
      inst->transfer_ins[free_idx]->buffer = in_buf;
      inst->transfer_ins[free_idx]->user_data = &ts_in[free_idx];
      /* ACTIVE before the submit: the transfer can complete on another thread's event handling (another
       * instance's worker) before submit returns; marking it after would overwrite that completion. */
      ST_SET(ts_in[free_idx], TRANS_ACTIVE);
      int ret = libusb_submit_transfer(inst->transfer_ins[free_idx]);
      if (ret) {
        ST_SET(ts_in[free_idx], TRANS_ERR);
        submit_err = ret;
        submit_dir = "IN";
        goto err;
      }
      in_buf += cur_todo;
      in_active += cur_todo;
      free_idx = (free_idx + 1) % USB_IN_TRANSFERS; /* Increment (and wrap around). */
    }

    /* Actually get some work done. */
    struct timeval tv;
    if (!time_left(&end, &tv, 1000000)) {
      stalled = true;
      goto err;
    }
    libusb_handle_events_timeout(NULL, &tv);

    /* Check for the write */
    if (out_done < writecnt) {
      if (ST_GET((*ts_outp)) == TRANS_ERR) {
        goto err;
      } else if (ST_GET((*ts_outp)) > 0) {
        out_done += ST_GET((*ts_outp));
        ST_SET((*ts_outp), TRANS_IDLE);
        deadline_after(&end, PINEDIO_XFER_MS);
      }
    }
    /* Check for completed transfers. */
    while (ST_GET(ts_in[in_idx]) != TRANS_IDLE && ST_GET(ts_in[in_idx]) != TRANS_ACTIVE) {
      if (ST_GET(ts_in[in_idx]) == TRANS_ERR) {
        goto err;
      }
      /* If a transfer is done, record the number of bytes read and reuse it later. */
      in_done += ST_GET(ts_in[in_idx]);
      in_active -= ST_GET(ts_in[in_idx]);
      ST_SET(ts_in[in_idx], TRANS_IDLE);
      in_idx = (in_idx + 1) % USB_IN_TRANSFERS; /* Increment (and wrap around). */
      deadline_after(&end, PINEDIO_XFER_MS);
    }
  } while ((out_done < writecnt) || (in_done < readcnt));

  if (readcnt > 0)
    memcpy(readarr, inst->req_buf_in, readcnt);
  return 0;
err:
  /* Clean up on errors. The report waits until the transfers are settled or abandoned (below). */
  bool write_failed = ST_GET((*ts_outp)) == TRANS_ERR;
  /* First, we must cancel any ongoing requests and wait for them to be canceled. Only the callback publishes
   * the end of a submitted transfer: a cancel that fails (e.g. NOT_FOUND, the transfer is completing) changes
   * nothing, and the wait below runs until its callback has run. Marking it failed here let this call return
   * while the callback was still to write into this stack frame. */
  if ((writecnt > 0) && (ST_GET((*ts_outp)) == TRANS_ACTIVE)) {
    (void) libusb_cancel_transfer(inst->transfer_out);
  }
  if (readcnt > 0) {
    unsigned int i;
    for (i = 0; i < USB_IN_TRANSFERS; i++) {
      if (ST_GET(ts_in[i]) == TRANS_ACTIVE)
        (void) libusb_cancel_transfer(inst->transfer_ins[i]);
    }
  }

  /* Wait for the callbacks, at most PINEDIO_DRAIN_MS in all. A transfer whose callback does not come (a failed
   * cancel, a broken backend) abandons the request: nothing of it is reused or freed while that callback is
   * possible, later calls are refused until it has run, and deinit returns -1 (retryable) meanwhile. */
  struct timespec drain_end;
  deadline_after(&drain_end, PINEDIO_DRAIN_MS);
  bool drained = req_drain(inst, &drain_end);
  if (!drained)
    inst->req_abandoned = true;
  if (func) {
    if (submit_err)
      fprintf(stderr, "%s: failed to submit %s transfer: %s\n", func, submit_dir, libusb_error_name(submit_err));
    else if (stalled)
      fprintf(stderr, "%s: no USB transfer progress within %d ms\n", func, PINEDIO_XFER_MS);
    else
      report_failed(func, inst);
    fprintf(stderr, "%s: Failed to %s %u bytes\n", func, write_failed ? "write" : "read",
            write_failed ? writecnt : readcnt);
  }
  if (!drained)
    fprintf(stderr, "%s: a USB transfer did not end within %d ms: request abandoned\n",
            func ? func : "usb_transfer", PINEDIO_DRAIN_MS);
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

/* init's early returns after the mutex and condvar exist (no worker, no transfers yet). */
static void sync_destroy(struct pinedio_inst *inst) {
  pthread_cond_destroy(&inst->poll_cond);
  pthread_mutex_destroy(&inst->usb_access_mutex);
}

static void *pinedio_poll_worker(void *arg);

int32_t pinedio_init(struct pinedio_inst *inst, void *driver) {
  return pinedio_init_at_port(inst, driver, NULL);
}

int32_t pinedio_init_at_port(struct pinedio_inst *inst, void *driver, const char *port_path) {
  int32_t ret;
  /* The instance is caller memory and may be uninitialised (upstream's test.c): every pointer the error
   * paths and deinit look at is set here, first. */
  inst->handle = NULL;
  inst->transfer_out = NULL;
  inst->req_buf_out = inst->req_buf_in = NULL;
  inst->req_cap_out = inst->req_cap_in = 0;
  inst->req_abandoned = false;
  inst->req_out.st = TRANS_IDLE;
  for (int i = 0; i < USB_IN_TRANSFERS; i++)
    inst->req_in[i].st = TRANS_IDLE;
  for (int i = 0; i < USB_IN_TRANSFERS; i++)
    inst->transfer_ins[i] = NULL;
  inst->worker_started = false;
  inst->worker_done = false;
  inst->closed = false;
  inst->int_running_cnt = 0;
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
  ret = pthread_condattr_init(&cond_attr);
  if (ret == 0) {
    ret = pthread_condattr_setclock(&cond_attr, CLOCK_MONOTONIC);   /* the worker's deadlines are monotonic */
    if (ret == 0)
      ret = pthread_cond_init(&inst->poll_cond, &cond_attr);
    pthread_condattr_destroy(&cond_attr);
  }
  if (ret != 0) {
    fprintf(stderr, "Failed to initialize condvar, res: %d.\n", ret);
    pthread_mutex_destroy(&inst->usb_access_mutex);
    return -1;
  }
  inst->worker_started = false;
  pthread_mutex_lock(&pin_state_mutex);   /* this device starts from the template */
  inst->d_mode = pinedio_d_mode;
  inst->d_state = pinedio_d_state;
  pthread_mutex_unlock(&pin_state_mutex);
  inst->closing = false;
  inst->api_active = 0;
  inst->int_running_cnt = 0;
  __atomic_store_n(&inst->in_error, false, __ATOMIC_SEQ_CST);

  ret = libusb_init(NULL);
  if (ret < 0) {
    fprintf(stderr, "Couldn't initialize libusb!\n");
    sync_destroy(inst);
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
  if (cnt < 0) {
    fprintf(stderr, "Couldn't list the USB devices: %s\n", libusb_error_name((int) cnt));
    sync_destroy(inst);
    return -2;   /* the libusb reference was taken */
  }

  for (i = 0; i < cnt; i++) {
    libusb_device *device = list[i];
    struct libusb_device_descriptor desc;
    ret = libusb_get_device_descriptor(device, &desc);
    if (ret != 0)
      continue;   /* unreadable: not a candidate */

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
    sync_destroy(inst);
    return -2;
  }

  // Allocate and pre-fill transfer structures.
  inst->transfer_out = libusb_alloc_transfer(0);
  if (!inst->transfer_out) {
    fprintf(stderr, "Failed to alloc libusb OUT transfer.\n");
    ret = -3;   /* the libusb reference was taken; never the stale 0 of the claim */
    goto deinit_on_error;
  }
  for (int i = 0; i < USB_IN_TRANSFERS; i++) {
    inst->transfer_ins[i] = libusb_alloc_transfer(0);
    if (inst->transfer_ins[i] == NULL) {
      fprintf(stderr, "Failed to alloc libusb IN transfer %d.\n", (int) i);
      ret = -3;
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

  if (pinedio_set_cs(inst, false) != 0) {
    fprintf(stderr, "Failed to set the initial CS level.\n");
    ret = -3;   /* the libusb reference was taken */
    goto deinit_on_error;
  }

  ret = pthread_create(&inst->pin_poll_thread, NULL, pinedio_poll_worker, inst);
  if (ret != 0) {
    fprintf(stderr, "Failed to create the poll thread, res: %d\n", ret);
    ret = -3;   /* the libusb reference was taken, as with -2 and 0 */
    goto deinit_on_error;
  }
  inst->worker_started = true;
  return 0;

deinit_on_error:
  pinedio_deinit(inst);
  return ret;
}

/* Configuration: before init, or after init only while no other call on this instance can run (the caller
 * serialises it; init resets AUTO_CS, so a caller sets that one after init). Not counted: before init the
 * instance's mutex does not exist yet. */
int32_t pinedio_set_option(struct pinedio_inst *inst, enum pinedio_option option, uint32_t value) {
  if ((unsigned) option >= PINEDIO_OPTION_MAX)
    return -1;
  inst->options[option] = value;
  return 0;
}

/* The mode reaches the device with the next pin write (the direction is in every UIO command). */
static int32_t set_pin_mode_locked(struct pinedio_inst *inst, uint32_t pin, uint32_t mode) {
  if (mode == 1) { // output
    inst->d_mode |= (1 << pin);
  } else {
    inst->d_mode &= ~(1 << pin);
  }
  return 0;
}

int32_t pinedio_set_pin_mode(struct pinedio_inst *inst, uint32_t pin, uint32_t mode) {
  PINEDIO_API(inst, set_pin_mode_locked(inst, pin, mode));
}

static int32_t digital_write_locked(struct pinedio_inst *inst, uint32_t pin, bool active) {
  if (active) {
    inst->d_state |= (1 << pin);
  } else {
    inst->d_state &= ~(1 << pin);
  }
    uint8_t buf[] = {
          CH341_CMD_UIO_STREAM,
          CH341_CMD_UIO_STM_OUT | inst->d_state,  // bitfield controlling value of d0-d7 where the rightmost bit is d0
          CH341_CMD_UIO_STM_DIR | inst->d_mode,
          CH341_CMD_UIO_STM_END
  };

  int32_t ret = usb_transfer(inst, "pinedio_digital_write", sizeof(buf), 0, buf, NULL);
  if (ret < 0) {
    printf("Failed to set CS pin.\n");
  }
  return ret;

}

int32_t pinedio_digital_write(struct pinedio_inst *inst, uint32_t pin, bool active) {
  PINEDIO_API(inst, digital_write_locked(inst, pin, active));
}

int32_t pinedio_set_cs(struct pinedio_inst *inst, bool active) {
  PINEDIO_API(inst, digital_write_locked(inst, 0, active));
}

static int32_t write_read_locked(struct pinedio_inst* inst, uint8_t *writearr, uint32_t writecnt, uint8_t* readarr, uint32_t readcnt) {
  if (writecnt + readcnt == 0)
    return 0;   /* a no-op: nothing to send or read; the stack buffers below would have no element */
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
  if (inst->options[PINEDIO_OPTION_AUTO_CS] && digital_write_locked(inst, 0, true) < 0) {
    return -1;   /* CS not asserted: no SPI transfer */
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

  int32_t ret = usb_transfer(inst, "pinedio_write_read", packets + writecnt + readcnt,
                             writecnt + readcnt, wbuf, rbuf);
  /* CS is released even after a failed transfer; a failed release fails a transfer that went through */
  if (inst->options[PINEDIO_OPTION_AUTO_CS] && digital_write_locked(inst, 0, false) < 0 && ret == 0)
    ret = -1;
  if (ret < 0)
    return -1;

  unsigned int i;
  for (i = 0; i < readcnt; i++) {
    *readarr++ = reverse_byte(rbuf[writecnt + i]);
  }

  return 0;
}

int32_t pinedio_write_read(struct pinedio_inst* inst, uint8_t *writearr, uint32_t writecnt, uint8_t* readarr, uint32_t readcnt) {
  PINEDIO_API(inst, write_read_locked(inst, writearr, writecnt, readarr, readcnt));
}

static int32_t transceive_locked(struct pinedio_inst* inst, uint8_t *write_buf, uint8_t* read_buf, uint32_t count) {
  if (count == 0)
    return 0;   /* a no-op: nothing to send or read; the stack buffer below would have no element */
  const size_t packets = (count + CH341_PACKET_LENGTH - 2) / (CH341_PACKET_LENGTH - 1);

  uint8_t wbuf[packets*CH341_PACKET_LENGTH];


  uint8_t *ptr = wbuf;
  if (inst->options[PINEDIO_OPTION_AUTO_CS] && digital_write_locked(inst, 0, true) < 0) {
    return -1;   /* CS not asserted: no SPI transfer */
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

  int32_t ret = usb_transfer(inst, "pinedio_transceive", packets + count,
                             count, wbuf, read_buf);
  /* CS is released even after a failed transfer; a failed release fails a transfer that went through */
  if (inst->options[PINEDIO_OPTION_AUTO_CS] && digital_write_locked(inst, 0, false) < 0 && ret == 0)
    ret = -1;
  if (ret < 0)
    return -1;

  unsigned int i;
  for (i = 0; i < count; i++) {
    read_buf[i] = reverse_byte(read_buf[i]);   /* was *read_buf++ = reverse_byte(*read_buf): unsequenced (UB) */
  }

  return 0;
}

int32_t pinedio_transceive(struct pinedio_inst* inst, uint8_t *write_buf, uint8_t* read_buf, uint32_t count) {
  PINEDIO_API(inst, transceive_locked(inst, write_buf, read_buf, count));
}

static int32_t digital_read_locked(struct pinedio_inst *inst, uint32_t pin) {
  uint8_t buf[] = {
    0xA0,
  };
  uint8_t output[6];

  int32_t ret = usb_transfer(inst, "pinedio_digital_read", sizeof(buf), sizeof(output), buf, output);
  if (ret < 0) {
    fprintf(stderr, "Could not get input pins.\n");
    return ret;
  }
  // *input = ((output[2] & 0x80) << 16) | ((output[1] & 0xef) << 8) | output[0];
  return output[0] & 1 << pin; // maybe?
}

int32_t pinedio_digital_read(struct pinedio_inst *inst, uint32_t pin) {
  PINEDIO_API(inst, digital_read_locked(inst, pin));
}

/* The CH341 input word (D-lines + status bits); the mutex is held. func NULL: no log lines. */
static int32_t read_input_locked(struct pinedio_inst *inst, const char *func, uint32_t *input)
{
  uint8_t buf[] = {
    0xA0,
  };
  uint8_t output[6] = {0};

  int32_t ret = usb_transfer(inst, func, sizeof(buf), sizeof(output), buf, output);
  *input = ((output[2] & 0x80) << 16) | ((output[1] & 0xef) << 8) | output[0];
  return ret;
}

static int32_t pinedio_get_input(struct pinedio_inst *inst, uint32_t* input)
{
  int32_t ret = read_input_locked(inst, "pinedio_get_input", input);
  if (ret < 0) {
    fprintf(stderr, "Could not get input pins.\n");
  }
  return ret;
}

static int32_t get_irq_state_locked(struct pinedio_inst *inst, uint32_t pin) {
  uint32_t input;
  int32_t ret = pinedio_get_input(inst, &input);
  if (ret != 0) {
    return ret;
  }
  return (input & (1 << pin)) != 0 ? 1 : 0;
}

int32_t pinedio_get_irq_state(struct pinedio_inst *inst, uint32_t pin) {
  PINEDIO_API(inst, get_irq_state_locked(inst, pin));
}

/* The worker's read: the mutex is held by the caller (the worker). Quiet once in_error is set, so a gone device
 * logs one failure, not one every poll. */
static int32_t pinedio_poll_read(struct pinedio_inst *inst, uint32_t *input) {
  PINEDIO_TEST_HOOK(in_read);
  return read_input_locked(inst, __atomic_load_n(&inst->in_error, __ATOMIC_SEQ_CST) ? NULL : "pinedio_poll_read",
                           input);
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
      if (!__atomic_load_n(&inst->in_error, __ATOMIC_SEQ_CST))
        fprintf(stderr, "Failed to get input, res: %d\n", ret);
      __atomic_store_n(&inst->in_error, true, __ATOMIC_SEQ_CST);   /* read by callers without the mutex */
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
  inst->worker_done = true;   /* deinit's bounded wait ends here; its join then only reaps the thread */
  pthread_cond_broadcast(&inst->poll_cond);
  pthread_mutex_unlock(&inst->usb_access_mutex);
  PINEDIO_TEST_HOOK(worker_exit);
  return NULL;
}

/* Enable a pin: mode and callback only. Never starts, stops or waits for a thread. */
int32_t
pinedio_attach_interrupt(struct pinedio_inst *inst, enum pinedio_int_pin int_pin, enum pinedio_int_mode int_mode,
                         void (*callback)(void)) {
  if (api_enter(inst) != 0) {   /* first: the call is counted before anything else */
    fprintf(stderr, "pinedio_attach_interrupt: instance is closing, refused\n");
    return -1;
  }
  if ((unsigned) int_pin >= PINEDIO_INT_PIN_MAX || callback == NULL) {
    api_exit(inst);
    return -1;   /* a NULL callback would enable nothing: detach disables a pin */
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
  if (api_enter(inst) != 0) {   /* first: the call is counted before anything else */
    return -1;
  }
  if ((unsigned) int_pin >= PINEDIO_INT_PIN_MAX) {
    api_exit(inst);
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

/* 0: closed, the instance may be freed. -1: refused (called from a poll callback); nothing was released and the
 * caller must not free the instance. */
int32_t pinedio_deinit(struct pinedio_inst *inst) {
  if (inst->closed)
    return 0;   /* closed already: its mutex and condvar are destroyed, nothing to do */
  if (inst->worker_started && pthread_equal(pthread_self(), inst->pin_poll_thread)) {
    fprintf(stderr, "pinedio_deinit: called from a poll callback, refused\n");
    return -1;
  }
  PINEDIO_TEST_HOOK(deinit_entered);
  /* Every wait below ends by one deadline, PINEDIO_DRAIN_MS from here: a caller or a callback that does not
   * return (a USB event that never comes, a callback blocked in the caller's code) makes deinit return -1,
   * nothing released, retryable; it never blocks deinit for ever. */
  struct timespec end;
  deadline_after(&end, PINEDIO_DRAIN_MS);
  if (lock_until(&inst->usb_access_mutex, &end) != 0) {
    fprintf(stderr, "pinedio_deinit: the instance stayed busy for %d ms, nothing released\n", PINEDIO_DRAIN_MS);
    return -1;
  }
  inst->closing = true;
  for (int i = 0; i < PINEDIO_INT_PIN_MAX; i++)
    inst->interrupts[i].callback = NULL;
  inst->int_running_cnt = 0;
  pthread_cond_broadcast(&inst->poll_cond);
  /* The worker leaves its loop and every counted caller passes its last unlock before anything is freed or
   * destroyed. */
  while ((inst->worker_started && !inst->worker_done) || __atomic_load_n(&inst->api_active, __ATOMIC_SEQ_CST) > 0)
    if (pthread_cond_timedwait(&inst->poll_cond, &inst->usb_access_mutex, &end) == ETIMEDOUT)
      break;
  bool busy = (inst->worker_started && !inst->worker_done) || __atomic_load_n(&inst->api_active, __ATOMIC_SEQ_CST) > 0;
  pthread_mutex_unlock(&inst->usb_access_mutex);
  if (busy) {
    fprintf(stderr, "pinedio_deinit: a call or the poll worker did not return within %d ms, nothing released\n",
            PINEDIO_DRAIN_MS);
    return -1;
  }

  if (inst->worker_started) {
    int jr = pthread_join(inst->pin_poll_thread, NULL);   /* it has left its loop: only its return is left */
    if (jr != 0) {   /* not proven terminated: release nothing */
      fprintf(stderr, "pinedio_deinit: the poll worker could not be joined (%d), nothing released\n", jr);
      return -1;
    }
    inst->worker_started = false;   /* the worker has terminated */
  }
  PINEDIO_TEST_HOOK(deinit_past_wait);

  /* An abandoned request may still be called back: drain it within what is left of the deadline, and free
   * nothing (-1, retryable) if its callback still has not come. */
  if (inst->req_abandoned && !req_drain(inst, &end)) {
    fprintf(stderr, "pinedio_deinit: a USB transfer is still outstanding, nothing released\n");
    return -1;
  }
  inst->req_abandoned = false;

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
  free(inst->req_buf_out);
  free(inst->req_buf_in);
  inst->req_buf_out = inst->req_buf_in = NULL;
  inst->req_cap_out = inst->req_cap_in = 0;
  pthread_cond_destroy(&inst->poll_cond);
  pthread_mutex_destroy(&inst->usb_access_mutex);
  inst->closed = true;   /* only a teardown that ran to the end: a refused one (-1) stays retryable */
  return 0;
}
