/* A fake libusb for the library's tests: each fake device counts what was done to it. Include it, then the
 * library's source ("../libpinedio-usb.c"), then call fake_helpers.h's helpers. */
#ifndef FAKE_LIBUSB_H
#define FAKE_LIBUSB_H
#define _GNU_SOURCE
#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <libusb.h>

struct fake_dev {
  uint16_t vid, pid;
  const char *serial;          /* NULL: iSerialNumber 0 */
  int serial_fails;            /* the descriptor read fails */
  uint8_t bus, ports[3], nports;
  int claim_fails;
  int opened, detached, claimed, closed;
};

static struct fake_dev g_devs[4];
static int g_ndev;
struct libusb_device { int idx; };
struct libusb_device_handle { int idx; };
static struct libusb_device g_dev_objs[4];
static struct libusb_device_handle g_handles[4];
static libusb_device *g_list[5];

int libusb_init(libusb_context **c) { (void) c; return 0; }
int libusb_set_option(libusb_context *c, enum libusb_option o, ...) { (void) c; (void) o; return 0; }
ssize_t libusb_get_device_list(libusb_context *c, libusb_device ***list) {
  (void) c;
  for (int i = 0; i < g_ndev; i++) { g_dev_objs[i].idx = i; g_list[i] = &g_dev_objs[i]; }
  g_list[g_ndev] = NULL;
  *list = g_list;
  return g_ndev;
}
void libusb_free_device_list(libusb_device **l, int u) { (void) l; (void) u; }
int libusb_get_device_descriptor(libusb_device *d, struct libusb_device_descriptor *desc) {
  memset(desc, 0, sizeof(*desc));
  desc->idVendor = g_devs[d->idx].vid;
  desc->idProduct = g_devs[d->idx].pid;
  desc->iSerialNumber = g_devs[d->idx].serial ? 3 : 0;
  desc->iProduct = 2;
  return 0;
}
uint8_t libusb_get_bus_number(libusb_device *d) { return g_devs[d->idx].bus; }
int libusb_get_port_numbers(libusb_device *d, uint8_t *p, int n) {
  int k = g_devs[d->idx].nports;
  if (k > n) return LIBUSB_ERROR_OVERFLOW;
  memcpy(p, g_devs[d->idx].ports, (size_t) k);
  return k;
}
int libusb_open(libusb_device *d, libusb_device_handle **h) {
  g_devs[d->idx].opened++;
  g_handles[d->idx].idx = d->idx;
  *h = &g_handles[d->idx];
  return 0;
}
void libusb_close(libusb_device_handle *h) { g_devs[h->idx].closed++; }
int libusb_detach_kernel_driver(libusb_device_handle *h, int i) { (void) i; g_devs[h->idx].detached++; return 0; }
int libusb_attach_kernel_driver(libusb_device_handle *h, int i) { (void) h; (void) i; return 0; }
int libusb_claim_interface(libusb_device_handle *h, int i) {
  (void) i;
  if (g_devs[h->idx].claim_fails) return LIBUSB_ERROR_BUSY;
  g_devs[h->idx].claimed++;
  return 0;
}
int libusb_release_interface(libusb_device_handle *h, int i) { (void) h; (void) i; return 0; }
int libusb_get_string_descriptor_ascii(libusb_device_handle *h, uint8_t idx, unsigned char *buf, int len) {
  struct fake_dev *f = &g_devs[h->idx];
  (void) idx;
  if (f->serial_fails || !f->serial) return LIBUSB_ERROR_IO;
  /* as libusb: at most len - 1 characters, then a NUL; a failed read writes nothing */
  size_t n = strlen(f->serial);
  if (n > (size_t) len - 1) n = (size_t) len - 1;
  memcpy(buf, f->serial, n);
  buf[n] = 0;
  return (int) n;
}
int libusb_control_transfer(libusb_device_handle *h, uint8_t t, uint8_t r, uint16_t v, uint16_t i, unsigned char *b,
                            uint16_t n, unsigned int to) {
  /* libusb_get_string_descriptor (inline in libusb.h) for the product string: an empty answer */
  (void) h; (void) t; (void) r; (void) v; (void) i; (void) to; if (n) b[0] = 0; return 0;
}
const char *libusb_error_name(int e) { (void) e; return "fake"; }
/* Transfers: by default every submit fails (init's selection is what most tests judge). With g_complete set, a
 * submit is queued and the next libusb_handle_events_timeout completes it in full; an IN transfer is filled with
 * the running byte sequence g_in_next, g_in_next + 1, ... */
static int g_complete;
static uint8_t g_in_next;
static volatile int g_in_fixed = -1;   /* >= 0: every IN byte is this value (the input pins), not the sequence */
static volatile int g_fail_all;        /* every submit fails, as on a device that is gone */
/* Which threads handled USB events, and when (the poll threads: one per running poller). */
#include <pthread.h>
#include <time.h>
static pthread_mutex_t g_thr_lock = PTHREAD_MUTEX_INITIALIZER;
static pthread_t g_thr[16];
static struct timespec g_thr_last[16];
static int g_nthr;
static void note_thread(void) {
  pthread_mutex_lock(&g_thr_lock);
  int i;
  for (i = 0; i < g_nthr && !pthread_equal(g_thr[i], pthread_self()); i++) {}
  if (i == g_nthr && g_nthr < 16) g_thr[g_nthr++] = pthread_self();
  if (i < 16) clock_gettime(CLOCK_MONOTONIC, &g_thr_last[i]);
  pthread_mutex_unlock(&g_thr_lock);
}
/* Threads that handled events in the last ms milliseconds, the caller excluded. */
static int active_threads(long ms) {
  struct timespec now; clock_gettime(CLOCK_MONOTONIC, &now);
  int n = 0;
  pthread_mutex_lock(&g_thr_lock);
  for (int i = 0; i < g_nthr; i++) {
    long age = (now.tv_sec - g_thr_last[i].tv_sec) * 1000 + (now.tv_nsec - g_thr_last[i].tv_nsec) / 1000000;
    if (!pthread_equal(g_thr[i], pthread_self()) && age < ms) n++;
  }
  pthread_mutex_unlock(&g_thr_lock);
  return n;
}
static struct libusb_transfer *g_queue[40];
static int g_nqueue;
struct libusb_transfer *libusb_alloc_transfer(int n) {
  (void) n;
  return g_complete ? calloc(1, sizeof(struct libusb_transfer)) : NULL;
}
void libusb_free_transfer(struct libusb_transfer *t) { free(t); }
int libusb_submit_transfer(struct libusb_transfer *t) {
  if (!g_complete || g_fail_all || g_nqueue == 40) return LIBUSB_ERROR_IO;
  g_queue[g_nqueue++] = t;
  return 0;
}
int libusb_cancel_transfer(struct libusb_transfer *t) { (void) t; return LIBUSB_ERROR_NOT_FOUND; }
int libusb_handle_events_timeout(libusb_context *c, struct timeval *tv) {
  (void) c; (void) tv;
  note_thread();
  int n = g_nqueue;
  g_nqueue = 0;
  for (int i = 0; i < n; i++) {
    struct libusb_transfer *t = g_queue[i];
    if (t->endpoint & 0x80)
      for (int k = 0; k < t->length; k++) t->buffer[k] = g_in_fixed >= 0 ? (uint8_t) g_in_fixed : g_in_next++;
    t->status = LIBUSB_TRANSFER_COMPLETED;
    t->actual_length = t->length;
    t->callback(t);
  }
  return 0;
}
void libusb_exit(libusb_context *c) { (void) c; }

#endif
