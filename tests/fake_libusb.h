/* A fake libusb for the library's tests: each fake device counts what was done to it. Include it, then the
 * library's source ("../libpinedio-usb.c"), then call fake_helpers.h's helpers. */
#ifndef FAKE_LIBUSB_H
#define FAKE_LIBUSB_H
#define _GNU_SOURCE
#include <assert.h>
#include <stdio.h>
#include <pthread.h>
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

static int g_fake_init_rc;              /* libusb_init's result */
int libusb_init(libusb_context **c) { (void) c; return g_fake_init_rc; }
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
  /* a real read leaves the rest of the buffer as it was: the library must zero it itself */
  size_t n = strlen(f->serial);
  memcpy(buf, f->serial, n < (size_t) len ? n : (size_t) len);
  return (int) n;
}
int libusb_control_transfer(libusb_device_handle *h, uint8_t t, uint8_t r, uint16_t v, uint16_t i, unsigned char *b,
                            uint16_t n, unsigned int to) {
  /* libusb_get_string_descriptor (inline in libusb.h) for the product string: an empty answer */
  (void) h; (void) t; (void) r; (void) v; (void) i; (void) to; if (n) b[0] = 0; return 0;
}
const char *libusb_error_name(int e) { (void) e; return "fake"; }
/* Transfers. By default every transfer fails (IO), as before. With g_fake_io_ok set, transfers are allocated,
 * a submit queues the transfer and libusb_handle_events_timeout completes the queue: an IN transfer reads the
 * scripted input word g_fake_in[0..2]; g_fake_fail makes every submit fail again (a gone device). Submit and
 * handle_events run under the library's USB mutex, so the queue needs no lock of its own. */
static int g_fake_io_ok;
static int g_fake_fail;                 /* atomic: set by the test while the worker polls */
static uint8_t g_fake_in[3];            /* atomic bytes: the input word the next IN transfer returns */
static int g_fake_freed;                /* transfers freed */
static int g_fake_submits;              /* submits that reached the device */
static uint8_t g_fake_last_out[4][4];   /* per device: the first 4 bytes of its last OUT transfer */
static int g_fake_fail_status;          /* atomic: queued transfers complete with LIBUSB_TRANSFER_NO_DEVICE */
static void (*g_fake_on_events)(void);  /* called at the start of every handle_events (a test hold point) */
static struct libusb_transfer *g_fake_q[4 * (8 + 1)];
static int g_fake_nq;
static int g_fake_events;               /* handle_events calls */
static int g_fake_alloc_fail_at = -1;   /* the n-th allocation (0 = first) fails */
static int g_fake_allocs;
/* The default libusb context is shared by every instance: two instances submit and handle events under their
 * own mutexes, so the queue has its own lock, and any thread's handle_events completes every queued transfer. */
static pthread_mutex_t g_fake_qm = PTHREAD_MUTEX_INITIALIZER;
struct libusb_transfer *libusb_alloc_transfer(int n) {
  (void) n;
  if (!g_fake_io_ok) return NULL;
  if (g_fake_allocs++ == g_fake_alloc_fail_at) return NULL;
  return calloc(1, sizeof(struct libusb_transfer));
}
void libusb_free_transfer(struct libusb_transfer *t) {
  if (!t) return;
  __atomic_fetch_add(&g_fake_freed, 1, __ATOMIC_SEQ_CST);
  if (g_fake_io_ok) free(t);
}
static int g_fake_short;               /* IN transfers complete with 0 bytes (1) or one byte short (2) */
static int g_fake_fail_in;             /* every IN submit fails (the OUT one goes through) */
static int g_fake_fail_in_after;       /* > 0: IN submits after that many fail (counted in g_fake_in_submits) */
static int g_fake_in_submits;
static int g_fake_complete_in_submit;  /* complete an IN transfer inside submit (another thread's events) */
static int g_fake_complete_out_in_submit;   /* the same for an OUT transfer */
static int g_fake_hold_out;            /* OUT transfers stay queued (never completed) while set */
static int g_fake_hold_in;             /* IN transfers likewise */
static int g_fake_out_short_at, g_fake_out_n;   /* the n-th OUT completion (counted from 0 when armed) is short */
static enum libusb_transfer_status g_fake_in_status[8];   /* IN completions' statuses, in order */
static int g_fake_in_status_n, g_fake_in_status_i;
static void fake_complete(struct libusb_transfer *t);
int libusb_submit_transfer(struct libusb_transfer *t) {
  if (!g_fake_io_ok || __atomic_load_n(&g_fake_fail, __ATOMIC_SEQ_CST)) return LIBUSB_ERROR_IO;
  if ((t->endpoint & 0x80) && __atomic_load_n(&g_fake_fail_in, __ATOMIC_SEQ_CST)) return LIBUSB_ERROR_IO;
  if ((t->endpoint & 0x80) && __atomic_load_n(&g_fake_fail_in_after, __ATOMIC_SEQ_CST) > 0 &&
      __atomic_add_fetch(&g_fake_in_submits, 1, __ATOMIC_SEQ_CST) > __atomic_load_n(&g_fake_fail_in_after, __ATOMIC_SEQ_CST))
    return LIBUSB_ERROR_IO;
  if (__atomic_load_n((t->endpoint & 0x80) ? &g_fake_complete_in_submit : &g_fake_complete_out_in_submit,
                      __ATOMIC_SEQ_CST)) {
    __atomic_fetch_add(&g_fake_submits, 1, __ATOMIC_SEQ_CST);
    fake_complete(t);                  /* done before submit returns, as another event thread could */
    return 0;
  }
  __atomic_fetch_add(&g_fake_submits, 1, __ATOMIC_SEQ_CST);
  pthread_mutex_lock(&g_fake_qm);
  if (!(t->endpoint & 0x80) && t->dev_handle)
    memcpy(g_fake_last_out[t->dev_handle->idx], t->buffer, t->length < 4 ? (size_t) t->length : 4);
  g_fake_q[g_fake_nq++] = t;
  pthread_mutex_unlock(&g_fake_qm);
  return 0;
}
static int g_fake_cancels;
static int g_fake_cancel_not_found;     /* cancel answers NOT_FOUND; the transfer completes in the next events */
int libusb_cancel_transfer(struct libusb_transfer *t) {
  (void) t;
  __atomic_fetch_add(&g_fake_cancels, 1, __ATOMIC_SEQ_CST);
  return __atomic_load_n(&g_fake_cancel_not_found, __ATOMIC_SEQ_CST) ? LIBUSB_ERROR_NOT_FOUND : 0;
}
static int fake_queued(void) { pthread_mutex_lock(&g_fake_qm); int n = g_fake_nq; pthread_mutex_unlock(&g_fake_qm); return n; }
int libusb_handle_events_timeout(libusb_context *c, struct timeval *tv) {
  (void) c; (void) tv;
  void (*on_events)(void) = __atomic_load_n(&g_fake_on_events, __ATOMIC_SEQ_CST);
  if (on_events) on_events();
  __atomic_fetch_add(&g_fake_events, 1, __ATOMIC_SEQ_CST);
  struct libusb_transfer *done[4 * (8 + 1)];
  pthread_mutex_lock(&g_fake_qm);
  int n = 0, kept = 0;
  for (int i = 0; i < g_fake_nq; i++) {          /* a held OUT transfer stays queued: its callback never comes */
    struct libusb_transfer *t = g_fake_q[i];
    if (__atomic_load_n((t->endpoint & 0x80) ? &g_fake_hold_in : &g_fake_hold_out, __ATOMIC_SEQ_CST)) g_fake_q[kept++] = t;
    else done[n++] = t;
  }
  g_fake_nq = kept;
  pthread_mutex_unlock(&g_fake_qm);
  for (int i = 0; i < n; i++)
    fake_complete(done[i]);
  return 0;
}
void libusb_exit(libusb_context *c) { (void) c; }

static void fake_complete(struct libusb_transfer *t) {
    t->status = __atomic_load_n(&g_fake_fail_status, __ATOMIC_SEQ_CST) ? LIBUSB_TRANSFER_NO_DEVICE
                                                                       : LIBUSB_TRANSFER_COMPLETED;
    if ((t->endpoint & 0x80) && g_fake_in_status_n > 0) {     /* a scripted status per IN completion */
      int k = __atomic_fetch_add(&g_fake_in_status_i, 1, __ATOMIC_SEQ_CST);
      if (k < g_fake_in_status_n) t->status = g_fake_in_status[k];
    }
    int short_mode = __atomic_load_n(&g_fake_short, __ATOMIC_SEQ_CST);   /* 1: zero bytes, 2: one short */
    t->actual_length = (t->endpoint & 0x80) && short_mode ? (short_mode == 1 ? 0 : t->length - 1) : t->length;
    if (!(t->endpoint & 0x80) && __atomic_load_n(&g_fake_out_short_at, __ATOMIC_SEQ_CST) > 0 &&
        __atomic_add_fetch(&g_fake_out_n, 1, __ATOMIC_SEQ_CST) == __atomic_load_n(&g_fake_out_short_at, __ATOMIC_SEQ_CST))
      t->actual_length = t->length - 1;                         /* this OUT completes one byte short */
    if (t->endpoint & 0x80)
      for (int k = 0; k < t->length && k < 3; k++) t->buffer[k] = __atomic_load_n(&g_fake_in[k], __ATOMIC_SEQ_CST);
    t->callback(t);
}

#endif
