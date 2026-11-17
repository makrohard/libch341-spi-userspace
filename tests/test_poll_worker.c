/* The poll worker: one joinable thread per instance, started by init and joined by deinit; attach and detach
 * only enable or disable a pin; deinit sets CLOSING, joins the worker and waits for every counted caller before
 * it frees or destroys anything. Each case runs in a forked child: a hang (the old lifecycle) is turned into a
 * failure by alarm(); nothing synchronises by sleeping. Threads are ordered by hold points (PINEDIO_TEST_HOOK)
 * and by records taken where a join or the library's mutex orders them. Run: tests/run.sh */
#include "fake_libusb.h"
#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <signal.h>
#include <stdlib.h>
#include <sys/ioctl.h>
#include <sys/socket.h>
#include <sys/wait.h>
#include <dirent.h>
#include <poll.h>
#include <sched.h>
#include <time.h>
#include <unistd.h>

/* ---- hold points and records (real pthread calls: defined before the wrapping macros) ---------------------- */
static pthread_mutex_t g_m = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t g_c = PTHREAD_COND_INITIALIZER;
static int g_seq;

struct hold { int armed, arrived, released; pthread_t only; int only_set; };
static struct hold H_in_read, H_before_cb, H_api_entered;

static void hold_point(struct hold *h) {
  pthread_mutex_lock(&g_m);
  if (h->armed && !h->released && (!h->only_set || pthread_equal(h->only, pthread_self()))) {
    h->arrived = 1;
    pthread_cond_broadcast(&g_c);
    while (!h->released) pthread_cond_wait(&g_c, &g_m);
  }
  pthread_mutex_unlock(&g_m);
}
static void arm(struct hold *h) { pthread_mutex_lock(&g_m); memset(h, 0, sizeof(*h)); h->armed = 1; pthread_mutex_unlock(&g_m); }
static void wait_arrived(struct hold *h) { pthread_mutex_lock(&g_m); while (!h->arrived) pthread_cond_wait(&g_c, &g_m); pthread_mutex_unlock(&g_m); }
static void release(struct hold *h) { pthread_mutex_lock(&g_m); h->released = 1; pthread_cond_broadcast(&g_c); pthread_mutex_unlock(&g_m); }

/* named events: the sequence number of the first occurrence, and counts */
enum { EV_WORKER_EXIT, EV_DEINIT_ENTERED, EV_JOIN_ENTER, EV_JOIN_RETURN, EV_DEINIT_COND_WAIT, EV_DEINIT_PAST_WAIT,
       EV_API_EXIT_D, EV_CB_A, EV_CB_B, EV_API_ENTERED, EV_MAX };
static int g_ev_seq[EV_MAX], g_ev_n[EV_MAX], g_polls;
static pthread_t g_deinit_thread, g_d_thread, g_tb_thread;
static int g_deinit_thread_set, g_d_thread_set, g_tb_set;

static void event(int e) {
  pthread_mutex_lock(&g_m);
  if (g_ev_n[e]++ == 0) g_ev_seq[e] = ++g_seq;
  pthread_cond_broadcast(&g_c);
  pthread_mutex_unlock(&g_m);
}
static int ev_n(int e) { pthread_mutex_lock(&g_m); int n = g_ev_n[e]; pthread_mutex_unlock(&g_m); return n; }
static void wait_event(int e) { pthread_mutex_lock(&g_m); while (!g_ev_n[e]) pthread_cond_wait(&g_c, &g_m); pthread_mutex_unlock(&g_m); }
/* the first of two events; returns which */
static int wait_either(int a, int b) {
  pthread_mutex_lock(&g_m);
  while (!g_ev_n[a] && !g_ev_n[b]) pthread_cond_wait(&g_c, &g_m);
  int r = g_ev_n[a] ? a : b;
  pthread_mutex_unlock(&g_m);
  return r;
}
static int polls(void) { pthread_mutex_lock(&g_m); int n = g_polls; pthread_mutex_unlock(&g_m); return n; }
static void wait_polls(int target) { pthread_mutex_lock(&g_m); while (g_polls < target) pthread_cond_wait(&g_c, &g_m); pthread_mutex_unlock(&g_m); }
static int is_thread(pthread_t *t, int *set) {
  pthread_mutex_lock(&g_m);
  int r = *set && pthread_equal(*t, pthread_self());
  pthread_mutex_unlock(&g_m);
  return r;
}
static int is_deinit_thread(void) { return is_thread(&g_deinit_thread, &g_deinit_thread_set); }
static int is_tb(void) { return is_thread(&g_tb_thread, &g_tb_set); }
static void register_self(pthread_t *t, int *set) {
  pthread_mutex_lock(&g_m); *t = pthread_self(); *set = 1; pthread_mutex_unlock(&g_m);
}
static int is_d_thread(void) {
  pthread_mutex_lock(&g_m);
  int r = g_d_thread_set && pthread_equal(g_d_thread, pthread_self());
  pthread_mutex_unlock(&g_m);
  return r;
}

static int g_close_at_diag = -1;   /* S37g: the pipe's read end, closed between diag_write's open and its write */
static void test_hook(const char *name) {
  if (!strcmp(name, "diag_opened")) {
    if (g_close_at_diag >= 0) { close(g_close_at_diag); g_close_at_diag = -1; }
  } else if (!strcmp(name, "in_read")) {
    pthread_mutex_lock(&g_m); g_polls++; pthread_cond_broadcast(&g_c); pthread_mutex_unlock(&g_m);
    hold_point(&H_in_read);
  } else if (!strcmp(name, "before_callbacks")) hold_point(&H_before_cb);
  else if (!strcmp(name, "api_entered")) { event(EV_API_ENTERED); if (is_d_thread()) hold_point(&H_api_entered); }
  else if (!strcmp(name, "api_exit")) { if (is_d_thread()) event(EV_API_EXIT_D); }
  else if (!strcmp(name, "worker_exit")) event(EV_WORKER_EXIT);
  else if (!strcmp(name, "deinit_entered")) event(EV_DEINIT_ENTERED);
  else if (!strcmp(name, "deinit_past_wait")) event(EV_DEINIT_PAST_WAIT);
}

/* ---- wrappers for the library's pthread calls ---------------------------------------------------------------- */
static int g_created, g_joined, g_fail_create, g_spurious, g_fail_cond_init, g_mutex_destroyed, g_cond_destroyed;
static int g_fail_join, g_cond_clock = -1;
static struct timespec g_deadlines[16];
static int g_deadline_polls[16], g_ndeadlines;

static int w_create(pthread_t *t, const pthread_attr_t *a, void *(*f)(void *), void *arg) {
  if (g_fail_create) return EAGAIN;
  __atomic_fetch_add(&g_created, 1, __ATOMIC_SEQ_CST);
  return pthread_create(t, a, f, arg);
}
static int w_join(pthread_t t, void **r, clockid_t c, const struct timespec *end) {
  event(EV_JOIN_ENTER);
  if (g_fail_join) return EINVAL;
  int rc = pthread_clockjoin_np(t, r, c, end);
  if (rc == 0) __atomic_fetch_add(&g_joined, 1, __ATOMIC_SEQ_CST);
  event(EV_JOIN_RETURN);
  return rc;
}
static int w_cond_init(pthread_cond_t *c, const pthread_condattr_t *a) {
  if (g_fail_cond_init) return ENOMEM;
  return pthread_cond_init(c, a);
}
static int w_mutex_destroy(pthread_mutex_t *m) { g_mutex_destroyed++; return pthread_mutex_destroy(m); }
static void *g_watch_mutex;             /* a mutex whose locks are counted (S24) */
static int g_watched_locks;
static int w_mutex_lock(pthread_mutex_t *m) {
  if (m == g_watch_mutex) __atomic_fetch_add(&g_watched_locks, 1, __ATOMIC_SEQ_CST);
  return pthread_mutex_lock(m);
}
static int w_condattr_setclock(pthread_condattr_t *a, clockid_t c) { g_cond_clock = (int) c; return pthread_condattr_setclock(a, c); }
static int w_cond_destroy(pthread_cond_t *c) { g_cond_destroyed++; return pthread_cond_destroy(c); }
static int w_cond_wait(pthread_cond_t *c, pthread_mutex_t *m) {
  if (is_deinit_thread()) event(EV_DEINIT_COND_WAIT);
  return pthread_cond_wait(c, m);
}
static int w_cond_timedwait(pthread_cond_t *c, pthread_mutex_t *m, const struct timespec *at) {
  if (is_deinit_thread()) {                       /* deinit's bounded wait: not a worker deadline */
    event(EV_DEINIT_COND_WAIT);
    return pthread_cond_timedwait(c, m, at);
  }
  pthread_mutex_lock(&g_m);
  int spurious = g_spurious > 0;
  if (g_ndeadlines < 16) { g_deadlines[g_ndeadlines] = *at; g_deadline_polls[g_ndeadlines] = g_polls; g_ndeadlines++; }
  if (spurious) g_spurious--;
  pthread_cond_broadcast(&g_c);
  pthread_mutex_unlock(&g_m);
  if (spurious) return 0;                         /* a spurious wakeup: back at once, the mutex still held */
  return pthread_cond_timedwait(c, m, at);
}

#define PINEDIO_TEST_HOOK(name) test_hook(#name)
#define PINEDIO_DRAIN_MS 1000  /* the cleanup's drain deadline and deinit's: short for S30-S32, long enough for
                                  * the cases that hold a thread while deinit waits */
#define PINEDIO_XFER_MS 200    /* a request's progress deadline, short for S31 */
#define pthread_create w_create
#define pthread_clockjoin_np w_join
#define pthread_cond_wait w_cond_wait
#define pthread_cond_timedwait w_cond_timedwait
#define pthread_cond_init w_cond_init
#define pthread_mutex_destroy w_mutex_destroy
#define pthread_cond_destroy w_cond_destroy
#define pthread_condattr_setclock w_condattr_setclock
#define pthread_mutex_lock w_mutex_lock
#include "../libpinedio-usb.c"
#undef pthread_create
#undef pthread_clockjoin_np
#undef pthread_cond_wait
#undef pthread_cond_timedwait
#undef pthread_cond_init
#undef pthread_mutex_destroy
#undef pthread_cond_destroy
#undef pthread_condattr_setclock
#undef pthread_mutex_lock
#include "fake_helpers.h"

/* ---- fixtures ------------------------------------------------------------------------------------------------ */
static struct pinedio_inst *g_inst;
static int g_cb_a, g_cb_b, g_cb_rc1, g_cb_rc2;
static void cb_a(void) { __atomic_fetch_add(&g_cb_a, 1, __ATOMIC_SEQ_CST); event(EV_CB_A); }
static void cb_b(void) { __atomic_fetch_add(&g_cb_b, 1, __ATOMIC_SEQ_CST); event(EV_CB_B); }

static void set_input(uint8_t d) { __atomic_store_n(&g_fake_in[0], d, __ATOMIC_SEQ_CST); }

static struct pinedio_inst *open_inst(void) {
  devices(1);
  g_fake_io_ok = 1;
  struct pinedio_inst *inst = calloc(1, sizeof(*inst));
  *inst = fresh(NULL);
  assert(pinedio_init(inst, NULL) == 0);
  g_inst = inst;
  return inst;
}
static void close_inst(struct pinedio_inst *inst) {
  pinedio_deinit(inst);
  memset(inst, 0xA5, sizeof(*inst));             /* a thread still using it fails */
  free(inst);
}
/* attach D0 and let one poll set its previous state, so the next change is an edge */
static void attach_d0(struct pinedio_inst *inst, void (*cb)(void)) {
  set_input(0);
  int p = polls();
  assert(pinedio_attach_interrupt(inst, PINEDIO_PIN_D0, PINEDIO_INT_MODE_RISING, cb) == 0);
  wait_polls(p + 2);
}

static void *deinit_thread(void *arg) {
  pthread_mutex_lock(&g_m); g_deinit_thread = pthread_self(); g_deinit_thread_set = 1; pthread_mutex_unlock(&g_m);
  pinedio_deinit(arg);
  return NULL;
}
static pthread_t start_deinit(struct pinedio_inst *inst) {
  pthread_t t;
  assert(pthread_create(&t, NULL, deinit_thread, inst) == 0);
  return t;
}

static long ms_since(const struct timespec *t0) {
  struct timespec t1;
  clock_gettime(CLOCK_MONOTONIC, &t1);
  return (long) ((t1.tv_sec - t0->tv_sec) * 1000 + (t1.tv_nsec - t0->tv_nsec) / 1000000);
}
/* A failing call that ends by its error, not by the progress deadline (PINEDIO_XFER_MS). */
static int fails_at_once(struct pinedio_inst *inst, uint8_t *w, uint8_t *r, uint32_t n) {
  struct timespec t0;
  clock_gettime(CLOCK_MONOTONIC, &t0);
  int rc = pinedio_transceive(inst, w, r, n);
  return rc == -1 && ms_since(&t0) < PINEDIO_XFER_MS;
}

/* ---- cases --------------------------------------------------------------------------------------------------- */

/* S0, through the public path only (no hook, no wrapper): the threads alive in this process. After init and 10
 * re-attaches of one pin the process has the main thread and one poll worker; after deinit only the main thread.
 * On 98b656bf every re-attach started another poll thread and deinit joined only the newest: 11, then 10. */
static int live_threads(void) {
  DIR *d = opendir("/proc/self/task");
  int n = 0;
  struct dirent *e;
  while (d && (e = readdir(d))) if (e->d_name[0] != '.') n++;
  if (d) closedir(d);
  return n;
}
static void s0(void) {
  devices(1);
  g_fake_io_ok = 1;
  struct pinedio_inst *inst = calloc(1, sizeof(*inst));
  *inst = fresh(NULL);
  assert(pinedio_init(inst, NULL) == 0);
  for (int k = 0; k < 10; k++)
    assert(pinedio_attach_interrupt(inst, PINEDIO_PIN_D0, PINEDIO_INT_MODE_RISING, cb_a) == 0);
  int alive = live_threads();
  pinedio_deinit(inst);
  /* A joined thread can still be listed for a moment after pthread_join returns (the kernel reaps it after it
   * wakes the joiner): spin until it is gone; the child's alarm bounds it. */
  int after;
  while ((after = live_threads()) > 1) sched_yield();
  printf("     S0: threads after 10 re-attaches %d, after deinit %d\n", alive, after);
  fflush(stdout);
  assert(alive == 2 && after == 1);
  free(inst);
}

/* S1: 10 re-attaches and 10 detach/attach pairs never start a thread; the one worker comes from init. */
static void s1(void) {
  struct pinedio_inst *inst = open_inst();
  assert(g_created == 1);
  for (int k = 0; k < 10; k++)
    assert(pinedio_attach_interrupt(inst, PINEDIO_PIN_D0, PINEDIO_INT_MODE_RISING, cb_a) == 0);
  assert(g_created == 1 && inst->int_running_cnt == 1);
  for (int k = 0; k < 10; k++) {
    assert(pinedio_deattach_interrupt(inst, PINEDIO_PIN_D0) == 0 && inst->int_running_cnt == 0);
    assert(pinedio_attach_interrupt(inst, PINEDIO_PIN_D0, PINEDIO_INT_MODE_RISING, cb_a) == 0);
  }
  assert(g_created == 1);
  close_inst(inst);
  assert(g_joined == 1);
}

/* S2: attach and detach while the worker is inside a poll (it holds the mutex in its read): each call waits
 * for the mutex and then takes effect. The order is forced: attach B in one held poll, detach A in the next. */
static void *attach_b(void *arg) { return (void *) (intptr_t) pinedio_attach_interrupt(arg, PINEDIO_PIN_D1, PINEDIO_INT_MODE_RISING, cb_b); }
static void *detach_a(void *arg) { return (void *) (intptr_t) pinedio_deattach_interrupt(arg, PINEDIO_PIN_D0); }
static void during_held_poll(struct pinedio_inst *inst, void *(*fn)(void *)) {
  arm(&H_in_read);
  wait_arrived(&H_in_read);
  pthread_t t;
  void *r;
  int entered = ev_n(EV_API_ENTERED);
  assert(pthread_create(&t, NULL, fn, inst) == 0);
  pthread_mutex_lock(&g_m);                      /* the helper is counted and goes for the mutex the worker holds */
  while (g_ev_n[EV_API_ENTERED] == entered) pthread_cond_wait(&g_c, &g_m);
  pthread_mutex_unlock(&g_m);
  release(&H_in_read);
  pthread_join(t, &r);
  assert((intptr_t) r == 0);
}
static void s2(void) {
  struct pinedio_inst *inst = open_inst();
  attach_d0(inst, cb_a);
  during_held_poll(inst, attach_b);
  during_held_poll(inst, detach_a);
  assert(inst->int_running_cnt == 1 && inst->interrupts[PINEDIO_PIN_D0].callback == NULL);
  int p = polls();
  wait_polls(p + 2);                              /* D1's previous state is set */
  set_input(0x03);                                /* D0 and D1 rise */
  wait_event(EV_CB_B);
  assert(g_cb_a == 0 && g_created == 1);
  close_inst(inst);
}

/* S3: deinit while the worker is held inside its read: the worker exits before the join returns, and nothing
 * is freed before that (the join orders it). */
static void s3(void) {
  struct pinedio_inst *inst = open_inst();
  attach_d0(inst, cb_a);
  arm(&H_in_read);
  wait_arrived(&H_in_read);
  pthread_t h = start_deinit(inst);
  wait_event(EV_DEINIT_ENTERED);
  int freed_before = g_fake_freed;
  release(&H_in_read);
  pthread_join(h, NULL);
  assert(g_ev_n[EV_WORKER_EXIT] == 1 && g_ev_seq[EV_WORKER_EXIT] < g_ev_seq[EV_JOIN_RETURN]);
  assert(g_ev_seq[EV_JOIN_RETURN] < g_ev_seq[EV_DEINIT_PAST_WAIT]);
  assert(freed_before == 0 && g_fake_freed == USB_IN_TRANSFERS + 1 && g_devs[0].closed == 1);
  memset(inst, 0xA5, sizeof(*inst));
  free(inst);
}

/* S3b, through the public path (no library hook; the fake's event handling is the hold point): 10
 * re-attaches; every poller that enters the fake's handle_events is held there (its read in progress). While
 * held, this process holds main and exactly one poller; at 98b656bf it held 10 pollers (11 threads). Then the
 * pollers are released, deinit returns and only main is left. No edge and no previous state are involved. */
static pthread_t g_main_thread;
static struct hold H_poller;
static void poller_on_events(void) { if (!pthread_equal(pthread_self(), g_main_thread)) hold_point(&H_poller); }
static void s3b(void) {
  devices(1);
  g_fake_io_ok = 1;
  g_main_thread = pthread_self();
  struct pinedio_inst *inst = calloc(1, sizeof(*inst));
  *inst = fresh(NULL);
  assert(pinedio_init(inst, NULL) == 0);
  for (int k = 0; k < 10; k++)
    assert(pinedio_attach_interrupt(inst, PINEDIO_PIN_D0, PINEDIO_INT_MODE_RISING, cb_a) == 0);
  arm(&H_poller);
  __atomic_store_n(&g_fake_on_events, poller_on_events, __ATOMIC_SEQ_CST);
  wait_arrived(&H_poller);                        /* a poller is held inside its read */
  int during = live_threads();
  printf("     S3b: threads while a poller is held in its read %d\n", during);
  fflush(stdout);
  assert(during == 2);
  __atomic_store_n(&g_fake_on_events, NULL, __ATOMIC_SEQ_CST);
  release(&H_poller);
  assert(pinedio_deinit(inst) == 0);
  int after;
  while ((after = live_threads()) > 1) sched_yield();
  free(inst);
}

/* S4: deinit with the worker waiting: (a) idle, no pin; (b) in its interval wait, one pin. */
static void s4a(void) { struct pinedio_inst *inst = open_inst(); close_inst(inst); assert(g_joined == 1 && ev_n(EV_WORKER_EXIT) == 1); }
static void s4b(void) {                          /* public path: no hook wait */
  devices(1);
  g_fake_io_ok = 1;
  struct pinedio_inst *inst = calloc(1, sizeof(*inst));
  *inst = fresh(NULL);
  assert(pinedio_init(inst, NULL) == 0);
  assert(pinedio_attach_interrupt(inst, PINEDIO_PIN_D0, PINEDIO_INT_MODE_RISING, cb_a) == 0);
  pthread_mutex_lock(&g_m);                      /* the worker has entered its interval wait */
  while (g_ndeadlines == 0) pthread_cond_wait(&g_c, &g_m);
  pthread_mutex_unlock(&g_m);
  assert(pinedio_deinit(inst) == 0 && g_joined == 1);
  int after;
  while ((after = live_threads()) > 1) sched_yield();
  free(inst);
}

/* S5: deinit with an API caller between its count and its lock: deinit must wait for it before it frees or
 * destroys anything. */
static int g_d_rc = 99;
static void *d_thread(void *arg) {
  pthread_mutex_lock(&g_m); g_d_thread = pthread_self(); g_d_thread_set = 1; pthread_mutex_unlock(&g_m);
  uint8_t buf[4] = {0};
  g_d_rc = pinedio_transceive(arg, buf, buf, sizeof(buf));
  return NULL;
}
static void s5(void) {
  struct pinedio_inst *inst = open_inst();
  attach_d0(inst, cb_a);
  arm(&H_before_cb);
  set_input(0x01);                                /* an edge: the worker copies cb_a and holds before calling */
  wait_arrived(&H_before_cb);
  pthread_t h = start_deinit(inst);
  wait_event(EV_DEINIT_COND_WAIT);                /* closing is set, deinit waits for the worker and callers */
  pthread_t d;
  arm(&H_api_entered);                            /* held only for D (it registers itself first) */
  assert(pthread_create(&d, NULL, d_thread, inst) == 0);
  wait_arrived(&H_api_entered);                   /* D is counted, not yet locked */
  int waits = ev_n(EV_DEINIT_COND_WAIT);
  release(&H_before_cb);                          /* the worker calls cb_a, sees closing, exits */
  wait_event(EV_WORKER_EXIT);
  pthread_mutex_lock(&g_m);                       /* deinit, woken by the worker's exit, waits again or passes */
  while (g_ev_n[EV_DEINIT_COND_WAIT] <= waits && !g_ev_n[EV_DEINIT_PAST_WAIT]) pthread_cond_wait(&g_c, &g_m);
  int past = g_ev_n[EV_DEINIT_PAST_WAIT];
  pthread_mutex_unlock(&g_m);
  assert(!past);                                  /* the worker is gone, D is counted: deinit still waits */
  release(&H_api_entered);
  pthread_join(d, NULL);
  pthread_join(h, NULL);
  assert(g_d_rc == -1);
  assert(g_ev_seq[EV_API_EXIT_D] < g_ev_seq[EV_DEINIT_PAST_WAIT]);
  memset(inst, 0xA5, sizeof(*inst));
  free(inst);
}

/* S6: a callback attaching during CLOSING is refused; the worker still exits and deinit returns. */
static void cb_self_attach(void) {
  g_cb_rc1 = pinedio_attach_interrupt(g_inst, PINEDIO_PIN_D1, PINEDIO_INT_MODE_RISING, cb_b);
}
static void s6(void) {
  struct pinedio_inst *inst = open_inst();
  attach_d0(inst, cb_self_attach);
  g_cb_rc1 = 99;
  arm(&H_before_cb);
  set_input(0x01);
  wait_arrived(&H_before_cb);
  pthread_t h = start_deinit(inst);
  wait_event(EV_DEINIT_COND_WAIT);
  release(&H_before_cb);
  pthread_join(h, NULL);
  assert(g_cb_rc1 == -1 && g_created == 1 && g_joined == 1 && ev_n(EV_WORKER_EXIT) == 1);
  memset(inst, 0xA5, sizeof(*inst));
  free(inst);
}

/* S7: a callback detaching and re-attaching its own pin: no deadlock, one thread, the pin enabled again. */
static void cb_self_cycle(void) {
  g_cb_rc1 = pinedio_deattach_interrupt(g_inst, PINEDIO_PIN_D0);
  g_cb_rc2 = pinedio_attach_interrupt(g_inst, PINEDIO_PIN_D0, PINEDIO_INT_MODE_RISING, cb_a);
}
static void s7(void) {
  struct pinedio_inst *inst = open_inst();
  attach_d0(inst, cb_self_cycle);
  set_input(0x01);
  int p = polls();
  wait_polls(p + 3);
  assert(g_cb_rc1 == 0 && g_cb_rc2 == 0 && g_created == 1);
  assert(inst->interrupts[PINEDIO_PIN_D0].callback == cb_a && inst->int_running_cnt == 1);
  close_inst(inst);
}

/* S8: a failing read sets in_error, logs once, and the worker keeps going; a later good read does not clear it. */
static void s8(void) {
  struct pinedio_inst *inst = open_inst();
  attach_d0(inst, cb_a);
  fflush(stderr);
  FILE *log = tmpfile();
  int saved = dup(2);
  dup2(fileno(log), 2);
  __atomic_store_n(&g_fake_fail, 1, __ATOMIC_SEQ_CST);
  int p = polls();
  wait_polls(p + 4);
  assert(inst->in_error);
  __atomic_store_n(&g_fake_fail, 0, __ATOMIC_SEQ_CST);
  p = polls();
  wait_polls(p + 3);
  assert(inst->in_error && ev_n(EV_WORKER_EXIT) == 0);
  assert(pinedio_attach_interrupt(inst, PINEDIO_PIN_D0, PINEDIO_INT_MODE_RISING, cb_a) == 0 && g_created == 1);
  fflush(stderr);
  dup2(saved, 2);
  rewind(log);
  char line[256];
  int failed_lines = 0, lines = 0;
  while (fgets(line, sizeof(line), log)) {
    lines++;
    if (strstr(line, "Failed to get input")) failed_lines++;
  }
  printf("     S8: %d stderr lines over 7+ failing polls\n", lines);
  fflush(stdout);
  assert(failed_lines == 1 && lines <= 3);        /* the first failure's lines only, however many polls */
  close_inst(inst);
}

/* S8b: transfers that are submitted but complete with an error (the transfer callbacks log): bounded too. */
static void s8b(void) {
  struct pinedio_inst *inst = open_inst();
  attach_d0(inst, cb_a);
  fflush(stderr);
  FILE *log = tmpfile();
  int saved = dup(2);
  dup2(fileno(log), 2);
  __atomic_store_n(&g_fake_fail_status, 1, __ATOMIC_SEQ_CST);
  int p = polls();
  wait_polls(p + 8);
  assert(__atomic_load_n(&inst->in_error, __ATOMIC_SEQ_CST));
  fflush(stderr);
  dup2(saved, 2);
  rewind(log);
  char line[256];
  int lines = 0, first = 0;
  while (fgets(line, sizeof(line), log)) { lines++; if (!strncmp(line, "Failed to get input", 19)) first++; }
  printf("     S8b: %d stderr lines over 8 failing polls\n", lines);
  fflush(stdout);
  assert(lines <= 4 && first == 1);   /* bounded, and the first failure is reported (after the worker's unlock) */
  __atomic_store_n(&g_fake_fail_status, 0, __ATOMIC_SEQ_CST);
  close_inst(inst);
}

/* S9: spurious wakeups re-wait for the same absolute deadline, on the monotonic clock, with no read between. */
static void s9(void) {
  struct pinedio_inst *inst = open_inst();
  attach_d0(inst, cb_a);
  pthread_mutex_lock(&g_m);
  g_ndeadlines = 0;
  g_spurious = 5;
  while (g_ndeadlines < 6) pthread_cond_wait(&g_c, &g_m);
  struct timespec first = g_deadlines[0];
  for (int i = 1; i < 6; i++) {
    assert(g_deadlines[i].tv_sec == first.tv_sec && g_deadlines[i].tv_nsec == first.tv_nsec);
    assert(g_deadline_polls[i] == g_deadline_polls[0]);
  }
  pthread_mutex_unlock(&g_m);
  struct timespec now;
  clock_gettime(CLOCK_MONOTONIC, &now);
  long long ahead = (first.tv_sec - now.tv_sec) * 1000000000LL + (first.tv_nsec - now.tv_nsec);
  assert(ahead < 1000000000LL && ahead > -1000000000LL);   /* a CLOCK_REALTIME deadline is decades off */
  assert(g_cond_clock == CLOCK_MONOTONIC);                   /* the condvar waits on the same clock */
  close_inst(inst);
}

/* S10: deinit from a callback is refused and frees nothing; deinit from the main thread then frees once. */
static int g_wrapper_freed, g_deinit_rc = 99;
static void cb_deinit(void) {                    /* a wrapper's close, as ch341_adapter.c's: free only on 0 */
  g_deinit_rc = pinedio_deinit(g_inst);
  if (g_deinit_rc == 0) g_wrapper_freed = 1;
}
static void s10(void) {
  struct pinedio_inst *inst = open_inst();
  attach_d0(inst, cb_deinit);
  set_input(0x01);
  int p = polls();
  wait_polls(p + 2);
  assert(g_deinit_rc == -1 && g_wrapper_freed == 0 && g_fake_freed == 0 && g_devs[0].closed == 0);
  close_inst(inst);
  assert(g_fake_freed == USB_IN_TRANSFERS + 1 && g_devs[0].closed == 1 && g_joined == 1);
}

/* S11: every init failure after the mutex exists gives the mutex and condvar back, starts and joins nothing,
 * and returns the code the adapter expects (-1: no libusb reference taken; -2/-3: taken). */
static void init_fails(int expect_rc, int expect_mutex, int expect_cond) {
  struct pinedio_inst *inst = calloc(1, sizeof(*inst));
  *inst = fresh(NULL);
  assert(pinedio_init(inst, NULL) == expect_rc);
  assert(g_created == 0 && g_joined == 0);
  assert(g_mutex_destroyed == expect_mutex && g_cond_destroyed == expect_cond);
  free(inst);
}
static void s11a(void) { devices(1); g_fake_io_ok = 1; g_fake_init_rc = LIBUSB_ERROR_OTHER; init_fails(-1, 1, 1); }
static void s11b(void) { devices(0); g_fake_io_ok = 1; init_fails(-2, 1, 1); }
static void s11c(void) { devices(1); g_fake_io_ok = 1; g_fail_cond_init = 1; init_fails(-1, 1, 0); }   /* no condvar to destroy */
static void s11d(void) {
  devices(1); g_fake_io_ok = 1; g_fail_create = 1;
  init_fails(-3, 1, 1);
  assert(g_devs[0].closed == 1 && g_fake_freed == USB_IN_TRANSFERS + 1);
}

/* S11f: an instance that is uninitialised caller memory (upstream's test.c passes a stack struct as is): its
 * pointer fields hold garbage. With no device init returns -2; with an IN transfer that cannot be allocated it
 * returns -3 and frees only what it allocated. Neither path looks at the garbage. */
static void s11f_garbage(struct pinedio_inst *inst) {
  memset(inst, 0xA5, sizeof(*inst));
  memset(inst->options, 0, sizeof(inst->options));
  memset(inst->serial_number, 0, sizeof(inst->serial_number));
  inst->options[PINEDIO_OPTION_VID] = 0x1A86;
  inst->options[PINEDIO_OPTION_PID] = 0x5512;
}
static void s11f(void) {
  struct pinedio_inst *inst = malloc(sizeof(*inst));
  devices(0); g_fake_io_ok = 1;
  s11f_garbage(inst);
  assert(pinedio_init(inst, NULL) == -2);
  devices(1); g_fake_allocs = 0; g_fake_alloc_fail_at = 1; g_fake_freed = 0;   /* OUT ok, IN[0] fails */
  s11f_garbage(inst);
  assert(pinedio_init(inst, NULL) == -3 && g_fake_freed == 1 && g_devs[0].closed == 1);
  free(inst);
}

/* S18, through the public path: pinedio_transceive returns the received bytes bit-reversed (the CH341 shifts
 * LSB first), each at its own index. */
static void s18(void) {
  struct pinedio_inst *inst = open_inst();
  __atomic_store_n(&g_fake_in[0], 0x01, __ATOMIC_SEQ_CST);
  __atomic_store_n(&g_fake_in[1], 0x02, __ATOMIC_SEQ_CST);
  __atomic_store_n(&g_fake_in[2], 0xF0, __ATOMIC_SEQ_CST);
  uint8_t w[3] = {0}, r[3] = {0};
  assert(pinedio_transceive(inst, w, r, sizeof(r)) == 0);
  printf("     S18: %02x %02x %02x\n", r[0], r[1], r[2]);
  fflush(stdout);
  assert(r[0] == 0x80 && r[1] == 0x40 && r[2] == 0x0F);
  close_inst(inst);
}

/* S19, through the public path: an IN transfer that completes before its submit returns (another instance's
 * worker can handle the events first). The call returns its data; marking the transfer active after the
 * submit overwrote the completion and the call waited forever (the child's alarm). */
static void s19(void) {
  struct pinedio_inst *inst = open_inst();
  __atomic_store_n(&g_fake_in[0], 0x01, __ATOMIC_SEQ_CST);
  __atomic_store_n(&g_fake_complete_in_submit, 1, __ATOMIC_SEQ_CST);
  uint8_t w[3] = {0}, r[3] = {0};
  assert(pinedio_transceive(inst, w, r, sizeof(r)) == 0 && r[0] == 0x80);
  __atomic_store_n(&g_fake_complete_in_submit, 0, __ATOMIC_SEQ_CST);
  close_inst(inst);
}

/* S20, through the public path: the OUT submit of a read fails (a disconnect during an SPI read). The cleanup
 * looks only at IN transfers this call submitted: none here, so nothing is cancelled and the call returns -1.
 * The stack is sprayed with TRANS_ACTIVE first: an IN state read before it was set looks active, the cleanup
 * cancels it and waits forever (the alarm). Without -ftrivial-auto-var-init the spray reaches the old code's
 * uninitialised states; with it (tests/run.sh) this is a regression case. */
static __attribute__((noinline)) void spray_stack(void) {
  volatile int a[4096];
  for (int i = 0; i < 4096; i++) a[i] = TRANS_ACTIVE;
  (void) a[0];
}
static void s20(void) {
  struct pinedio_inst *inst = open_inst();
  uint8_t w[3] = {0}, r[3] = {0};
  __atomic_store_n(&g_fake_fail, 1, __ATOMIC_SEQ_CST);
  int cancels = g_fake_cancels;
  spray_stack();
  assert(pinedio_transceive(inst, w, r, sizeof(r)) == -1);
  assert(g_fake_cancels == cancels);
  __atomic_store_n(&g_fake_fail, 0, __ATOMIC_SEQ_CST);
  close_inst(inst);
}

/* S21, through the public path: the OUT submit of a read goes through and the first IN submit fails. The
 * failed IN transfer is marked failed, so the cleanup cancels only the submitted OUT transfer (one cancel) and
 * the call returns -1. Without that mark the IN transfer still looks active (it is marked before its submit):
 * the cleanup cancels a transfer that was never submitted and waits for it forever (the child's alarm). */
static void s21(void) {
  struct pinedio_inst *inst = open_inst();
  uint8_t w[3] = {0}, r[3] = {0};
  int cancels = __atomic_load_n(&g_fake_cancels, __ATOMIC_SEQ_CST);
  __atomic_store_n(&g_fake_fail_in, 1, __ATOMIC_SEQ_CST);
  assert(pinedio_transceive(inst, w, r, sizeof(r)) == -1);
  __atomic_store_n(&g_fake_fail_in, 0, __ATOMIC_SEQ_CST);
  assert(__atomic_load_n(&g_fake_cancels, __ATOMIC_SEQ_CST) == cancels + 1);
  /* the next read starts from fresh transfer states (the request lives in the instance): it succeeds */
  assert(pinedio_transceive(inst, w, r, sizeof(r)) == 0);
  close_inst(inst);
}

/* S22, through the public path: a read whose IN submit fails, so its submitted OUT transfer must be cancelled,
 * and the cancel answers NOT_FOUND (the transfer is completing). The call returns -1 only after that
 * transfer's callback has run: when it returns, no transfer of it is still queued. Marking the transfer failed
 * on the cancel error returned with the OUT transfer still queued; its callback would then write into the
 * returned call's stack. */
static void s22(void) {
  struct pinedio_inst *inst = open_inst();
  uint8_t w[3] = {0}, r[3] = {0};
  assert(pinedio_set_option(inst, PINEDIO_OPTION_AUTO_CS, 0) == 0);   /* no CS write after the read drains it */
  __atomic_store_n(&g_fake_fail_in, 1, __ATOMIC_SEQ_CST);
  __atomic_store_n(&g_fake_cancel_not_found, 1, __ATOMIC_SEQ_CST);
  int rc = pinedio_transceive(inst, w, r, sizeof(r));
  int queued = fake_queued();
  __atomic_store_n(&g_fake_cancel_not_found, 0, __ATOMIC_SEQ_CST);
  __atomic_store_n(&g_fake_fail_in, 0, __ATOMIC_SEQ_CST);
  printf("     S22: rc %d, still queued at return %d\n", rc, queued);
  fflush(stdout);
  assert(rc == -1 && queued == 0);
  close_inst(inst);
}

/* S23: attach and detach are counted from their first statement, rejected arguments included: after
 * rejected calls (a pin past the table, a NULL callback) no call is left counted and deinit returns. A rejected
 * call that left without its api_exit would leave deinit waiting forever (the child's alarm). */
static void s23(void) {
  struct pinedio_inst *inst = open_inst();
  assert(pinedio_attach_interrupt(inst, (enum pinedio_int_pin) PINEDIO_INT_PIN_MAX, PINEDIO_INT_MODE_RISING, cb_a) == -1);
  assert(pinedio_attach_interrupt(inst, PINEDIO_PIN_D0, PINEDIO_INT_MODE_RISING, NULL) == -1);
  assert(pinedio_deattach_interrupt(inst, (enum pinedio_int_pin) 255) == -1);
  assert(__atomic_load_n(&inst->api_active, __ATOMIC_SEQ_CST) == 0);
  close_inst(inst);
}

/* S24: a second deinit after a completed one returns 0 and touches neither the destroyed mutex nor the
 * condvar (counted locks on the instance's mutex: 0). A deinit that was refused stays retryable (S16). Before,
 * the second deinit locked the destroyed mutex. */
static void s24(void) {
  struct pinedio_inst *inst = open_inst();
  assert(pinedio_deinit(inst) == 0);
  g_watch_mutex = &inst->usb_access_mutex;
  int destroyed = g_mutex_destroyed;
  assert(pinedio_deinit(inst) == 0);
  printf("     S24: locks of the destroyed mutex by the second deinit: %d\n", g_watched_locks);
  fflush(stdout);
  assert(g_watched_locks == 0 && g_mutex_destroyed == destroyed);
  g_watch_mutex = NULL;
  free(inst);
}

/* S25, through the public path: an IN transfer that completes with no byte, or one byte short. The call
 * returns -1 (an error, its other transfers drained); it waited for the missing bytes forever before, with the
 * USB mutex held (the child's alarm). */
static void s25(void) {
  struct pinedio_inst *inst = open_inst();
  uint8_t w[3] = {0}, r[3] = {0};
  for (int mode = 1; mode <= 2; mode++) {
    __atomic_store_n(&g_fake_short, mode, __ATOMIC_SEQ_CST);
    assert(fails_at_once(inst, w, r, sizeof(r)));   /* the short completion is the error, not a stall */
  }
  __atomic_store_n(&g_fake_short, 0, __ATOMIC_SEQ_CST);
  assert(pinedio_transceive(inst, w, r, sizeof(r)) == 0);   /* whole completions work again */
  close_inst(inst);
}

/* S22b, the IN twin of S22: a 40-byte read takes two IN transfers. Its OUT transfer completes at once (inside
 * its submit), the first IN transfer is submitted, the second IN submit fails, and the cancel of the submitted IN
 * transfer answers NOT_FOUND. The call returns -1 only after that transfer's callback has run: none of its
 * transfers is still queued. A failed IN cancel that marked the transfer failed returned with it still queued. */
static void s22b(void) {
  struct pinedio_inst *inst = open_inst();
  uint8_t w[40] = {0}, r[40] = {0};
  assert(pinedio_set_option(inst, PINEDIO_OPTION_AUTO_CS, 0) == 0);
  __atomic_store_n(&g_fake_in_submits, 0, __ATOMIC_SEQ_CST);
  __atomic_store_n(&g_fake_fail_in_after, 1, __ATOMIC_SEQ_CST);
  __atomic_store_n(&g_fake_cancel_not_found, 1, __ATOMIC_SEQ_CST);
  __atomic_store_n(&g_fake_complete_out_in_submit, 1, __ATOMIC_SEQ_CST);
  int cancels = __atomic_load_n(&g_fake_cancels, __ATOMIC_SEQ_CST);
  int rc = pinedio_transceive(inst, w, r, sizeof(r));
  int queued = fake_queued();
  __atomic_store_n(&g_fake_complete_out_in_submit, 0, __ATOMIC_SEQ_CST);
  __atomic_store_n(&g_fake_cancel_not_found, 0, __ATOMIC_SEQ_CST);
  __atomic_store_n(&g_fake_fail_in_after, 0, __ATOMIC_SEQ_CST);
  printf("     S22b: rc %d, cancels %d, still queued at return %d\n", rc,
         __atomic_load_n(&g_fake_cancels, __ATOMIC_SEQ_CST) - cancels, queued);
  fflush(stdout);
  assert(rc == -1 && queued == 0 && __atomic_load_n(&g_fake_cancels, __ATOMIC_SEQ_CST) - cancels == 1);
  close_inst(inst);
}

/* S26a/b: attach and detach with a rejected argument are counted before the argument check: held at
 * api_entered (counted, before its lock) while deinit starts, deinit waits for it (its api_active wait) and
 * returns only after the call was released and left with -1. With the check before api_enter the call never
 * reached api_entered and returned uncounted (the hold is never reached: the child's alarm). */
static int g_bad_rc = 99;
static void *d_attach_bad(void *arg) {
  register_self(&g_d_thread, &g_d_thread_set);
  g_bad_rc = pinedio_attach_interrupt(arg, (enum pinedio_int_pin) PINEDIO_INT_PIN_MAX, PINEDIO_INT_MODE_RISING, cb_a);
  return NULL;
}
static void *d_detach_bad(void *arg) {
  register_self(&g_d_thread, &g_d_thread_set);
  g_bad_rc = pinedio_deattach_interrupt(arg, (enum pinedio_int_pin) 255);
  return NULL;
}
static void rejected_call_counted(void *(*fn)(void *)) {
  struct pinedio_inst *inst = open_inst();
  arm(&H_api_entered);
  pthread_t d;
  assert(pthread_create(&d, NULL, fn, inst) == 0);
  wait_arrived(&H_api_entered);                   /* counted, before its lock */
  pthread_t h = start_deinit(inst);
  assert(wait_either(EV_DEINIT_COND_WAIT, EV_DEINIT_PAST_WAIT) == EV_DEINIT_COND_WAIT);
  release(&H_api_entered);
  pthread_join(d, NULL);
  pthread_join(h, NULL);
  assert(g_bad_rc == -1);
  assert(g_ev_seq[EV_API_EXIT_D] < g_ev_seq[EV_DEINIT_PAST_WAIT]);
  memset(inst, 0xA5, sizeof(*inst));
  free(inst);
}
static void s26a(void) { rejected_call_counted(d_attach_bad); }
static void s26b(void) { rejected_call_counted(d_detach_bad); }

/* S27 (the gate-2 round 2 case), through the public path: a 40-byte read has two IN transfers queued; the
 * first completes CANCELLED, the second with an error. The call returns -1. With CANCELLED stored as
 * TRANS_IDLE the completion processing stopped at the first slot, never saw the second slot's error, and the
 * read waited forever with the USB mutex held (the child's alarm). */
static void s27(void) {
  struct pinedio_inst *inst = open_inst();
  uint8_t w[40] = {0}, r[40] = {0};
  assert(pinedio_set_option(inst, PINEDIO_OPTION_AUTO_CS, 0) == 0);
  g_fake_in_status[0] = LIBUSB_TRANSFER_CANCELLED;
  g_fake_in_status[1] = LIBUSB_TRANSFER_NO_DEVICE;
  __atomic_store_n(&g_fake_in_status_i, 0, __ATOMIC_SEQ_CST);
  g_fake_in_status_n = 2;
  assert(fails_at_once(inst, w, r, sizeof(r)));     /* the second slot's error is seen, not a stall */
  g_fake_in_status_n = 0;
  assert(__atomic_load_n(&inst->api_active, __ATOMIC_SEQ_CST) == 0);
  close_inst(inst);
}

/* S28, through the public path: a zero-length transceive or write_read is a no-op: it returns 0 before any
 * stack buffer is sized, sends nothing and leaves no call counted. (An error code here would reach the daemon's
 * HAL as a failed SPI transfer and latch a stick loss.) Before, write_read cleared 32 bytes of a zero-size buffer
 * and transceive declared one (found by the gate-2 round with ASan/UBSan; no sanitizer links on this host). */
static void s28(void) {
  struct pinedio_inst *inst = open_inst();
  int submits = __atomic_load_n(&g_fake_submits, __ATOMIC_SEQ_CST);
  uint8_t b[1] = {0};
  assert(pinedio_transceive(inst, b, b, 0) == 0);
  assert(pinedio_write_read(inst, NULL, 0, NULL, 0) == 0);
  assert(__atomic_load_n(&g_fake_submits, __ATOMIC_SEQ_CST) == submits);
  assert(__atomic_load_n(&inst->api_active, __ATOMIC_SEQ_CST) == 0);
  close_inst(inst);
}

/* S28b, through the public path: write_read with 0x80000000 + 0x80000000 bytes (the 32-bit sum is 0) returns -1,
 * not the empty operation's 0; an operation over PINEDIO_SPI_MAX bytes returns -1; none sends anything or leaves
 * a call counted. Before, the wrapped sum passed the empty-operation guard and returned 0. */
static void s28b(void) {
  struct pinedio_inst *inst = open_inst();
  int submits = __atomic_load_n(&g_fake_submits, __ATOMIC_SEQ_CST);
  uint8_t b[1] = {0};
  assert(pinedio_write_read(inst, b, 0x80000000u, b, 0x80000000u) == -1);
  assert(pinedio_write_read(inst, b, PINEDIO_SPI_MAX, b, 1) == -1);
  assert(pinedio_write_read(inst, b, 1, b, 0xFFFFFFFFu) == -1);
  assert(pinedio_transceive(inst, b, b, PINEDIO_SPI_MAX + 1) == -1);
  assert(__atomic_load_n(&g_fake_submits, __ATOMIC_SEQ_CST) == submits);
  assert(__atomic_load_n(&inst->api_active, __ATOMIC_SEQ_CST) == 0);
  close_inst(inst);
}

/* S29, through the public path: the CS writes around an SPI transfer are checked. With AUTO_CS a transceive is
 * three OUT transfers (CS assert, the SPI stream, CS release). (a) The assert completes short: the call returns -1,
 * sends no SPI stream and still releases CS (the short assertion can have reached the device). (b) The release completes short after a good transfer: -1. (c) init's initial CS write
 * completes short: init fails (-3). Before, (a) and (b) returned 0 and init went on. */
static int out_short_call(int at) {
  struct pinedio_inst *inst = open_inst();
  uint8_t w[3] = {0}, r[3] = {0};
  __atomic_store_n(&g_fake_out_n, 0, __ATOMIC_SEQ_CST);
  __atomic_store_n(&g_fake_out_short_at, at, __ATOMIC_SEQ_CST);
  int submits = __atomic_load_n(&g_fake_submits, __ATOMIC_SEQ_CST);
  int rc = pinedio_transceive(inst, w, r, sizeof(r));
  int sent = __atomic_load_n(&g_fake_submits, __ATOMIC_SEQ_CST) - submits;
  int released = !(g_fake_last_out[0][1] & 1);   /* the last OUT wrote D0 (CS) inactive */
  __atomic_store_n(&g_fake_out_short_at, 0, __ATOMIC_SEQ_CST);
  printf("     S29 short OUT #%d: rc %d, submits %d\n", at, rc, sent);
  fflush(stdout);
  close_inst(inst);
  return at == 1 ? (rc == -1 && sent == 2 && released) : rc == -1;
}
static void s29(void) {
  assert(out_short_call(1));                      /* (a) no SPI stream after a failed assert; CS released */
  assert(out_short_call(3));                      /* (b) a failed release fails the call */
  devices(1); g_fake_io_ok = 1;                   /* (c) the initial CS write in init */
  struct pinedio_inst *inst = calloc(1, sizeof(*inst));
  *inst = fresh(NULL);
  __atomic_store_n(&g_fake_out_n, 0, __ATOMIC_SEQ_CST);
  __atomic_store_n(&g_fake_out_short_at, 1, __ATOMIC_SEQ_CST);
  assert(pinedio_init(inst, NULL) == -3);
  __atomic_store_n(&g_fake_out_short_at, 0, __ATOMIC_SEQ_CST);
  free(inst);
}

/* S29d, through the public path: the CS assertion's transfer never calls back (abandoned), so the release after
 * it is refused. The release stays pending: the next transceive releases CS first, then asserts, streams and
 * releases. Before, that call asserted on a CS left asserted (the SPI stream would continue the half-finished
 * transaction): three OUT transfers, no release first. */
static void s29d(void) {
  struct pinedio_inst *inst = open_inst();
  uint8_t w[3] = {0}, r[3] = {0};
  __atomic_store_n(&g_fake_hold_out, 1, __ATOMIC_SEQ_CST);
  __atomic_store_n(&g_fake_cancel_not_found, 1, __ATOMIC_SEQ_CST);
  assert(pinedio_transceive(inst, w, r, sizeof(r)) == -1);
  assert(inst->req_abandoned);
  __atomic_store_n(&g_fake_hold_out, 0, __ATOMIC_SEQ_CST);
  __atomic_store_n(&g_fake_cancel_not_found, 0, __ATOMIC_SEQ_CST);
  libusb_handle_events_timeout(NULL, &(struct timeval){0, 0});   /* the late callback comes */
  __atomic_store_n(&g_fake_out_logn, 0, __ATOMIC_SEQ_CST);
  assert(pinedio_transceive(inst, w, r, sizeof(r)) == 0);
  int n = __atomic_load_n(&g_fake_out_logn, __ATOMIC_SEQ_CST);
  printf("     S29d: %d OUT transfers in the next call\n", n);
  fflush(stdout);
  assert(n == 4);
  assert(g_fake_out_log[0][0] == CH341_CMD_UIO_STREAM && !(g_fake_out_log[0][1] & 1));   /* release first */
  assert(g_fake_out_log[1][0] == CH341_CMD_UIO_STREAM && (g_fake_out_log[1][1] & 1));    /* assert */
  assert(g_fake_out_log[2][0] == CH341_CMD_SPI_STREAM);                                  /* the stream */
  assert(g_fake_out_log[3][0] == CH341_CMD_UIO_STREAM && !(g_fake_out_log[3][1] & 1));   /* release */
  close_inst(inst);
}

/* S30 (the gate-2 round 2 case), through the public path: a read whose IN submit fails; the cancel of its
 * submitted OUT transfer fails and that transfer never completes. The call returns -1 after the drain deadline
 * (the request is abandoned) instead of waiting forever with the USB mutex held. While the transfer is
 * outstanding every call is refused without USB traffic and deinit returns -1 releasing nothing; once its
 * callback can come (the fake releases it), deinit drains it and returns 0. Red before: the read never
 * returned (the child's alarm). */
static void s30(void) {
  struct pinedio_inst *inst = open_inst();
  uint8_t w[3] = {0}, r[3] = {0};
  assert(pinedio_set_option(inst, PINEDIO_OPTION_AUTO_CS, 0) == 0);
  __atomic_store_n(&g_fake_hold_out, 1, __ATOMIC_SEQ_CST);
  __atomic_store_n(&g_fake_fail_in, 1, __ATOMIC_SEQ_CST);
  __atomic_store_n(&g_fake_cancel_not_found, 1, __ATOMIC_SEQ_CST);
  assert(pinedio_transceive(inst, w, r, sizeof(r)) == -1);
  assert(inst->req_abandoned);
  __atomic_store_n(&g_fake_fail_in, 0, __ATOMIC_SEQ_CST);
  int submits = __atomic_load_n(&g_fake_submits, __ATOMIC_SEQ_CST);
  assert(pinedio_transceive(inst, w, r, sizeof(r)) == -1);           /* refused while outstanding */
  assert(__atomic_load_n(&g_fake_submits, __ATOMIC_SEQ_CST) == submits);
  int freed = g_fake_freed;
  assert(pinedio_deinit(inst) == -1);                                 /* nothing released, retryable */
  assert(g_fake_freed == freed && g_devs[0].closed == 0 && !inst->closed);
  __atomic_store_n(&g_fake_hold_out, 0, __ATOMIC_SEQ_CST);           /* its callback can come now */
  __atomic_store_n(&g_fake_cancel_not_found, 0, __ATOMIC_SEQ_CST);
  assert(pinedio_deinit(inst) == 0 && inst->closed && g_devs[0].closed == 1);
  free(inst);
}

/* S31 (the gate-2 round 3 case), through the public path: a read whose OUT transfer completes and whose IN
 * transfer never calls back (its timeout cancellation fails: no terminal callback). The call fails after the
 * progress deadline (PINEDIO_XFER_MS), cancels, drains for PINEDIO_DRAIN_MS and abandons the request instead of
 * waiting forever with the USB mutex held; deinit returns -1 while the transfer is outstanding and 0 once its
 * callback has come. Red before: the read never returned (the child's alarm). */
static void s31(void) {
  struct pinedio_inst *inst = open_inst();
  uint8_t w[3] = {0}, r[3] = {0};
  assert(pinedio_set_option(inst, PINEDIO_OPTION_AUTO_CS, 0) == 0);
  __atomic_store_n(&g_fake_hold_in, 1, __ATOMIC_SEQ_CST);
  __atomic_store_n(&g_fake_cancel_not_found, 1, __ATOMIC_SEQ_CST);
  struct timespec t0;
  clock_gettime(CLOCK_MONOTONIC, &t0);
  __atomic_store_n(&g_fake_ev_end_max, 0, __ATOMIC_SEQ_CST);
  __atomic_store_n(&g_fake_tv_max, 0, __ATOMIC_SEQ_CST);
  assert(pinedio_transceive(inst, w, r, sizeof(r)) == -1);
  long long returned = fake_now_ms();
  long long longest = __atomic_load_n(&g_fake_tv_max, __ATOMIC_SEQ_CST);
  long ms = ms_since(&t0);
  long long over = __atomic_load_n(&g_fake_ev_end_max, __ATOMIC_SEQ_CST) - returned;
  printf("     S31: the call returned after %ld ms; latest event-wait end %lld ms after its return; longest wait %lld ms\n",
         ms, over, longest);
  fflush(stdout);
  assert(ms >= PINEDIO_XFER_MS && ms < PINEDIO_XFER_MS + PINEDIO_DRAIN_MS + 1000);
  assert(over <= 5);   /* no event wait (the loop's, the drain's) went past the deadline it had */
  assert(longest <= PINEDIO_XFER_MS);   /* the loop's waits are capped by its progress deadline, not 1 s */
  assert(inst->req_abandoned && __atomic_load_n(&inst->api_active, __ATOMIC_SEQ_CST) == 0);
  int freed = g_fake_freed;
  assert(pinedio_deinit(inst) == -1);                                 /* outstanding: nothing released */
  assert(g_fake_freed == freed && g_devs[0].closed == 0 && !inst->closed);
  __atomic_store_n(&g_fake_hold_in, 0, __ATOMIC_SEQ_CST);            /* its callback can come now */
  __atomic_store_n(&g_fake_cancel_not_found, 0, __ATOMIC_SEQ_CST);
  assert(pinedio_deinit(inst) == 0 && inst->closed && g_devs[0].closed == 1);
  free(inst);
}

/* S31b, through the public path: a request that keeps progressing is not cut by the progress deadline. A
 * 3000-byte transceive needs four rounds of 32 IN transfers; each event round takes 100 ms (the fake's hook), so
 * the call lasts longer than PINEDIO_XFER_MS but never goes that long without a completion: it returns 0. With
 * the deadline not restarted by completions it failed after PINEDIO_XFER_MS. */
static void slow_events(void) {
  if (pthread_equal(pthread_self(), g_main_thread)) { struct timespec d = {0, 100000000L}; nanosleep(&d, NULL); }
}
static void s31b(void) {
  g_main_thread = pthread_self();
  struct pinedio_inst *inst = open_inst();
  static uint8_t w[3000], r[3000];
  assert(pinedio_set_option(inst, PINEDIO_OPTION_AUTO_CS, 0) == 0);
  __atomic_store_n(&g_fake_on_events, slow_events, __ATOMIC_SEQ_CST);
  struct timespec t0;
  clock_gettime(CLOCK_MONOTONIC, &t0);
  int rc = pinedio_transceive(inst, w, r, sizeof(r));
  long ms = ms_since(&t0);
  __atomic_store_n(&g_fake_on_events, NULL, __ATOMIC_SEQ_CST);
  printf("     S31b: rc %d after %ld ms\n", rc, ms);
  fflush(stdout);
  assert(rc == 0 && ms > PINEDIO_XFER_MS);
  close_inst(inst);
}

/* S32a, through the public path: a caller holds the USB mutex inside its event handling and does not come back
 * (a callback blocked in the caller's code: the fake's hold point). deinit returns -1 within its one deadline
 * (PINEDIO_DRAIN_MS) releasing nothing; once the caller has returned, deinit returns 0. Red before: deinit
 * blocked on the mutex (the child's alarm). */
static struct hold H_blocked;
static void blocked_on_events(void) { if (!pthread_equal(pthread_self(), g_main_thread)) hold_point(&H_blocked); }
static void *blocked_read(void *arg) { return (void *) (intptr_t) pinedio_digital_read(arg, 1); }
static void s32a(void) {
  g_main_thread = pthread_self();
  struct pinedio_inst *inst = open_inst();
  arm(&H_blocked);
  __atomic_store_n(&g_fake_on_events, blocked_on_events, __ATOMIC_SEQ_CST);
  pthread_t t;
  assert(pthread_create(&t, NULL, blocked_read, inst) == 0);
  wait_arrived(&H_blocked);                       /* the caller holds the mutex, in its event handling */
  struct timespec t0;
  clock_gettime(CLOCK_MONOTONIC, &t0);
  int freed = g_fake_freed;
  assert(pinedio_deinit(inst) == -1);
  long ms = ms_since(&t0);
  printf("     S32a: deinit returned -1 after %ld ms\n", ms);
  fflush(stdout);
  assert(ms < PINEDIO_DRAIN_MS + 1000 && g_fake_freed == freed && g_devs[0].closed == 0 && !inst->closed);
  __atomic_store_n(&g_fake_on_events, NULL, __ATOMIC_SEQ_CST);
  release(&H_blocked);
  pthread_join(t, NULL);
  assert(pinedio_deinit(inst) == 0 && inst->closed && g_devs[0].closed == 1);
  free(inst);
}

/* S32b, through the public path: the worker is inside an interrupt callback that does not return. deinit
 * returns -1 within its one deadline releasing nothing (the worker is not joined); once the callback has
 * returned, deinit joins the worker and returns 0. Red before: deinit blocked in its join (the child's alarm). */
static struct hold H_cb_blocked;
static void cb_blocked(void) { hold_point(&H_cb_blocked); }
static void s32b(void) {
  struct pinedio_inst *inst = open_inst();
  arm(&H_cb_blocked);
  attach_d0(inst, cb_blocked);
  set_input(0x01);
  wait_arrived(&H_cb_blocked);                    /* the worker is in the callback, without the mutex */
  struct timespec t0;
  clock_gettime(CLOCK_MONOTONIC, &t0);
  int freed = g_fake_freed;
  assert(pinedio_deinit(inst) == -1);
  long ms = ms_since(&t0);
  printf("     S32b: deinit returned -1 after %ld ms\n", ms);
  fflush(stdout);
  assert(ms < PINEDIO_DRAIN_MS + 1000 && g_fake_freed == freed && g_joined == 0 && !inst->closed);
  release(&H_cb_blocked);
  assert(pinedio_deinit(inst) == 0 && inst->closed && g_joined == 1);
  free(inst);
}

/* S33: a callback only stores its state: with stderr a full pipe (every write blocks), an error, a short and a
 * cancelled completion each return and publish TRANS_ERR. A diagnostic written in the callback blocked the thread
 * handling the events, and with it every deadline of the transfer loop and the drain. Red before: the error
 * callback blocked in fprintf. */
static int g_cb_done;
static void *cb_in_thread(void *t) {
  cb_in(t);
  pthread_mutex_lock(&g_m); g_cb_done = 1; pthread_cond_broadcast(&g_c); pthread_mutex_unlock(&g_m);
  return NULL;
}
static int callback_returns(enum libusb_transfer_status status, int actual) {
  struct pinedio_xfer_state ts = { .st = TRANS_ACTIVE };
  uint8_t buf[4] = {0};
  struct libusb_transfer t = { .status = status, .length = 4, .actual_length = actual, .buffer = buf, .user_data = &ts };
  g_cb_done = 0;
  pthread_t th;
  assert(pthread_create(&th, NULL, cb_in_thread, &t) == 0);
  struct timespec at;
  clock_gettime(CLOCK_REALTIME, &at);
  at.tv_sec += 1;
  pthread_mutex_lock(&g_m);
  while (!g_cb_done && pthread_cond_timedwait(&g_c, &g_m, &at) != ETIMEDOUT) { }
  int done = g_cb_done;
  pthread_mutex_unlock(&g_m);
  if (!done) return 0;                            /* blocked (the child ends; the thread stays blocked) */
  pthread_join(th, NULL);
  return ST_GET(ts) == TRANS_ERR;
}
static void s33(void) {
  int p[2];
  assert(pipe(p) == 0);
  char junk[4096];
  memset(junk, 'x', sizeof(junk));
  assert(fcntl(p[1], F_SETFL, O_NONBLOCK) == 0);
  while (write(p[1], junk, sizeof(junk)) > 0) { }
  while (write(p[1], junk, 1) == 1) { }           /* the pipe is full */
  assert(fcntl(p[1], F_SETFL, 0) == 0);           /* and blocking: the next write would wait for ever */
  fflush(stderr);
  int saved = dup(2);
  dup2(p[1], 2);
  int ok = callback_returns(LIBUSB_TRANSFER_NO_DEVICE, 0) && callback_returns(LIBUSB_TRANSFER_COMPLETED, 3) &&
           callback_returns(LIBUSB_TRANSFER_CANCELLED, 0);
  dup2(saved, 2);
  printf("     S33: callbacks with a blocking stderr: %s\n", ok ? "all returned" : "one blocked");
  fflush(stdout);
  assert(ok);
}

/* S34, through the public path, with stderr a full blocking pipe (every write would block): no diagnostic is
 * written while the USB mutex is held and none delays deinit. (a) A failing call (its IN submit fails) returns at
 * once and deinit returns 0 within its deadline. (b) A caller holds the mutex in its event handling: deinit
 * returns -1 within its deadline (its refusal line never delays the return). (c) The worker's failing polls do not
 * hold the mutex: a public call and deinit return. Before, the report was written under the mutex and deinit's
 * refusal line before its return: each case blocked (the child's alarm). */
static int stderr_full(int *saved) {
  int p[2];
  assert(pipe(p) == 0);
  char junk[4096];
  memset(junk, 'x', sizeof(junk));
  assert(fcntl(p[1], F_SETFL, O_NONBLOCK) == 0);
  while (write(p[1], junk, sizeof(junk)) > 0) { }
  while (write(p[1], junk, 1) == 1) { }
  assert(fcntl(p[1], F_SETFL, 0) == 0);
  fflush(stderr);
  *saved = dup(2);
  dup2(p[1], 2);
  return p[0];
}
static void stderr_back(int saved) { dup2(saved, 2); }
static void s34a(void) {
  struct pinedio_inst *inst = open_inst();
  uint8_t w[3] = {0}, r[3] = {0};
  assert(pinedio_set_option(inst, PINEDIO_OPTION_AUTO_CS, 0) == 0);
  int saved;
  stderr_full(&saved);
  __atomic_store_n(&g_fake_fail_in, 1, __ATOMIC_SEQ_CST);
  struct timespec t0;
  clock_gettime(CLOCK_MONOTONIC, &t0);
  int rc = pinedio_transceive(inst, w, r, sizeof(r));
  long call_ms = ms_since(&t0);
  clock_gettime(CLOCK_MONOTONIC, &t0);
  int drc = pinedio_deinit(inst);
  long d_ms = ms_since(&t0);
  stderr_back(saved);
  printf("     S34a: call rc %d after %ld ms, deinit rc %d after %ld ms\n", rc, call_ms, drc, d_ms);
  fflush(stdout);
  assert(rc == -1 && call_ms < PINEDIO_XFER_MS && drc == 0 && d_ms < PINEDIO_DRAIN_MS);
  free(inst);
}
static void s34b(void) {
  g_main_thread = pthread_self();
  struct pinedio_inst *inst = open_inst();
  arm(&H_blocked);
  __atomic_store_n(&g_fake_on_events, blocked_on_events, __ATOMIC_SEQ_CST);
  pthread_t t;
  assert(pthread_create(&t, NULL, blocked_read, inst) == 0);
  wait_arrived(&H_blocked);                       /* the caller holds the mutex, in its event handling */
  int saved;
  stderr_full(&saved);
  struct timespec t0;
  clock_gettime(CLOCK_MONOTONIC, &t0);
  int drc = pinedio_deinit(inst);
  long ms = ms_since(&t0);
  stderr_back(saved);
  printf("     S34b: deinit rc %d after %ld ms\n", drc, ms);
  fflush(stdout);
  assert(drc == -1 && ms < PINEDIO_DRAIN_MS + 500);
  __atomic_store_n(&g_fake_on_events, NULL, __ATOMIC_SEQ_CST);
  release(&H_blocked);
  pthread_join(t, NULL);
  assert(pinedio_deinit(inst) == 0);
  free(inst);
}
static void s34c(void) {
  struct pinedio_inst *inst = open_inst();
  attach_d0(inst, cb_a);
  int saved;
  stderr_full(&saved);
  __atomic_store_n(&g_fake_fail_status, 1, __ATOMIC_SEQ_CST);
  int p = polls();
  wait_polls(p + 3);                              /* failing polls, each with its diagnostic */
  assert(__atomic_load_n(&inst->in_error, __ATOMIC_SEQ_CST));
  assert(pinedio_set_pin_mode(inst, 1, 1) == 0);  /* the mutex is not held by a write */
  struct timespec t0;
  clock_gettime(CLOCK_MONOTONIC, &t0);
  int drc = pinedio_deinit(inst);
  long ms = ms_since(&t0);
  stderr_back(saved);
  printf("     S34c: deinit rc %d after %ld ms\n", drc, ms);
  fflush(stdout);
  assert(drc == 0 && ms < PINEDIO_DRAIN_MS);
  __atomic_store_n(&g_fake_fail_status, 0, __ATOMIC_SEQ_CST);
  free(inst);
}

/* S35, through the public path: deinit's last drain gets what is left of its one deadline, not a fresh one. A
 * request is abandoned (S30's sequence); a counted caller keeps deinit waiting for about 400 ms; then the drain of
 * the still outstanding transfer ends with the deadline: deinit returns -1 within PINEDIO_DRAIN_MS (+ slack), and
 * no event wait it made went past that deadline. */
static volatile long g_s35_ms;
static volatile int g_s35_rc;
static struct timespec g_s35_t0;
static void *s35_deinit(void *arg) {
  pthread_mutex_lock(&g_m); g_deinit_thread = pthread_self(); g_deinit_thread_set = 1; pthread_mutex_unlock(&g_m);
  clock_gettime(CLOCK_MONOTONIC, &g_s35_t0);
  g_s35_rc = pinedio_deinit(arg);
  g_s35_ms = ms_since(&g_s35_t0);
  return NULL;
}
static void s35(void) {
  struct pinedio_inst *inst = open_inst();
  uint8_t w[3] = {0}, r[3] = {0};
  assert(pinedio_set_option(inst, PINEDIO_OPTION_AUTO_CS, 0) == 0);
  __atomic_store_n(&g_fake_hold_out, 1, __ATOMIC_SEQ_CST);
  __atomic_store_n(&g_fake_fail_in, 1, __ATOMIC_SEQ_CST);
  __atomic_store_n(&g_fake_cancel_not_found, 1, __ATOMIC_SEQ_CST);
  assert(pinedio_transceive(inst, w, r, sizeof(r)) == -1 && inst->req_abandoned);
  __atomic_store_n(&g_fake_fail_in, 0, __ATOMIC_SEQ_CST);
  pthread_t d, h;
  arm(&H_api_entered);                            /* D is counted and held before its lock */
  assert(pthread_create(&d, NULL, d_thread, inst) == 0);
  wait_arrived(&H_api_entered);
  __atomic_store_n(&g_fake_ev_end_max, 0, __ATOMIC_SEQ_CST);
  assert(pthread_create(&h, NULL, s35_deinit, inst) == 0);
  wait_event(EV_DEINIT_COND_WAIT);                /* deinit waits for D */
  struct timespec spend = {0, 400000000L};
  nanosleep(&spend, NULL);                        /* the time D keeps deinit waiting (the case's subject) */
  release(&H_api_entered);
  pthread_join(d, NULL);
  pthread_join(h, NULL);
  long long t0_ms = (long long) g_s35_t0.tv_sec * 1000 + g_s35_t0.tv_nsec / 1000000;
  long long over = __atomic_load_n(&g_fake_ev_end_max, __ATOMIC_SEQ_CST) - (t0_ms + PINEDIO_DRAIN_MS);
  printf("     S35: deinit rc %d after %ld ms; latest event-wait end %lld ms past its deadline\n", g_s35_rc,
         g_s35_ms, over);
  fflush(stdout);
  assert(g_s35_rc == -1 && g_s35_ms < PINEDIO_DRAIN_MS + 200 && over <= 5);
  __atomic_store_n(&g_fake_hold_out, 0, __ATOMIC_SEQ_CST);
  __atomic_store_n(&g_fake_cancel_not_found, 0, __ATOMIC_SEQ_CST);
  assert(pinedio_deinit(inst) == 0);
  free(inst);
}

/* S36, through the public path, with stderr a full blocking pipe: the lines S34 does not reach. (a) S31's sequence
 * (an IN callback that never comes): the call returns -1 within its deadlines although it abandons the request
 * (the abandonment line), and deinit returns -1 within its deadline ("still outstanding"); once the callback can
 * come, deinit returns 0. (b) S32b's sequence (a poll callback that does not return): deinit -1 within its deadline
 * ("did not return"). (c) S10's sequence (deinit from a poll callback): the callback's deinit returns -1 at once
 * ("from a poll callback"). Each line written through stdio would block (mutants X13, X9, X8, X10). */
static void s36a(void) {
  struct pinedio_inst *inst = open_inst();
  uint8_t w[3] = {0}, r[3] = {0};
  assert(pinedio_set_option(inst, PINEDIO_OPTION_AUTO_CS, 0) == 0);
  __atomic_store_n(&g_fake_hold_in, 1, __ATOMIC_SEQ_CST);
  __atomic_store_n(&g_fake_cancel_not_found, 1, __ATOMIC_SEQ_CST);
  int saved;
  stderr_full(&saved);
  struct timespec t0;
  clock_gettime(CLOCK_MONOTONIC, &t0);
  int rc = pinedio_transceive(inst, w, r, sizeof(r));
  long call_ms = ms_since(&t0);
  clock_gettime(CLOCK_MONOTONIC, &t0);
  int drc = pinedio_deinit(inst);
  long d_ms = ms_since(&t0);
  stderr_back(saved);
  printf("     S36a: call rc %d after %ld ms (abandoned %d), deinit rc %d after %ld ms\n", rc, call_ms,
         inst->req_abandoned, drc, d_ms);
  fflush(stdout);
  assert(rc == -1 && inst->req_abandoned && call_ms < PINEDIO_XFER_MS + PINEDIO_DRAIN_MS + 500);
  assert(drc == -1 && d_ms < PINEDIO_DRAIN_MS + 500);
  __atomic_store_n(&g_fake_hold_in, 0, __ATOMIC_SEQ_CST);
  __atomic_store_n(&g_fake_cancel_not_found, 0, __ATOMIC_SEQ_CST);
  assert(pinedio_deinit(inst) == 0);
  free(inst);
}
static void s36b(void) {
  struct pinedio_inst *inst = open_inst();
  arm(&H_cb_blocked);
  attach_d0(inst, cb_blocked);
  set_input(0x01);
  wait_arrived(&H_cb_blocked);                    /* the worker is in the callback, without the mutex */
  int saved;
  stderr_full(&saved);
  struct timespec t0;
  clock_gettime(CLOCK_MONOTONIC, &t0);
  int drc = pinedio_deinit(inst);
  long ms = ms_since(&t0);
  stderr_back(saved);
  printf("     S36b: deinit rc %d after %ld ms\n", drc, ms);
  fflush(stdout);
  assert(drc == -1 && ms < PINEDIO_DRAIN_MS + 500);
  release(&H_cb_blocked);
  assert(pinedio_deinit(inst) == 0);
  free(inst);
}
static void s36c(void) {
  struct pinedio_inst *inst = open_inst();
  attach_d0(inst, cb_deinit);
  int saved;
  stderr_full(&saved);
  set_input(0x01);
  int p = polls();
  wait_polls(p + 2);                              /* the callback ran and its deinit returned */
  stderr_back(saved);
  assert(g_deinit_rc == -1 && g_wrapper_freed == 0);
  close_inst(inst);
}

/* S12: a NULL callback is refused and never touches the count, on a disabled and on an enabled pin. */
static void s12(void) {
  struct pinedio_inst *inst = open_inst();
  assert(pinedio_attach_interrupt(inst, PINEDIO_PIN_D1, PINEDIO_INT_MODE_RISING, NULL) == -1 && inst->int_running_cnt == 0);
  assert(pinedio_attach_interrupt(inst, PINEDIO_PIN_D0, PINEDIO_INT_MODE_RISING, cb_a) == 0 && inst->int_running_cnt == 1);
  assert(pinedio_attach_interrupt(inst, PINEDIO_PIN_D0, PINEDIO_INT_MODE_RISING, NULL) == -1);
  assert(inst->int_running_cnt == 1 && inst->interrupts[PINEDIO_PIN_D0].callback == cb_a);
  close_inst(inst);
}

/* S13a: a public call whose first statement runs after deinit set CLOSING is refused before any device access.
 * S13b: a call already inside its transfer when deinit starts finishes first; deinit frees only after its exit. */
static void s13a(void) {
  struct pinedio_inst *inst = open_inst();
  attach_d0(inst, cb_a);
  arm(&H_before_cb);
  set_input(0x01);
  wait_arrived(&H_before_cb);                     /* the worker is in a callback: deinit will wait for it */
  pthread_t h = start_deinit(inst);
  wait_event(EV_DEINIT_COND_WAIT);
  int submits = g_fake_submits;
  uint8_t b[4] = {0};
  assert(pinedio_get_irq_state(inst, PINEDIO_PIN_D0) == -1);
  assert(pinedio_transceive(inst, b, b, sizeof(b)) == -1);
  assert(pinedio_digital_write(inst, 1, true) == -1);
  assert(pinedio_attach_interrupt(inst, PINEDIO_PIN_D1, PINEDIO_INT_MODE_RISING, cb_b) == -1);
  assert(pinedio_deattach_interrupt(inst, PINEDIO_PIN_D0) == -1);
  assert(pinedio_digital_read(inst, 1) == -1);
  assert(pinedio_write_read(inst, b, 1, b, 1) == -1);
  assert(pinedio_set_cs(inst, true) == -1);
  assert(pinedio_set_pin_mode(inst, 1, 1) == -1);
  assert(g_fake_submits == submits);
  release(&H_before_cb);
  pthread_join(h, NULL);
  memset(inst, 0xA5, sizeof(*inst));
  free(inst);
}
static struct hold H_events;
static void on_events(void) { if (is_d_thread()) hold_point(&H_events); }
static void s13b(void) {
  struct pinedio_inst *inst = open_inst();
  pthread_t d;
  arm(&H_events);
  __atomic_store_n(&g_fake_on_events, on_events, __ATOMIC_SEQ_CST);
  assert(pthread_create(&d, NULL, d_thread, inst) == 0);
  wait_arrived(&H_events);                        /* D holds the mutex inside its transfer */
  pthread_t h = start_deinit(inst);
  wait_event(EV_DEINIT_ENTERED);
  release(&H_events);
  pthread_join(d, NULL);
  pthread_join(h, NULL);
  assert(g_d_rc == 0);
  assert(g_ev_seq[EV_API_EXIT_D] < g_ev_seq[EV_DEINIT_PAST_WAIT]);
  memset(inst, 0xA5, sizeof(*inst));
  free(inst);
}

/* S11e: a transfer that cannot be allocated (the OUT one, or the k-th IN one) fails init with -3, never the
 * stale 0 of the claim; what was allocated is freed, the device closed, the mutex and condvar destroyed. */
static void s11e(void) {
  for (int k = 0; k <= USB_IN_TRANSFERS; k++) {
    devices(1); g_fake_io_ok = 1;
    g_fake_allocs = 0; g_fake_alloc_fail_at = k; g_fake_freed = 0; g_mutex_destroyed = g_cond_destroyed = 0;
    struct pinedio_inst *inst = calloc(1, sizeof(*inst));
    *inst = fresh(NULL);
    assert(pinedio_init(inst, NULL) == -3);
    assert(g_fake_freed == k && g_devs[0].closed == 1 && g_mutex_destroyed == 1 && g_cond_destroyed == 1);
    assert(g_created == 0);
    free(inst);
  }
}

/* S14, through the public path: each device has its own pin mode and state. A pin written or made an output
 * on device A is not in device B's next UIO command; a seed reaches only devices opened after it. On 98b656bf
 * (and 8d609e90) the pin state was process-wide: A's pin 1 showed up in B's command. */
static struct pinedio_inst *open_at(const char *port) {
  struct pinedio_inst *inst = calloc(1, sizeof(*inst));
  *inst = fresh(NULL);
  assert(pinedio_init_at_port(inst, NULL, port) == 0);
  return inst;
}
static void s14(void) {
  devices(3); g_fake_io_ok = 1;
  struct pinedio_inst *a = open_at("1-1.1"), *b = open_at("1-1.2");
  assert(pinedio_set_pin_mode(a, 4, 1) == 0 && pinedio_digital_write(a, 1, true) == 0);
  assert(pinedio_digital_write(b, 2, true) == 0);
  uint8_t st_b = g_fake_last_out[1][1], dir_b = g_fake_last_out[1][2];   /* 0x80|state, 0x40|mode */
  printf("     S14: B state 0x%02x dir 0x%02x\n", st_b, dir_b);
  fflush(stdout);
  assert((st_b & (1u << 2)) && !(st_b & (1u << 1)));    /* B's own pin 2; not A's pin 1 */
  assert(!(dir_b & (1u << 4)));                          /* not A's output pin 4 */
  pinedio_seed_pin_state(1u << 5, 1u << 5);              /* a template bit: only later opens get it */
  assert(pinedio_digital_write(b, 2, true) == 0 && !(g_fake_last_out[1][1] & (1u << 5)));
  struct pinedio_inst *c = open_at("1-1.3");
  assert(pinedio_digital_write(c, 3, true) == 0);
  assert((g_fake_last_out[2][1] & (1u << 5)) && (g_fake_last_out[2][2] & (1u << 5)));
  assert(pinedio_deinit(a) == 0 && pinedio_deinit(b) == 0 && pinedio_deinit(c) == 0);
  free(a); free(b); free(c);
}

/* S15 (the documented contract): a callback the worker took before a non-final detach still runs once after
 * that detach returned. */
static void s15(void) {
  struct pinedio_inst *inst = open_inst();
  assert(pinedio_attach_interrupt(inst, PINEDIO_PIN_D1, PINEDIO_INT_MODE_RISING, cb_b) == 0);
  attach_d0(inst, cb_a);
  arm(&H_before_cb);
  set_input(0x01);                                /* D0 rises: cb_a copied */
  wait_arrived(&H_before_cb);
  assert(pinedio_deattach_interrupt(inst, PINEDIO_PIN_D0) == 0);
  release(&H_before_cb);
  wait_event(EV_CB_A);
  assert(g_cb_a == 1);
  close_inst(inst);
}

/* S16: a join that fails is not "terminated": deinit returns -1 and releases nothing; a later deinit frees. */
static void s16(void) {
  struct pinedio_inst *inst = open_inst();
  g_fail_join = 1;
  assert(pinedio_deinit(inst) == -1);
  assert(g_fake_freed == 0 && g_devs[0].closed == 0 && g_mutex_destroyed == 0);
  g_fail_join = 0;
  assert(pinedio_deinit(inst) == 0);
  assert(g_fake_freed == USB_IN_TRANSFERS + 1 && g_devs[0].closed == 1 && g_mutex_destroyed == 1);
  free(inst);
}

/* S17: the quiet log belongs to the call: A's worker polls a gone device quietly; B's failing transfer,
 * completed inside A's worker's handle_events (the shared default context), is still reported, by B's call. */
static struct hold H_tb_events;
static void tb_on_events(void) { if (is_tb()) hold_point(&H_tb_events); }
static void *tb_read(void *arg) { register_self(&g_tb_thread, &g_tb_set); return (void *) (intptr_t) pinedio_digital_read(arg, 1); }
static void s17(void) {
  devices(2); g_fake_io_ok = 1;
  struct pinedio_inst *a = open_at("1-1.1"), *b = open_at("1-1.2");
  g_inst = a;
  attach_d0(a, cb_a);
  __atomic_store_n(&g_fake_fail_status, 1, __ATOMIC_SEQ_CST);
  int p = polls();
  wait_polls(p + 3);                              /* A is in_error: its worker reads quietly */
  assert(__atomic_load_n(&a->in_error, __ATOMIC_SEQ_CST));
  fflush(stderr);
  FILE *log = tmpfile();
  int saved = dup(2);
  dup2(fileno(log), 2);
  arm(&H_tb_events);
  __atomic_store_n(&g_fake_on_events, tb_on_events, __ATOMIC_SEQ_CST);
  pthread_t tb;
  assert(pthread_create(&tb, NULL, tb_read, b) == 0);
  wait_arrived(&H_tb_events);                     /* B's transfers are queued; B waits before handling them */
  p = polls();
  wait_polls(p + 2);                              /* A's worker handles events: B's transfers complete there */
  release(&H_tb_events);
  void *rb;
  pthread_join(tb, &rb);
  __atomic_store_n(&g_fake_on_events, NULL, __ATOMIC_SEQ_CST);
  fflush(stderr);
  dup2(saved, 2);
  rewind(log);
  char line[256];
  int b_lines = 0;
  while (fgets(line, sizeof(line), log))
    if (!strncmp(line, "pinedio_digital_read: ", 22) && strstr(line, " transfer: error: ")) b_lines++;
  assert((intptr_t) rb < 0 && b_lines >= 1);
  __atomic_store_n(&g_fake_fail_status, 0, __ATOMIC_SEQ_CST);
  assert(pinedio_deinit(a) == 0 && pinedio_deinit(b) == 0);
  free(a); free(b);
}

/* S37, through the public path (S10's sequence: deinit from a poll callback, its refusal line): a diagnostic never
 * blocks and never raises SIGPIPE, whatever stderr is, and is written where it can be. (a) A pipe with room gets
 * the line. (b) A pipe with no reader, SIGPIPE at its default: the process lives (before: SIGPIPE ended it).
 * (c) A socket gets the line. (d) A full socket: nothing blocks. (e) A full pipe drops the line (S36c); after the
 * pipe is drained, the next line is preceded by the count of dropped lines. (f) A terminal gets the line.
 * (g) The reader goes between the private open and the write (the write fails EPIPE): no SIGPIPE ends the process,
 * and none is left pending. (h) A socket whose peer is gone: no SIGPIPE. (i) A regular file gets two lines, the
 * second after the first (written to stderr itself, never through a new open at offset 0).
 * A full pipe that a competing writer fills after any check cannot block either: the write is on a private
 * O_NONBLOCK open (S34/S36 run it with stderr full). */
static void deinit_from_cb_stderr(int fd) {
  struct pinedio_inst *inst = open_inst();
  attach_d0(inst, cb_deinit);
  fflush(stderr);
  int saved = dup(2);
  dup2(fd, 2);
  set_input(0x01);
  int p = polls();
  wait_polls(p + 2);                              /* the callback ran and its deinit returned */
  dup2(saved, 2);
  close(saved);
  assert(g_deinit_rc == -1 && g_wrapper_freed == 0);
  close_inst(inst);
}
static void s37a(void) {
  int p[2];
  assert(pipe(p) == 0);
  deinit_from_cb_stderr(p[1]);
  int n = -1;
  assert(ioctl(p[0], FIONREAD, &n) == 0);
  char buf[512] = {0};
  assert(n > 0 && read(p[0], buf, sizeof(buf) - 1) == n);
  printf("     S37a: %d bytes in the pipe\n", n);
  fflush(stdout);
  assert(strstr(buf, "pinedio_deinit: called from a poll callback, refused\n") && !strstr(buf, "dropped"));
}
static void s37b(void) {
  signal(SIGPIPE, SIG_DFL);
  int p[2];
  assert(pipe(p) == 0);
  close(p[0]);
  deinit_from_cb_stderr(p[1]);
}
static void s37c(void) {
  int s[2];
  assert(socketpair(AF_UNIX, SOCK_STREAM, 0, s) == 0);
  deinit_from_cb_stderr(s[1]);
  char buf[512] = {0};
  ssize_t n = recv(s[0], buf, sizeof(buf) - 1, MSG_DONTWAIT);
  assert(n > 0 && strstr(buf, "pinedio_deinit: called from a poll callback, refused"));
}
static void s37d(void) {
  int s[2];
  assert(socketpair(AF_UNIX, SOCK_STREAM, 0, s) == 0);
  char junk[4096];
  memset(junk, 'x', sizeof(junk));
  while (send(s[1], junk, sizeof(junk), MSG_DONTWAIT) > 0) { }
  while (send(s[1], junk, 1, MSG_DONTWAIT) == 1) { }
  deinit_from_cb_stderr(s[1]);                    /* returns: the line is dropped */
}
static void s37e(void) {
  int p[2];
  assert(pipe(p) == 0);
  char junk[4096];
  memset(junk, 'x', sizeof(junk));
  assert(fcntl(p[1], F_SETFL, O_NONBLOCK) == 0);
  while (write(p[1], junk, sizeof(junk)) > 0) { }
  while (write(p[1], junk, 1) == 1) { }
  assert(fcntl(p[1], F_SETFL, 0) == 0);
  deinit_from_cb_stderr(p[1]);                    /* full: the line is dropped and counted */
  assert(fcntl(p[0], F_SETFL, O_NONBLOCK) == 0);
  while (read(p[0], junk, sizeof(junk)) > 0) { } /* drained */
  g_deinit_rc = 99;
  deinit_from_cb_stderr(p[1]);
  char buf[512] = {0};
  ssize_t n = read(p[0], buf, sizeof(buf) - 1);
  printf("     S37e: %s", n > 0 ? buf : "(nothing)\n");
  fflush(stdout);
  assert(n > 0 && strstr(buf, "libpinedio-usb: 1 diagnostic line(s) dropped (stderr full)\n") == buf &&
         strstr(buf, "pinedio_deinit: called from a poll callback, refused\n"));
}
static void s37g(void) {
  signal(SIGPIPE, SIG_DFL);
  int p[2];
  assert(pipe(p) == 0);
  g_close_at_diag = p[0];
  deinit_from_cb_stderr(p[1]);
  sigset_t pending;
  assert(g_close_at_diag == -1 && sigpending(&pending) == 0 && !sigismember(&pending, SIGPIPE));
}
static void s37h(void) {
  signal(SIGPIPE, SIG_DFL);
  int s[2];
  assert(socketpair(AF_UNIX, SOCK_STREAM, 0, s) == 0);
  close(s[0]);
  deinit_from_cb_stderr(s[1]);
}
static void s37i(void) {
  FILE *log = tmpfile();
  deinit_from_cb_stderr(fileno(log));
  g_deinit_rc = 99;
  deinit_from_cb_stderr(fileno(log));
  rewind(log);
  char line[256];
  int n = 0;
  while (fgets(line, sizeof(line), log))
    if (!strcmp(line, "pinedio_deinit: called from a poll callback, refused\n")) n++;
  printf("     S37i: %d lines in the file\n", n);
  fflush(stdout);
  assert(n == 2);
}
static void s37f(void) {
  int m = posix_openpt(O_RDWR | O_NOCTTY);
  assert(m >= 0 && grantpt(m) == 0 && unlockpt(m) == 0);
  int t = open(ptsname(m), O_RDWR | O_NOCTTY);
  assert(t >= 0);
  deinit_from_cb_stderr(t);
  char buf[512] = {0};
  struct pollfd q = { .fd = m, .events = POLLIN };
  assert(poll(&q, 1, 1000) == 1);
  ssize_t n = read(m, buf, sizeof(buf) - 1);
  assert(n > 0 && strstr(buf, "pinedio_deinit: called from a poll callback, refused"));
}

/* S38: deinit's join ends by its one deadline too. The worker leaves its loop (worker_done) and returns, but a
 * thread-local destructor a poll callback set does not return: deinit returns -1 within its deadline, nothing
 * released; once the destructor returns, deinit returns 0. Before, the join after worker_done had no deadline. */
static pthread_key_t g_tls_key;
static struct hold H_tls;
static void tls_dtor(void *v) { (void) v; hold_point(&H_tls); }
static void cb_tls(void) { pthread_setspecific(g_tls_key, (void *) 1); event(EV_CB_A); }
static void s38(void) {
  assert(pthread_key_create(&g_tls_key, tls_dtor) == 0);
  arm(&H_tls);
  struct pinedio_inst *inst = open_inst();
  attach_d0(inst, cb_tls);
  set_input(0x01);
  wait_event(EV_CB_A);                            /* the worker thread now has a destructor to run at its exit */
  struct timespec t0;
  clock_gettime(CLOCK_MONOTONIC, &t0);
  int drc = pinedio_deinit(inst);
  long ms = ms_since(&t0);
  printf("     S38: deinit rc %d after %ld ms\n", drc, ms);
  fflush(stdout);
  assert(drc == -1 && ms < PINEDIO_DRAIN_MS + 500 && inst->worker_started && g_fake_freed == 0);
  wait_arrived(&H_tls);                           /* the worker left its loop: only its destructor holds it */
  release(&H_tls);
  assert(pinedio_deinit(inst) == 0);
  free(inst);
}

/* ---- runner -------------------------------------------------------------------------------------------------- */
static int run(const char *name, void (*fn)(void)) {
  fflush(stdout);
  pid_t pid = fork();
  if (pid == 0) {
    alarm(10);                                    /* a hang is a failure, not a wait */
    fn();
    _exit(0);
  }
  int st = 0;
  waitpid(pid, &st, 0);
  int ok = WIFEXITED(st) && WEXITSTATUS(st) == 0;
  printf("%s %s", ok ? "ok  " : "FAIL", name);
  if (!ok) {
    if (WIFSIGNALED(st)) printf(" (signal %d%s)", WTERMSIG(st), WTERMSIG(st) == SIGALRM ? ", hang" : "");
    else printf(" (exit %d)", WEXITSTATUS(st));
  }
  printf("\n");
  return ok;
}

int main(int argc, char **argv) {
  int passed = 0, total = 0;   /* argv[1]: run only the cases whose name starts with it */
  struct { const char *n; void (*f)(void); } cases[] = {
    {"S0 public path: one poll thread, none after deinit", s0}, {"S1 re-attaches start no thread", s1}, {"S2 attach/detach during a poll", s2},
    {"S3 deinit during a held read", s3}, {"S4a deinit, worker idle", s4a}, {"S4b deinit, worker in its wait", s4b},
    {"S5 deinit waits for a counted caller", s5}, {"S6 callback attach during CLOSING refused", s6},
    {"S7 callback self-detach and re-attach", s7}, {"S8 read failure: sticky in_error, one line", s8},
    {"S9 spurious wakeups keep the deadline", s9}, {"S10 deinit from a callback refused", s10},
    {"S3b public path: one poller while one is held in its read", s3b},
    {"S8b transfer errors: bounded log", s8b},
    {"S11a init: libusb_init fails", s11a}, {"S11b init: no device", s11b},
    {"S11c init: condvar init fails", s11c}, {"S11d init: worker cannot start", s11d},
    {"S12 NULL callback refused, count untouched", s12},
    {"S13a a call after CLOSING is refused, no device access", s13a},
    {"S13b a call inside its transfer finishes before the free", s13b},
    {"S11e init: a transfer allocation fails (each position)", s11e},
    {"S11f init: uninitialised caller memory", s11f},
    {"S18 transceive returns the bytes reversed, in order", s18},
    {"S19 an IN transfer completed inside its submit", s19},
    {"S20 an OUT submit failure reads no unset IN state", s20},
    {"S21 a failed IN submit is not cancelled", s21},
    {"S22 a failed cancel waits for the callback", s22},
    {"S22b a failed IN cancel waits for the callback", s22b},
    {"S23 rejected attach/detach arguments leave no call counted", s23},
    {"S24 a second deinit touches no destroyed sync object", s24},
    {"S25 a short or zero-byte completion is an error", s25},
    {"S26a a rejected attach is counted before its check", s26a},
    {"S26b a rejected detach is counted before its check", s26b},
    {"S27 a cancelled IN completion is an error, not nothing", s27},
    {"S28 a zero-length transfer is a no-op before any buffer", s28},
    {"S28b a wrapped or oversized SPI length is refused, not a no-op", s28b},
    {"S29 the CS writes around a transfer and in init are checked", s29},
    {"S29d a refused CS release goes out before the next assertion", s29d},
    {"S30 a transfer whose callback never comes: bounded, abandoned, retryable", s30},
    {"S31 an IN callback that never comes: the call ends by its progress deadline", s31},
    {"S31b a request that keeps progressing outlives the progress deadline", s31b},
    {"S32a a caller blocked with the mutex: deinit -1 within its deadline", s32a},
    {"S32b a callback that does not return: deinit -1 within its deadline", s32b},
    {"S33 a callback only stores: it never blocks on stderr", s33},
    {"S34a stderr blocked: a failing call and deinit return", s34a},
    {"S34b stderr blocked: deinit -1 within its deadline", s34b},
    {"S34c stderr blocked: the worker's failing polls hold no mutex", s34c},
    {"S35 deinit's last drain gets what is left of its deadline", s35},
    {"S36a stderr blocked: an abandoning call and deinit's refusal return", s36a},
    {"S36b stderr blocked: deinit -1 while a poll callback does not return", s36b},
    {"S36c stderr blocked: deinit from a poll callback returns -1", s36c},
    {"S37a stderr a pipe with room: the line arrives", s37a},
    {"S37b stderr a pipe without a reader: no SIGPIPE", s37b},
    {"S37c stderr a socket: the line arrives", s37c},
    {"S37d stderr a full socket: the line is dropped, nothing blocks", s37d},
    {"S37e stderr a full pipe: dropped, counted, the count precedes the next line", s37e},
    {"S37f stderr a terminal: the line arrives", s37f},
    {"S37g stderr's reader goes before the write: no SIGPIPE", s37g},
    {"S37h stderr a socket without a peer: no SIGPIPE", s37h},
    {"S37i stderr a regular file: every line appended", s37i},
    {"S38 deinit's join ends by its deadline (a worker's exit that does not return)", s38},
    {"S14 each device has its own pin state", s14},
    {"S15 a callback taken before a non-final detach runs once", s15},
    {"S16 a failed join releases nothing", s16},
    {"S17 the quiet log belongs to the transfer", s17},
  };
  for (unsigned i = 0; i < sizeof(cases) / sizeof(cases[0]); i++) {
    if (argc > 1 && strncmp(cases[i].n, argv[1], strlen(argv[1])) != 0) continue;
    total++;
    passed += run(cases[i].n, cases[i].f);
  }
  printf("test_poll_worker: %d passed\n", passed);
  return passed == total ? 0 : 1;
}
