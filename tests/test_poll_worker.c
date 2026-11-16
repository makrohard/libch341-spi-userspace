/* The poll worker: one joinable thread per instance, started by init and joined by deinit; attach and detach
 * only enable or disable a pin; deinit sets CLOSING, joins the worker and waits for every counted caller before
 * it frees or destroys anything. Each case runs in a forked child: a hang (the old lifecycle) is turned into a
 * failure by alarm(); nothing synchronises by sleeping. Threads are ordered by hold points (PINEDIO_TEST_HOOK)
 * and by records taken where a join or the library's mutex orders them. Run: tests/run.sh */
#include "fake_libusb.h"
#include <errno.h>
#include <pthread.h>
#include <signal.h>
#include <stdlib.h>
#include <sys/wait.h>
#include <dirent.h>
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

static void test_hook(const char *name) {
  if (!strcmp(name, "in_read")) {
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
static int w_join(pthread_t t, void **r) {
  event(EV_JOIN_ENTER);
  if (g_fail_join) return EINVAL;
  int rc = pthread_join(t, r);
  __atomic_fetch_add(&g_joined, 1, __ATOMIC_SEQ_CST);
  event(EV_JOIN_RETURN);
  return rc;
}
static int w_cond_init(pthread_cond_t *c, const pthread_condattr_t *a) {
  if (g_fail_cond_init) return ENOMEM;
  return pthread_cond_init(c, a);
}
static int w_mutex_destroy(pthread_mutex_t *m) { g_mutex_destroyed++; return pthread_mutex_destroy(m); }
static int w_condattr_setclock(pthread_condattr_t *a, clockid_t c) { g_cond_clock = (int) c; return pthread_condattr_setclock(a, c); }
static int w_cond_destroy(pthread_cond_t *c) { g_cond_destroyed++; return pthread_cond_destroy(c); }
static int w_cond_wait(pthread_cond_t *c, pthread_mutex_t *m) {
  if (is_deinit_thread()) event(EV_DEINIT_COND_WAIT);
  return pthread_cond_wait(c, m);
}
static int w_cond_timedwait(pthread_cond_t *c, pthread_mutex_t *m, const struct timespec *at) {
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
#define pthread_create w_create
#define pthread_join w_join
#define pthread_cond_wait w_cond_wait
#define pthread_cond_timedwait w_cond_timedwait
#define pthread_cond_init w_cond_init
#define pthread_mutex_destroy w_mutex_destroy
#define pthread_cond_destroy w_cond_destroy
#define pthread_condattr_setclock w_condattr_setclock
#include "../libpinedio-usb.c"
#undef pthread_create
#undef pthread_join
#undef pthread_cond_wait
#undef pthread_cond_timedwait
#undef pthread_cond_init
#undef pthread_mutex_destroy
#undef pthread_cond_destroy
#undef pthread_condattr_setclock
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
  wait_event(EV_JOIN_ENTER);                      /* closing is set, deinit waits for the worker */
  pthread_t d;
  arm(&H_api_entered);                            /* held only for D (it registers itself first) */
  assert(pthread_create(&d, NULL, d_thread, inst) == 0);
  wait_arrived(&H_api_entered);                   /* D is counted, not yet locked */
  release(&H_before_cb);                          /* the worker calls cb_a, sees closing, exits */
  int first = wait_either(EV_DEINIT_COND_WAIT, EV_DEINIT_PAST_WAIT);
  assert(first == EV_DEINIT_COND_WAIT);           /* deinit waits for D */
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
  wait_event(EV_JOIN_ENTER);
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
  int lines = 0;
  while (fgets(line, sizeof(line), log)) lines++;
  printf("     S8b: %d stderr lines over 8 failing polls\n", lines);
  fflush(stdout);
  assert(lines <= 4);
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
  wait_arrived(&H_before_cb);                     /* the worker is in a callback: deinit will wait in its join */
  pthread_t h = start_deinit(inst);
  wait_event(EV_JOIN_ENTER);
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

/* S17: the quiet log belongs to the transfer: A's worker polls a gone device quietly; B's failing transfer,
 * completed inside A's worker's handle_events (the shared default context), still logs its error. */
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
  int cb_lines = 0;
  while (fgets(line, sizeof(line), log)) if (!strncmp(line, "cb_", 3)) cb_lines++;
  assert((intptr_t) rb < 0 && cb_lines >= 1);
  __atomic_store_n(&g_fake_fail_status, 0, __ATOMIC_SEQ_CST);
  assert(pinedio_deinit(a) == 0 && pinedio_deinit(b) == 0);
  free(a); free(b);
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
