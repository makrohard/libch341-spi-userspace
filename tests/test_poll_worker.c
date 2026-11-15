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
static void arm_for(struct hold *h, pthread_t t) { arm(h); pthread_mutex_lock(&g_m); h->only = t; h->only_set = 1; pthread_mutex_unlock(&g_m); }
static void wait_arrived(struct hold *h) { pthread_mutex_lock(&g_m); while (!h->arrived) pthread_cond_wait(&g_c, &g_m); pthread_mutex_unlock(&g_m); }
static void release(struct hold *h) { pthread_mutex_lock(&g_m); h->released = 1; pthread_cond_broadcast(&g_c); pthread_mutex_unlock(&g_m); }

/* named events: the sequence number of the first occurrence, and counts */
enum { EV_WORKER_EXIT, EV_DEINIT_ENTERED, EV_JOIN_ENTER, EV_JOIN_RETURN, EV_DEINIT_COND_WAIT, EV_DEINIT_PAST_WAIT,
       EV_API_EXIT_D, EV_CB_A, EV_CB_B, EV_MAX };
static int g_ev_seq[EV_MAX], g_ev_n[EV_MAX], g_polls;
static pthread_t g_deinit_thread, g_d_thread;
static int g_deinit_thread_set, g_d_thread_set;

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
static int is_deinit_thread(void) { return g_deinit_thread_set && pthread_equal(g_deinit_thread, pthread_self()); }
static int is_d_thread(void) { return g_d_thread_set && pthread_equal(g_d_thread, pthread_self()); }

static void test_hook(const char *name) {
  if (!strcmp(name, "in_read")) {
    pthread_mutex_lock(&g_m); g_polls++; pthread_cond_broadcast(&g_c); pthread_mutex_unlock(&g_m);
    hold_point(&H_in_read);
  } else if (!strcmp(name, "before_callbacks")) hold_point(&H_before_cb);
  else if (!strcmp(name, "api_entered")) hold_point(&H_api_entered);
  else if (!strcmp(name, "api_exit")) { if (is_d_thread()) event(EV_API_EXIT_D); }
  else if (!strcmp(name, "worker_exit")) event(EV_WORKER_EXIT);
  else if (!strcmp(name, "deinit_entered")) event(EV_DEINIT_ENTERED);
  else if (!strcmp(name, "deinit_past_wait")) event(EV_DEINIT_PAST_WAIT);
}

/* ---- wrappers for the library's pthread calls ---------------------------------------------------------------- */
static int g_created, g_joined, g_fail_create, g_spurious;
static struct timespec g_deadlines[16];
static int g_deadline_polls[16], g_ndeadlines;

static int w_create(pthread_t *t, const pthread_attr_t *a, void *(*f)(void *), void *arg) {
  if (g_fail_create) return EAGAIN;
  __atomic_fetch_add(&g_created, 1, __ATOMIC_SEQ_CST);
  return pthread_create(t, a, f, arg);
}
static int w_join(pthread_t t, void **r) {
  event(EV_JOIN_ENTER);
  int rc = pthread_join(t, r);
  __atomic_fetch_add(&g_joined, 1, __ATOMIC_SEQ_CST);
  event(EV_JOIN_RETURN);
  return rc;
}
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
#include "../libpinedio-usb.c"
#undef pthread_create
#undef pthread_join
#undef pthread_cond_wait
#undef pthread_cond_timedwait
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
  int after = live_threads();
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

/* S2: attach and detach while the worker is inside a poll: both wait for the mutex, then take effect. */
static void *attach_b(void *arg) { return (void *) (intptr_t) pinedio_attach_interrupt(arg, PINEDIO_PIN_D1, PINEDIO_INT_MODE_RISING, cb_b); }
static void *detach_a(void *arg) { return (void *) (intptr_t) pinedio_deattach_interrupt(arg, PINEDIO_PIN_D0); }
static void s2(void) {
  struct pinedio_inst *inst = open_inst();
  attach_d0(inst, cb_a);
  arm(&H_in_read);
  wait_arrived(&H_in_read);                       /* the worker holds the mutex in its read */
  pthread_t tb, ta;
  assert(pthread_create(&tb, NULL, attach_b, inst) == 0);
  assert(pthread_create(&ta, NULL, detach_a, inst) == 0);
  release(&H_in_read);
  void *rb, *ra;
  pthread_join(tb, &rb);
  pthread_join(ta, &ra);
  assert((intptr_t) rb == 0 && (intptr_t) ra == 0);
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

/* S4: deinit with the worker waiting: (a) idle, no pin; (b) in its interval wait, one pin. */
static void s4a(void) { struct pinedio_inst *inst = open_inst(); close_inst(inst); assert(g_joined == 1 && ev_n(EV_WORKER_EXIT) == 1); }
static void s4b(void) {
  struct pinedio_inst *inst = open_inst();
  attach_d0(inst, cb_a);
  close_inst(inst);
  assert(g_joined == 1 && ev_n(EV_WORKER_EXIT) == 1);
}

/* S5: deinit with an API caller between its count and its lock: deinit must wait for it before it frees or
 * destroys anything. */
static int g_d_rc = 99;
static void *d_thread(void *arg) {
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
  pthread_mutex_lock(&g_m);
  assert(pthread_create(&d, NULL, d_thread, inst) == 0);
  g_d_thread = d; g_d_thread_set = 1;
  pthread_mutex_unlock(&g_m);
  arm_for(&H_api_entered, d);
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
  int failed_lines = 0;
  while (fgets(line, sizeof(line), log))
    if (strstr(line, "Failed to get input")) failed_lines++;
  assert(failed_lines == 1);
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
  close_inst(inst);
}

/* S10: deinit from a callback is refused and frees nothing; deinit from the main thread then frees once. */
static void cb_deinit(void) { pinedio_deinit(g_inst); }
static void s10(void) {
  struct pinedio_inst *inst = open_inst();
  attach_d0(inst, cb_deinit);
  set_input(0x01);
  int p = polls();
  wait_polls(p + 2);
  assert(g_fake_freed == 0 && g_devs[0].closed == 0);
  close_inst(inst);
  assert(g_fake_freed == USB_IN_TRANSFERS + 1 && g_devs[0].closed == 1 && g_joined == 1);
}

/* S11: the worker cannot start: init fails, nothing to join, the device is given back. */
static void s11(void) {
  devices(1);
  g_fake_io_ok = 1;
  g_fail_create = 1;
  struct pinedio_inst *inst = calloc(1, sizeof(*inst));
  *inst = fresh(NULL);
  assert(pinedio_init(inst, NULL) != 0);
  assert(g_created == 0 && g_joined == 0 && g_devs[0].closed == 1 && g_fake_freed == USB_IN_TRANSFERS + 1);
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
    {"S11 worker cannot start: init fails", s11},
  };
  for (unsigned i = 0; i < sizeof(cases) / sizeof(cases[0]); i++) {
    if (argc > 1 && strncmp(cases[i].n, argv[1], strlen(argv[1])) != 0) continue;
    total++;
    passed += run(cases[i].n, cases[i].f);
  }
  printf("test_poll_worker: %d passed\n", passed);
  return passed == total ? 0 : 1;
}
