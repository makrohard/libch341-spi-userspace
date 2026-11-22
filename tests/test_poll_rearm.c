/* Through the public API: an interrupt callback that detaches and re-attaches its own pin (re-arming, as a
 * caller may do on every interrupt). Afterwards exactly one poll thread may run, and none after deinit. */
#include "fake_libusb.h"
#include "../libpinedio-usb.c"
#include "fake_helpers.h"
#include "poll_common.h"

static struct pinedio_inst inst;
static volatile int calls;
static void cb(void) {
  calls++;
  pinedio_deattach_interrupt(&inst, PINEDIO_PIN_D1);
  pinedio_attach_interrupt(&inst, PINEDIO_PIN_D1, PINEDIO_INT_MODE_RISING, cb);
}

int main(void) {
  g_complete = 1;
  devices(1);
  inst = fresh(NULL);
  assert(pinedio_init(&inst, NULL) == 0);
  pin_low();
  assert(pinedio_attach_interrupt(&inst, PINEDIO_PIN_D1, PINEDIO_INT_MODE_RISING, cb) == 0);
  for (int i = 0; i < 3; i++) { pin_low(); pin_high(); }
  int during = active_threads(100);
  pinedio_deinit(&inst);
  ms(200);
  int after = active_threads(100);
  printf("callbacks %d (3 rising edges); poll threads running: %d before deinit, %d after deinit\n", calls, during,
         after);
  int ok = during == 1 && after == 0;
  printf("test_poll_rearm: %s\n", ok ? "1 passed" : "FAILED");
  fflush(stdout);
  _exit(ok ? 0 : 1);   /* a stray poll thread may still run: do not wait for it */
}
