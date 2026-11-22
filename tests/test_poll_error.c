/* Through the public API: the device stops answering for a while (every transfer fails), then answers again.
 * Measured: the error lines written while it fails, and whether a later edge still reaches the callback. */
#include "fake_libusb.h"
#include "../libpinedio-usb.c"
#include "fake_helpers.h"
#include "poll_common.h"

static struct pinedio_inst inst;
static volatile int calls;
static void cb(void) { calls++; }

int main(void) {
  g_complete = 1;
  devices(1);
  inst = fresh(NULL);
  assert(pinedio_init(&inst, NULL) == 0);
  pin_low();
  assert(pinedio_attach_interrupt(&inst, PINEDIO_PIN_D1, PINEDIO_INT_MODE_RISING, cb) == 0);
  fflush(stderr);
  FILE *log = freopen("poll_error.log", "w", stderr);   /* in the test's own directory */
  assert(log);
  g_fail_all = 1;
  ms(1000);                                             /* one second without the device */
  fflush(stderr);
  FILE *f = fopen("poll_error.log", "r");
  int lines = 0, poll_lines = 0;
  char buf[256];
  while (f && fgets(buf, sizeof(buf), f)) { lines++; if (strstr(buf, "Failed to get input")) poll_lines++; }
  if (f) fclose(f);
  g_fail_all = 0;
  pin_low(); pin_high();                                /* the device answers again: one rising edge */
  printf("one second failing: %d stderr lines (%d 'Failed to get input'); in_error %d; "
         "edge after recovery reached the callback: %s\n", lines, poll_lines, inst.in_error, calls ? "yes" : "no");
  printf("test_poll_error: measured\n");
  fflush(stdout);
  _exit(0);
}
