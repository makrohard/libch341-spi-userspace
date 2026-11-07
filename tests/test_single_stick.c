/* Golden first (G1-C1 B5): one stick and no selector, the plain pinedio_init every user makes, opens, detaches and
 * claims that stick once, with a serial and without one: as on pine64's b0694ec8. The same file passes unchanged on
 * b0694ec8 and on this fork. Run: tests/run.sh */
#include "fake_libusb.h"
#include "../libpinedio-usb.c"
#include "fake_helpers.h"

static void one_stick(const char *serial) {
  devices(1);
  g_devs[0].serial = serial;
  struct pinedio_inst inst = fresh(NULL);
  (void) pinedio_init(&inst, NULL);     /* the transfers fail in the fake after the selection; not judged here */
  assert(g_devs[0].opened == 1 && g_devs[0].detached == 1 && g_devs[0].claimed == 1);
}

int main(void) {
  one_stick("26073012");
  one_stick(NULL);
  printf("test_single_stick: 2 passed\n");
  return 0;
}
