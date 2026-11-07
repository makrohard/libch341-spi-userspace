/* Through the public pinedio_init (the call every user makes): a serial search detaches and claims no other stick.
 * Before the fix the library claimed each visited stick (and detached its kernel driver) before comparing the
 * serial, and closed a non-match without releasing it. Run: tests/run.sh */
#include "fake_libusb.h"
#include "../libpinedio-usb.c"
#include "fake_helpers.h"

int main(void) {
  devices(3);
  g_devs[0].serial = NULL;              /* the PINE64 */
  g_devs[1].serial = "26073012";        /* the Meshstick */
  g_devs[2].serial = "CHE1W433";        /* the 433 build */
  struct pinedio_inst inst = fresh("CHE1W433");
  (void) pinedio_init(&inst, NULL);     /* the transfers fail in the fake after the selection; not judged here */
  untouched(0);
  untouched(1);
  assert(g_devs[2].claimed == 1);
  printf("test_init_public: 1 passed\n");
  return 0;
}
