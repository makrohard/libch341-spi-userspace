/* pinedio_init_at_port: of two devices without a serial the one on the named port is taken and the other is never
 * opened; a port with no device opens nothing; port and serial both set: both must match. */
#include "fake_libusb.h"
#include "../libpinedio-usb.c"
#include "fake_helpers.h"

int main(void) {
#ifndef PINEDIO_HAS_INIT_AT_PORT
  printf("no pinedio_init_at_port in this library\n");
  return 1;
#else
  devices(2);                                   /* 1-1.1 and 1-1.2, neither has a serial */
  struct pinedio_inst a = fresh(NULL);
  (void) pinedio_init_at_port(&a, NULL, "1-1.2");
  assert(g_devs[1].claimed == 1 && g_devs[0].opened == 0);
  devices(2);
  struct pinedio_inst b = fresh(NULL);
  assert(pinedio_init_at_port(&b, NULL, "1-1.7") == -2);
  assert(g_devs[0].opened == 0 && g_devs[1].opened == 0);
  devices(2);
  g_devs[1].serial = "ABCDEFGH";
  struct pinedio_inst c = fresh("ZZZZZZZZ");
  assert(pinedio_init_at_port(&c, NULL, "1-1.2") == -2 && g_devs[1].claimed == 0);
  printf("test_port_select: 3 passed\n");
  return 0;
#endif
}
