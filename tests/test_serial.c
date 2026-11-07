/* A stick without a serial (iSerialNumber 0), or one whose serial read fails: after pinedio_init the instance's
 * serial_number is empty and terminated, and a serial search never matches it. run.sh builds with
 * -ftrivial-auto-var-init=pattern, so an uninitialised buffer reads the same non-zero bytes on every run. */
#include "fake_libusb.h"
#include "../libpinedio-usb.c"
#include "fake_helpers.h"

static void all_zero(const struct pinedio_inst *inst) {
  for (size_t i = 0; i < sizeof(inst->serial_number); i++) {
    assert(inst->serial_number[i] == '\0');
  }
}

int main(void) {
  devices(1);                                   /* no serial */
  struct pinedio_inst a = fresh(NULL);
  (void) pinedio_init(&a, NULL);
  all_zero(&a);

  devices(1);
  g_devs[0].serial = "ABCDEFGH"; g_devs[0].serial_fails = 1;     /* the descriptor read fails */
  struct pinedio_inst b = fresh(NULL);
  (void) pinedio_init(&b, NULL);
  all_zero(&b);

  devices(1);                                   /* a search never matches a stick without a serial */
  struct pinedio_inst c = fresh("");
  assert(pinedio_init(&c, NULL) == -2 && g_devs[0].claimed == 0);
  printf("test_serial: 3 passed\n");
  return 0;
}
