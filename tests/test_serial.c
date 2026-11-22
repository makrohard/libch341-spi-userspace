/* Through pinedio_init: a device without a serial (iSerialNumber 0) or with a failed serial read leaves the
 * instance's serial_number empty and terminated, and an empty serial never matches a search. Built with
 * -ftrivial-auto-var-init=pattern so an uninitialised local reads the same bytes on every run. */
#include "fake_libusb.h"
#include "../libpinedio-usb.c"
#include "fake_helpers.h"

static int empty(const char *what, struct pinedio_inst *inst) {
  int ok = inst->serial_number[0] == '\0';
  printf("%s: serial_number[0..8] =", what);
  for (int i = 0; i < 9; i++) printf(" %02x", (unsigned char) inst->serial_number[i]);
  printf(" -> %s\n", ok ? "empty" : "NOT EMPTY (uninitialised stack bytes copied)");
  return ok;
}

int main(void) {
  int ok = 1;
  devices(1);                                                    /* no serial */
  struct pinedio_inst a = fresh(NULL);
  (void) pinedio_init(&a, NULL);
  ok &= empty("no serial", &a);
  devices(1);
  g_devs[0].serial = "SERIAL01"; g_devs[0].serial_fails = 1;     /* the descriptor read fails */
  struct pinedio_inst b = fresh(NULL);
  (void) pinedio_init(&b, NULL);
  ok &= empty("serial read fails", &b);
  devices(2);                                                    /* a search: the failed read is skipped */
  g_devs[0].serial = "SERIAL01"; g_devs[0].serial_fails = 1;
  g_devs[1].serial = "SERIAL02";
  struct pinedio_inst c = fresh("SERIAL02");
  (void) pinedio_init(&c, NULL);
  int took2 = g_devs[1].claimed == 1 && strcmp(c.serial_number, "SERIAL02") == 0;
  printf("search, first read fails: %s\n", took2 ? "second device taken" : "WRONG");
  ok &= took2;
  devices(1);                                                    /* an empty search never matches */
  struct pinedio_inst d = fresh("");
  int r = pinedio_init(&d, NULL);
  printf("empty search, device without a serial: rc %d -> %s\n", r, r == -2 ? "no match" : "MATCHED");
  ok &= r == -2;
  printf("test_serial: %s\n", ok ? "4 passed" : "FAILED");
  return ok ? 0 : 1;
}
