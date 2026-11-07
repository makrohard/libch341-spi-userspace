/* pinedio_attach_interrupt / pinedio_deattach_interrupt index interrupts[PINEDIO_INT_PIN_MAX] with the pin they are
 * given: a pin outside the array (11 = SLCT, 255) is refused with -1 and nothing past the array changes (the options
 * that follow it in struct pinedio_inst). Run: tests/run.sh */
#include "fake_libusb.h"
#include "../libpinedio-usb.c"
#include "fake_helpers.h"

static void cb(void) {}

int main(void) {
  for (unsigned pin = PINEDIO_INT_PIN_MAX; pin <= 255; pin += 244) {          /* 11 and 255 */
    struct pinedio_inst inst = fresh(NULL);
    pthread_mutex_init(&inst.usb_access_mutex, NULL);
    struct pinedio_inst before;
    memcpy(&before, &inst, sizeof(inst));
    assert(pinedio_attach_interrupt(&inst, (enum pinedio_int_pin) pin, PINEDIO_INT_MODE_RISING, cb) == -1);
    assert(memcmp(inst.options, before.options, sizeof(inst.options)) == 0);
    assert(memcmp(inst.serial_number, before.serial_number, sizeof(inst.serial_number)) == 0);
    assert(inst.int_running_cnt == 0);
    assert(pinedio_deattach_interrupt(&inst, (enum pinedio_int_pin) pin) == -1);
    inst.int_running_cnt = 1;                    /* a poll thread running: the guard must not reach the array */
    memcpy(&before, &inst, sizeof(inst));
    assert(pinedio_deattach_interrupt(&inst, (enum pinedio_int_pin) pin) == -1);
    assert(memcmp(inst.options, before.options, sizeof(inst.options)) == 0 && inst.int_running_cnt == 1);
  }
  printf("test_interrupt_pin: 2 passed\n");
  return 0;
}
