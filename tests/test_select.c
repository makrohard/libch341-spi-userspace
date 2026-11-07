/* Device selection with a fake libusb: the library's own source is compiled into this test, every libusb call it
 * makes is answered by fake_libusb.h, and each fake device counts what was done to it. Run: tests/run.sh */
#include "fake_libusb.h"
#include "../libpinedio-usb.c"
#include "fake_helpers.h"

static void test_a_serial_search_takes_only_its_stick(void) {
  devices(3);
  g_devs[0].serial = NULL;              /* the PINE64: no serial */
  g_devs[1].serial = "26073012";        /* the Meshstick */
  g_devs[2].serial = "CHE1W433";        /* the 433 build */
  struct pinedio_inst inst = fresh("CHE1W433");
  assert(pinedio_select_device(&inst, NULL) == 0);
  assert(inst.handle == &g_handles[2] && g_devs[2].claimed == 1);
  untouched(0); untouched(1);           /* visited, never detached or claimed */
  assert(g_devs[0].closed == 1 && g_devs[1].closed == 1 && strcmp(inst.serial_number, "CHE1W433") == 0);
}

static void test_a_port_selects_the_serial_less_stick_and_opens_no_other(void) {
  devices(3);
  g_devs[0].serial = "26073012"; g_devs[1].serial = NULL; g_devs[2].serial = "CHE1W433";
  struct pinedio_inst inst = fresh(NULL);
  assert(pinedio_select_device(&inst, "1-1.2") == 0);
  assert(inst.handle == &g_handles[1] && g_devs[1].claimed == 1 && inst.serial_number[0] == '\0');
  assert(g_devs[0].opened == 0 && g_devs[2].opened == 0);   /* another port is never even opened */
}

static void test_a_port_that_is_not_listed_takes_nothing(void) {
  devices(2);
  struct pinedio_inst inst = fresh(NULL);
  assert(pinedio_select_device(&inst, "1-1.9") != 0);
  untouched(0); untouched(1);
  assert(g_devs[0].opened == 0 && g_devs[1].opened == 0);
}

static void test_port_and_serial_must_both_match(void) {
  devices(2);
  g_devs[0].serial = "AAAAAAAA"; g_devs[1].serial = "BBBBBBBB";
  struct pinedio_inst inst = fresh("AAAAAAAA");
  assert(pinedio_select_device(&inst, "1-1.2") != 0);       /* port 1-1.2 holds BBBBBBBB */
  untouched(0); untouched(1);
}

static void test_a_serial_less_or_unreadable_stick_never_matches_a_serial_search(void) {
  devices(2);
  g_devs[0].serial = NULL;
  g_devs[1].serial = "26073012"; g_devs[1].serial_fails = 1;
  struct pinedio_inst inst = fresh("26073012");
  assert(pinedio_select_device(&inst, NULL) != 0);
  untouched(0); untouched(1);
}

static void test_no_selector_takes_the_first_claimable_stick(void) {
  devices(2);
  g_devs[0].claim_fails = 1; g_devs[1].serial = "26073012";
  struct pinedio_inst inst = fresh(NULL);
  assert(pinedio_select_device(&inst, NULL) == 0 && inst.handle == &g_handles[1]);
  assert(g_devs[0].closed == 1 && strcmp(inst.serial_number, "26073012") == 0);   /* the taken stick's serial */
}

static void test_an_empty_serial_search_never_matches_a_serial_less_stick(void) {
  devices(1);
  g_devs[0].serial = NULL;
  struct pinedio_inst inst = fresh(NULL);
  inst.options[PINEDIO_OPTION_SEARCH_SERIAL] = 1;           /* searching, with an empty serial */
  assert(pinedio_select_device(&inst, NULL) != 0);
  untouched(0);
}

static void test_another_vid_pid_is_never_opened(void) {
  devices(2);
  g_devs[0].vid = 0x1A86; g_devs[0].pid = 0x7523;          /* a CH340 serial adapter */
  struct pinedio_inst inst = fresh(NULL);
  assert(pinedio_select_device(&inst, NULL) == 0 && inst.handle == &g_handles[1]);
  assert(g_devs[0].opened == 0);
}

static void test_the_port_path_is_the_sysfs_name(void) {
  devices(1);
  g_devs[0].bus = 3; g_devs[0].ports[0] = 1; g_devs[0].ports[1] = 4; g_devs[0].ports[2] = 2; g_devs[0].nports = 3;
  char p[32];
  pinedio_port_path(&g_dev_objs[0], p, sizeof(p));
  assert(strcmp(p, "3-1.4.2") == 0);
  pinedio_port_path(&g_dev_objs[0], p, 5);                  /* too short: no partial path that could match */
  assert(p[0] == '\0');
}

int main(void) {
  g_dev_objs[0].idx = 0;
  test_a_serial_search_takes_only_its_stick();
  test_a_port_selects_the_serial_less_stick_and_opens_no_other();
  test_a_port_that_is_not_listed_takes_nothing();
  test_port_and_serial_must_both_match();
  test_a_serial_less_or_unreadable_stick_never_matches_a_serial_search();
  test_no_selector_takes_the_first_claimable_stick();
  test_an_empty_serial_search_never_matches_a_serial_less_stick();
  test_another_vid_pid_is_never_opened();
  test_the_port_path_is_the_sysfs_name();
  printf("test_select: 9 passed\n");
  return 0;
}
