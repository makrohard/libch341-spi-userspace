/* Through pinedio_transceive: every received byte comes back bit-reversed at its own index (the line that read and
 * incremented read_buf in one expression). The fake completes each transfer; IN data is 0x01, 0x02, ... */
#include "fake_libusb.h"
#include "../libpinedio-usb.c"
#include "fake_helpers.h"

int main(void) {
  g_complete = 1;
  devices(1);
  struct pinedio_inst inst = fresh(NULL);
  assert(pinedio_init(&inst, NULL) == 0);
  for (unsigned count = 1; count <= 70; count += 23) {           /* 1, 24, 47, 70 bytes: one to three packets */
    uint8_t w[70], r[70];
    memset(w, 0x5a, sizeof(w));
    g_in_next = 1;
    assert(pinedio_transceive(&inst, w, r, count) == 0);
    for (unsigned i = 0; i < count; i++)
      if (r[i] != reverse_byte((uint8_t) (i + 1))) {
        printf("test_transceive: count %u byte %u = %02x, want %02x\n", count, i, r[i], reverse_byte((uint8_t) (i + 1)));
        return 1;
      }
  }
  pinedio_deinit(&inst);
  printf("test_transceive: 4 passed\n");
  return 0;
}
