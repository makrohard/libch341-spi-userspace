/* Helpers over fake_libusb.h, after the library's source is included. */
#ifndef FAKE_HELPERS_H
#define FAKE_HELPERS_H
static void devices(int n) {
  memset(g_devs, 0, sizeof(g_devs));
  g_ndev = n;
  for (int i = 0; i < n; i++) {
    g_devs[i].vid = 0x1A86; g_devs[i].pid = 0x5512; g_devs[i].bus = 1;
    g_devs[i].ports[0] = 1; g_devs[i].ports[1] = (uint8_t) (i + 1); g_devs[i].nports = 2;   /* 1-1.<i+1> */
  }
}

static struct pinedio_inst fresh(const char *serial) {
  struct pinedio_inst inst;
  memset(&inst, 0, sizeof(inst));
  inst.options[PINEDIO_OPTION_VID] = 0x1A86;
  inst.options[PINEDIO_OPTION_PID] = 0x5512;
  if (serial) {
    inst.options[PINEDIO_OPTION_SEARCH_SERIAL] = 1;
    strncpy(inst.serial_number, serial, 8);
  }
  return inst;
}

static void untouched(int i) { assert(g_devs[i].detached == 0 && g_devs[i].claimed == 0); }

#endif
