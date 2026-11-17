// SPDX-License-Identifier: GPL-2.0-or-later
/* Private to the shared object the library is built into (not part of libpinedio-usb.h): one diagnostic line to
 * stderr by the library's rules (libpinedio-usb.c, "Diagnostics"): it never blocks on stderr, never ends or stops
 * the caller by SIGPIPE, SIGXFSZ or SIGTTOU, drops and counts what it cannot write, and leaves errno as it was. */
#ifndef LIBPINEDIO_USB_DIAG_H
#define LIBPINEDIO_USB_DIAG_H
#ifdef __cplusplus
extern "C" {
#endif
__attribute__((visibility("hidden"), format(printf, 1, 2)))
void pinedio_diag_printf(const char *fmt, ...);
#ifdef __cplusplus
}
#endif
#endif
