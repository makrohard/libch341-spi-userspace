/* Shared by the poll tests: an input pin (D1) toggled through the fake, with waits. */
static void ms(long n) { struct timespec t = {n / 1000, (n % 1000) * 1000000L}; nanosleep(&t, NULL); }
static void pin_low(void) { g_in_fixed = 0x00; ms(150); }
static void pin_high(void) { g_in_fixed = 0x02; ms(150); }   /* D1 */
