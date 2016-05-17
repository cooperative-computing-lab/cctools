#ifndef TWISTER_H
#define TWISTER_H

#include <stdint.h>

void twister_init_genrand64(uint64_t seed);
void twister_init_by_array64(uint64_t init_key[], uint64_t key_length);
uint64_t twister_genrand64_int64();
int64_t twister_genrand64_int63();
double twister_genrand64_real1();
double twister_genrand64_real2();
double twister_genrand64_real3();

#endif
