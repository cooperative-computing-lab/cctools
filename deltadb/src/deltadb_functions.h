#ifndef DELTADB_FUNCTIONS_H
#define DELTADB_FUNCTIONS_H

#include "deltadb_value.h"

struct deltadb_value * deltadb_function_call( const char *name, struct deltadb_value *args );

#endif
