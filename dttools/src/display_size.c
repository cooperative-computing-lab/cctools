#include "display_size.h"

static const char *unit_names[] = {"GB", "MB", "KB", "B"};
static const int num_units = sizeof(unit_names)/sizeof(*(unit_names));
static const uint64_t gigabytes = 1000 * 1000 * 1000;

char * human_readable_size(uint64_t size){
	char *result = (char *) malloc(sizeof(char) * 21);
	uint64_t mult = gigabytes;

	int j;
	for (j= 0; j < num_units; j++, mult /= 1000){
		if ( size < mult ) continue;
		if ( size % mult == 0 )
			sprintf(result, "%llu %s", size/mult, unit_names[j]);
		else
			sprintf(result, "%.1f %s", (float)size/mult, unit_names[j]);
		return result;
	}
	strcpy(result, "0");
	return result;
}

/* vim: set noexpandtab tabstop=4: */
