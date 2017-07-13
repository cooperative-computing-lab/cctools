#include "terminal_size.h"

#include <unistd.h>
#include <stdlib.h>
#include <sys/ioctl.h>

void terminal_size( int *rows, int *columns )
{
	*rows = 25;
	*columns = 80;

       	struct winsize window;
	if(ioctl(STDOUT_FILENO, TIOCGWINSZ, &window) >= 0) {
		if(window.ws_row>1) *rows = window.ws_row;
		if(window.ws_col>1) *columns = window.ws_col;
	}

	char *str;

	str = getenv("COLUMNS");
	if(str && atoi(str)>1) *columns = atoi(str);

	str = getenv("ROWS");
	if(str && atoi(str)>1) *rows = atoi(str);
}

