
#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <string.h>

#include "find_in_path.h"

char * find_in_path( const char *cmd )
{
	char tmp[4096];

	if(access(cmd,X_OK)==0) return strdup(cmd);

	char * path = strdup(getenv("PATH"));
	char * p = strtok(path,":");
	while(p) {
		sprintf(tmp,"%s/%s",p,cmd);
		if(access(tmp,X_OK)==0) {
			free(path);
			return strdup(tmp);
		}
		p = strtok(0,":");
	}

	free(path);
	return 0;
}

