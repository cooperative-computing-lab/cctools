#include "s3client.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stringtools.h>
#include <time.h>
#include <unistd.h>

#include "s3passwd.h"

int main(int argc, char** argv) {
	struct list *dirents;
	struct s3_dirent_object *d;
	char long_list = 0;
	char c;
	int i;

	while( (c = getopt(argc, argv, "l")) != -1 ) {
		switch(c) {
			case 'l':
				long_list = 1;
				break;
			default:
				fprintf(stderr, "Error: invalid option (-%c)\n", c);
		}
	}

	if(optind >= argc) {
		fprintf(stderr, "usage: s3ls [-l] <bucket>\n");
		return -1;
	}

	dirents = list_create();
	for(i = optind; i < argc; i++) {
		char date[1024];
		if(argc-optind > 1) printf("%s:\n", argv[i]);
		s3_ls_bucket(argv[i], dirents, userid, key);
		while( (d = list_pop_head(dirents)) ) {
			strftime(date, 1024, "%b %d %H:%M", localtime(&d->last_modified));
			if(!long_list) printf("%s\n", d->key);
			else printf("-rw-------  1 %s\t%9d %s %s\n", d->display_name, d->size, date, d->key);
			free(d->display_name);
			free(d);
			d = NULL;
		}
	}
	list_delete(dirents);

	return 0;
}

