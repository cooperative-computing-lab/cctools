/*
Copyright (C) 2010- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/
#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <sys/stat.h>
#include <stringtools.h>
#include <console_login.h>
#include "s3common.h"


char *userid = NULL;
char *key = NULL;

inline const char* s3_userid() { return userid; }
inline const char* s3_key() { return key; }

int process_userpass(char *userpass, char **username, char **password) {
	int nargs;
	char **args;
	
	if(string_split(userpass,&nargs,&args)) {
		if(!*username) *username = strdup(args[0]);
		if(!*password) *password = strdup(args[1]);
		free(args);
		return 0;
	}
	return -1;
}

int process_configfile(char *configfile, char **username, char **password) {
	char *userpass;
	struct stat st_buf;
	FILE* config;

	if(stat(configfile, &st_buf)) return -1;
	userpass = malloc(sizeof(*userpass) * st_buf.st_size);
	config = fopen(configfile, "r");
	fread(userpass, st_buf.st_size, 1, config);
	fclose(config);

	return process_userpass(userpass, username, password);

}

void s3_initialize(int* argc, char** argv) {
	int i, mod, result, prompt = 0;
	char *username = NULL, *password = NULL, *configfile = NULL, *endpoint = NULL;
	char **argv2;

	for(i = 0; i < *argc; i++) {
		if(argv[i][0] == '-') {
			switch(argv[i][1]) {
				case 'e':
					endpoint = argv[i+1];
					i++;
					break;
				case 'u':
					if(username) free(username);
					username = argv[i+1];
					i++;
					break;
				case 'P':
					if(password) free(password);
					password = argv[i+1];
					i++;
					break;
				case 'p':
					prompt = 1;
					break;
				case 'c':
					if(configfile) free(configfile);
					configfile = argv[i+1];
					i++;
					break;
				case 'd':
			//		debug = 1;
					break;
				default:
					continue;
			}
		}
	}

	mod = 0;
	
	argv2 = malloc(*argc * sizeof(*argv2));
	for(i = 0; i < *argc; i++) {
		if(argv[i][0] == '-') {
			char c = argv[i][1];
			if(c == 'u' || c == 'P' || c == 'c') { i++; continue; }
			else if(c == 'p' || c == 'd') { continue; }
		}
		argv2[mod++] = argv[i];
	}
	for(i = 0; i < mod; i++) argv[i] = argv2[i];
	free(argv2);

	*argc = mod;


	if(!username || !password) {
		char* env = NULL;
		env = getenv("S3_USER_KEY");

		if(prompt) {
			if(!username) {
				username = malloc(1024);
				password = malloc(1024);
				if(!username || !password) exit(-1);
				console_login( "s3", username, 1024, password, 1024 );
			} else {
				password = malloc(1024);
				if(!password) exit(-1);
				console_input( "password:", password, 1024 );
			}

		} else if(env) process_userpass(env, &username, &password);
		else if(configfile) process_configfile(configfile, &username, &password);
		else {
			char default_configfile[2048];
			sprintf(default_configfile, "%s/%s", getenv("HOME"), DEFAULT_CONFIGFILE_NAME);
			process_configfile(default_configfile, &username, &password);
		}
	}

	result = s3_register_userid(username, password);
	if(result < 0) {
		fprintf(stderr, "Error: no username or password specified\n");
		exit(result);
	}
	
	if(endpoint || (endpoint = getenv("S3_ENDPOINT"))) {
		s3_set_endpoint(endpoint);
	}
}


int s3_register_userid(const char *new_userid, const char* new_key) {
	if(!new_userid || !new_key) return -1;

	s3_clear_userid();
	userid = strdup(new_userid);
	key = strdup(new_key);

	if(userid && key) return 0;

	s3_clear_userid();
	return -1;
}

void s3_clear_userid() {
	if(userid) {
		int length = strlen(userid);
		memset(userid, 0, length);
		free(userid);
		userid = NULL;
	}
	if(key) {
		int length = strlen(key);
		memset(key, 0, length);
		free(key);
		key = NULL;
	}
}

