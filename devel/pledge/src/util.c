#include "util.h"

/// Turn a relative path into an absolute path, based on CWD
/// @param abs_p is the buffer where we will store the absolute path
/// @param rel_p is the user given string
/// @param size is the size/len of abs_p
/// Return If the path does not need to be turned into an absolute path then we just
/// copy to the abs_p buffer and return that
/// TODO: Perhaps we do strlcpy/strlcat?
char *
rel2abspath(char *abs_p,
		char *rel_p,
		size_t size)
{
	if (rel_p == NULL) {
		fprintf(stderr, "Attempted to turn an empty string into an absolute path.\n");
		abs_p = NULL;
		return NULL;
	}
	// Strnlen??
	size_t rel_p_len = strlen(rel_p);
	if (rel_p_len >= 1) {
		if (rel_p[0] != '/') {
			if (rel_p_len >= 2) {
				if (rel_p[0] == '.' && rel_p[1] == '/') {
					rel_p = rel_p + 2;
				}
			}
			// get cwd
			if (getcwd(abs_p, size) == NULL) {
				fprintf(stderr, "Attempt to obtain cwd failed.\n");
				return rel_p;
			}
			size_t abs_p_len = strlen(abs_p);
			if (abs_p[abs_p_len - 1] != '/') {
				strncat(abs_p, "/", MAXPATHLEN);
			}
			strncat(abs_p, rel_p, MAXPATHLEN);
			return abs_p;
		}
		strncpy(abs_p, rel_p, MAXPATHLEN);
		return abs_p;
	}
	strncpy(abs_p, rel_p, MAXPATHLEN);
	return abs_p;
}
