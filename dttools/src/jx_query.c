/*
Copyright (C) 2020- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

/*
This is a test program for the jx library.
It first reads in a path to a JSON document which is used as the evaluation context.
Then, it reads a JX expression which is evaluated upon the given context.
The program exits either with the evaluated result of the expression or on the first failure.
*/

#include "getopt.h"
#include "hash_table.h"
#include "jx.h"
#include "jx_getopt.h"
#include "jx_parse.h"
#include "jx_print.h"
#include "jx_querytools.h"
#include "stringtools.h"

#include <errno.h>
#include <stdio.h>

int get_deposits(const char* path) {
    if(!path) return 1;
    return 0;
}

static void show_help() {
    printf("Use: ./jx_query [options] \n");
    printf("Basic Options:\n");
    printf(" -p,--path=<file>               Use the JSON document located at this path.\n");
    printf(" -u,--url=<url>                 Use the JSON document located at this URL.\n");
    printf(" -h,--help                      Show this help screen.\n");
    return;
}

int unlink_document(char *json) {
    int result = unlink(json);
    if(result == -1) {
        fprintf(stderr, "Could not delete local JSON document: %s - %s\n", json, strerror(errno));
        return 1;
    }
    return 0;
}

int main(int argc, char *argv[]) {
    int o, path, url;
    char *document;
    static const struct option long_options_run[] = {
        {"help", no_argument, 0, 'h'},
        {"path", required_argument, 0, 'p'},
        {"url", required_argument, 0, 'u'}
    };

    static const char option_string_run[] = "aAB:c::C:d:Ef:F:g:G:hj:J:l:L:m:M:N:o:Op:P:r:RS:t:T:u:vW:X:zZ:";
    struct hash_table *h = hash_table_create(0, NULL);

    while((o = jx_getopt(argc, argv, option_string_run, long_options_run, NULL)) >= 0) {
        switch(o) {
            case 'h':
                show_help();
                return 1;
            case 'p':
                if(url) {
                    fprintf(stderr, "Please use either a path or a URL, not both.\n");
                    show_help();
                    return 1;
                }
                document = optarg;
                path = 1;
                break;
            case 'u':
                if(path) {
                    fprintf(stderr, "Please use either a path or a URL, not both.\n");
                    show_help();
                    return 1;
                }
                fprintf(stderr, "Attempting to fetch: %s\n", optarg);
                char *fetch = string_format("curl %s -o tmp.json", optarg);
                int result = system(fetch);
                fprintf(stderr, "Fetch returned: %d - %s\n", result, strerror(errno));
                document = "tmp.json";
                url = 1;
                break;
            default:
                show_help();
                return 1;
        }
    }

    if(document == NULL || (!path && !url)) {
        fprintf(stderr, "Must specify a JSON document.\n");
        show_help();
        return 1;
    }

    FILE *json = NULL;
    if(path || url) json = fopen(document, "r");
    if(!json) {
        fprintf(stderr, "Error opening JSON file: %s - %s\n", document, strerror(errno));
        if(url) unlink_document(document);
        return 1;
    }

    struct jx_parser *c = jx_parser_create(0);
    if(path) jx_parser_read_stream(c, json);
    else {
        const char *parsed = jx_parse_from_html(document);
        jx_parser_read_string(c, parsed);
    }
    struct jx *context = jx_parse(c);
    fclose(json);
    if(!context) {
        fprintf(stderr, "Error parsing JSON document: %s\n", jx_parser_error_string(c));
        if(url) unlink_document(document);
        return 1;
    }
    if(jx_parser_errors(c)) {
        fprintf(stderr, "Invalid context expression: %s\n", jx_parser_error_string(c));
        if(url) unlink_document(document);
        return 1;
    }

    fprintf(stdout, "Enter expression:\n");
    while(1) {
        struct jx *k = jx_evaluate_query(context);
        if(!k) {
            jx_parser_delete(c);
            hash_table_clear(h);
            hash_table_delete(h);
            if(url) unlink_document(document);
            return 1;
        }
        else if(k->type != JX_ERROR) {
            char *value = jx_print_string(k);
            char *key = string_format("%u", hash_string(value));
            char *file = string_format("%s.json", key);
            FILE *f = fopen(file, "w");
            fprintf(f, "%s", value);
            fclose(f);
            hash_table_insert(h, key, file);
            free(value);
            fprintf(stderr, "New JX context saved with as: %s\n", file);
        }
        jx_delete(k);
        fprintf(stdout, "Enter expression:\n");
    }
    return 0;
}

// vim: tabstop=8 shiftwidth=4 softtabstop=4 expandtab shiftround autoindent
