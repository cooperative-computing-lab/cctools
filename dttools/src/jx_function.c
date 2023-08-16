/*
Copyright (C) 2022 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include <assert.h>
#include <math.h>
#include <inttypes.h>
#include <stdarg.h>
#include <stdbool.h>
#include <stdio.h>
#include <string.h>
#include <libgen.h>
#include <sys/types.h>
#include <dirent.h>
#include <errno.h>
#include <ctype.h>
#include <unistd.h>
#include <stdarg.h>

#include "jx.h"
#include "jx_eval.h"
#include "jx_sub.h"
#include "jx_match.h"
#include "jx_parse.h"
#include "jx_print.h"
#include "stringtools.h"
#include "xxmalloc.h"
#include "buffer.h"

#include "jx_function.h"

typedef enum {
    JX_DOUBLE_ARG=1,    // Function takes two arguments.
    JX_DEFER_EVAL=2,    // Defer evaluation of second argument.
    JX_EXTERNAL=4       // Function uses data external to context.
} jx_function_flags;

typedef union {
    struct jx * (*single_arg)(struct jx *args);
    struct jx * (*double_arg)(struct jx *args, struct jx *ctx);
} jx_function_pointer;

struct jx_function_info {
    const char *name;
    const char *help_text;
    jx_function_flags flags;
    jx_function_pointer function_pointer;
};

const struct jx_function_info jx_functions[] = {
    { "range", "range( start, stop, step )", 0, { .single_arg = jx_function_range }},
    { "format", "format( str: %s int: %d float: %f\", \"hello\", 42, 3.14159 )", 0,{ .single_arg = jx_function_format }},
    { "join", "join( array, delim )", 0, { .single_arg = jx_function_join }},
    { "ceil", "ceil( value )", 0, { .single_arg = jx_function_ceil }},
    { "floor", "floor( value )", 0, { .single_arg = jx_function_floor }},
    { "basename", "basename( path )", 0, { .single_arg = jx_function_basename }},
    { "dirname", "dirname( path )", 0, { .single_arg = jx_function_dirname }},
    { "listdir", "listdir( path )", JX_EXTERNAL, { .single_arg = jx_function_listdir }},
    { "escape", "escape( string )", 0, { .single_arg = jx_function_escape }},
    { "len", "len( array )", 0, { .single_arg = jx_function_len }},
    { "fetch", "fetch( URL/path )", JX_EXTERNAL, { .single_arg = jx_function_fetch }},
    { "schema", "schema( object )", 0, { .single_arg = jx_function_schema }},
    { "like", "like( object, string )", 0, { .single_arg = jx_function_like }},
    { "keys", "keys( object )", 0, { .single_arg = jx_function_keys }},
    { "values", "values( object )", 0, { .single_arg = jx_function_values }},
    { "items", "items( object )", 0, { .single_arg = jx_function_items}},
    { "template", "template( string [,object] )", JX_DOUBLE_ARG, { .double_arg = jx_function_template }},
    { "select", "select( array, boolean )", JX_DOUBLE_ARG|JX_DEFER_EVAL, { .double_arg = jx_function_select }},
    { "where", "where( array, boolean )", JX_DOUBLE_ARG|JX_DEFER_EVAL, { .double_arg = jx_function_select }},
    { "project", "project( array, expression )", JX_DOUBLE_ARG|JX_DEFER_EVAL, { .double_arg = jx_function_project }},
    { 0, 0, 0, {0} }
};

 
static struct jx *make_error(const char *funcname, struct jx *args, const char *fmt, ...) {
    assert(funcname);
    assert(args);
    assert(fmt);

    va_list ap;
    buffer_t buf;

    buffer_init(&buf);
    buffer_printf(&buf, "function %s on line %d: ", funcname, args->line);

    va_start(ap, fmt);
    buffer_vprintf(&buf, fmt, ap);
    va_end(ap);

    struct jx *err = jx_error(jx_string(buffer_tostring(&buf)));
    buffer_free(&buf);
    return err;
}

extern int __jx_eval_external_functions_flag;

struct jx * jx_function_eval(const char *funcname, struct jx *args, struct jx *ctx) {
    int i = 0;
    struct jx_function_info info;

    while ((info = jx_functions[i++]).name != NULL) {
        if (strcmp(info.name, funcname) != 0) {
            continue;
        }

        if(info.flags&JX_EXTERNAL && !__jx_eval_external_functions_flag) {
            return make_error(funcname,args,"external functions disabled");
        }

        struct jx *arg;
        if(info.flags&JX_DEFER_EVAL) {
            arg = jx_copy(args);
        } else {
            arg = jx_eval(args,ctx);
        }

        if (info.flags&JX_DOUBLE_ARG) {
            return (*info.function_pointer.double_arg)(arg, ctx);
        } else {
            return (*info.function_pointer.single_arg)(arg);
        }
    }

    return make_error(funcname, args, "invalid function name");
}

struct jx * jx_function_sub(const char *funcname, struct jx *args, struct jx *ctx) {
    int i = 0;
    struct jx_function_info info;

    while ((info = jx_functions[i++]).name != NULL) {
        if (strcmp(info.name, funcname) != 0) {
            continue;
        }

        if (!(info.flags & JX_DEFER_EVAL)) {
            return jx_sub(args, ctx);
        } else {
            // only sub objlist (ignoring select's boolean and project's expression)
            struct jx *val = jx_array_index(args, 0);
            struct jx *objlist = jx_array_index(args, 1);

            struct jx *new_val = jx_copy(val);
            struct jx *new_objlist = jx_sub(objlist, ctx);

            // add back to args
            struct jx *ret = jx_array(0);
            jx_array_append(ret, new_val);
            jx_array_append(ret, new_objlist);

            return ret;
        }
    }

    return make_error(funcname, args, "invalid function name");
}

void jx_function_help(FILE *file) {
    int i = 0;
    struct jx_function_info info;

    fprintf(file, "\n");
    while ((info = jx_functions[i++]).name != NULL) {
        fprintf(file, "  %s\n", info.help_text);
    }
    fprintf(file, "\n");
}


static char *jx_function_format_value(char spec, struct jx *args) {
    if (spec == '%') return xxstrdup("%");
    char *result = NULL;
    struct jx *j = jx_array_shift(args);
    switch (spec) {
        case 'd':
        case 'i':
            if (jx_istype(j, JX_INTEGER))
                result = string_format(
                    "%" PRIi64, j->u.integer_value);
            break;
        case 'e':
            if (jx_istype(j, JX_DOUBLE))
                result = string_format("%e", j->u.double_value);
            break;
        case 'E':
            if (jx_istype(j, JX_DOUBLE))
                result = string_format("%E", j->u.double_value);
            break;
        case 'f':
            if (jx_istype(j, JX_DOUBLE))
                result = string_format("%f", j->u.double_value);
            break;
        case 'F':
            if (jx_istype(j, JX_DOUBLE))
                result = string_format("%F", j->u.double_value);
            break;
        case 'g':
            if (jx_istype(j, JX_DOUBLE))
                result = string_format("%g", j->u.double_value);
            break;
        case 'G':
            if (jx_istype(j, JX_DOUBLE))
                result = string_format("%G", j->u.double_value);
            break;
        case 's':
            if (jx_istype(j, JX_STRING))
                result = xxstrdup(j->u.string_value);
            break;
        default:
            break;
    }
    jx_delete(j);
    return result;
}

struct jx *jx_function_format(struct jx *args) {
    assert(args);
    if (jx_istype(args, JX_ERROR)) return args;
    const char *func = "format";
    char *format = NULL;
    char *result = xxstrdup("");
    struct jx *j = jx_array_shift(args);
    if (!jx_match_string(j, &format)) {
        jx_delete(j);
        j = make_error(func, args, "invalid/missing format string");
        goto FAILURE;
    }
    jx_delete(j);
    char *i = format;
    bool spec = false;
    while (*i) {
        if (spec) {
            spec = false;
            char *next = jx_function_format_value(*i, args);
            if (!next) {
                j = make_error(func, args, "mismatched format specifier");
                goto FAILURE;
            }
            result = string_combine(result, next);
            free(next);
        } else if (*i == '%') {
            spec = true;
        } else {
            char next[2];
            snprintf(next, 2, "%c", *i);
            result = string_combine(result, next);
        }
        ++i;
    }
    if (spec) {
        j = make_error(func, args, "truncated format specifier");
        goto FAILURE;
    }
    if (jx_array_length(args) > 0) {
        j = make_error(func, args, "too many arguments for format specifier");
        goto FAILURE;
    }

    j = jx_string(result);
FAILURE:
    jx_delete(args);
    free(result);
    free(format);
    return j;
}

// see https://docs.python.org/2/library/functions.html#range
struct jx *jx_function_range(struct jx *args) {
    assert(args);
    if (jx_istype(args, JX_ERROR)) return args;
    const char *func = "range";
    jx_int_t start, stop, step;
    struct jx *result = NULL;

    assert(args);
    switch (jx_match_array(args, &start, JX_INTEGER, &stop, JX_INTEGER, &step, JX_INTEGER, NULL)) {
        case 1:
            stop = start;
            start = 0;
            step = 1;
            break;
        case 2: step = 1; break;
        case 3: break;
        default:
            result = make_error(func, args, "invalid arguments");
            goto FAILURE;
    }

    if (step == 0) {
        result = make_error(func, args, "step must be nonzero");
        goto FAILURE;
    }

    result = jx_array(NULL);

    if (((stop - start) * step) < 0) {
        // step is pointing the wrong way
        goto FAILURE;
    }

    for (jx_int_t i = start; stop >= start ? i < stop : i > stop; i += step) {
        jx_array_append(result, jx_integer(i));
    }

FAILURE:
    jx_delete(args);
    return result;
}


struct jx *jx_function_join(struct jx *args) {
    assert(args);
    if (jx_istype(args, JX_ERROR)) return args;
    const char *func = "join";
    char *result = NULL;
    struct jx *j = NULL;

    struct jx *list = NULL;
    struct jx *delimeter= NULL; 

    int length = jx_array_length(args);
    if(length>2){
        j = make_error(func, args, "too many arguments to join");
        goto FAILURE;
    }
    else if(length<=0){
        j = make_error(func, args, "too few arguments to join");
        goto FAILURE;
    }
    
    list = jx_array_shift(args);
    if (!jx_istype(list, JX_ARRAY)){
        j = make_error(func, args, "A list must be the first argument in join");
        goto FAILURE;
    }
    
    if (length==2){
        delimeter  = jx_array_shift(args);
        if(!jx_istype(delimeter, JX_STRING)){
            j = make_error(func, args, "A delimeter must be defined as a string");
            goto FAILURE;
        }
    }
    
    result=xxstrdup("");    
    struct jx *value=NULL;
    for (size_t location = 0; (value = jx_array_shift(list)); location++){
        if (!jx_istype(value, JX_STRING)){
            j = make_error(func, args, "All array values must be strings");
            goto FAILURE;
        }
        if(location > 0){   
            if(delimeter) result = string_combine(result, delimeter->u.string_value);
            else result = string_combine(result, " ");
        }
        result = string_combine(result, value->u.string_value);
        jx_delete(value);
    }

    j = jx_string(result);
FAILURE:
    free(result);
    jx_delete(args);
    jx_delete(list);
    jx_delete(delimeter);
    return j;
}

struct jx *jx_function_ceil(struct jx *args) {
    assert(args);
    if (jx_istype(args, JX_ERROR)) return args;
    const char *func = "ceil";
    struct jx *result = NULL;
    struct jx *val = NULL;

    int length = jx_array_length(args);
    if(length>1){
        result = make_error(func, args, "too many arguments");
        goto FAILURE;
    } else if(length<=0){
        result = make_error(func, args, "too few arguments");
        goto FAILURE;
    }

    val = jx_array_shift(args);

    switch (val->type) {
        case JX_DOUBLE:
            result = jx_double(ceil(val->u.double_value));
            break;
        case JX_INTEGER:
            result = jx_integer(ceil(val->u.integer_value));
            break;
        default:
            result = make_error(func, args, "arg of invalid type");
            goto FAILURE;
    }   

FAILURE:
    jx_delete(args);
    jx_delete(val);
    return result;
}

struct jx *jx_function_floor(struct jx *args) {
    assert(args);
    if (jx_istype(args, JX_ERROR)) return args;
    const char *func = "floor";
    struct jx *result = NULL;   
    struct jx *val = NULL;

    int length = jx_array_length(args);
    if(length>1){
        result = make_error(func, args, "too many arguments");
        goto FAILURE;
    } else if(length<=0){
        result = make_error(func, args, "too few arguments");
        goto FAILURE;
    }

    val = jx_array_shift(args);

    switch (val->type) {
        case JX_DOUBLE:
            result = jx_double(floor(val->u.double_value));
            break;
        case JX_INTEGER:
            result = jx_integer(floor(val->u.integer_value));
            break;
        default:
            result = make_error(func, args, "arg of invalid type");
            goto FAILURE;
    }   

FAILURE:
    jx_delete(args);
    jx_delete(val);
    return result;
}


struct jx *jx_function_basename(struct jx *args) {
    assert(args);
    if (jx_istype(args, JX_ERROR)) return args;
    const char *func = "basename";
    struct jx *result = NULL;

    int length = jx_array_length(args);
    if (length < 1){
        result = make_error(func, args, "one argument is required");
        goto FAILURE;
    }
    if (length > 2){
        result = make_error(func, args, "only two arguments are allowed");
        goto FAILURE;
    }

    struct jx *path = jx_array_index(args, 0);
    assert(path);
    struct jx *suffix = jx_array_index(args, 1);

    if (!jx_istype(path, JX_STRING)) {
        result = make_error(func, args, "path must be a string");
        goto FAILURE;
    }
    if (suffix && !jx_istype(suffix, JX_STRING)) {
        result = make_error(func, args, "suffix must be a string");
        goto FAILURE;
    }

    char *tmp = xxstrdup(path->u.string_value);
    char *b = basename(tmp);
    char *s = suffix ? suffix->u.string_value : NULL;
    if (s && string_suffix_is(b, s)) {
        result = jx_string(string_front(b, strlen(b) - strlen(s)));
    } else {
        result = jx_string(b);
    }
    free(tmp);

FAILURE:
    jx_delete(args);
    return result;
}

struct jx *jx_function_dirname(struct jx *args) {
    assert(args);
    if (jx_istype(args, JX_ERROR)) return args;
    const char *func = "dirname";
    struct jx *result = NULL;

    int length = jx_array_length(args);
    if (length != 1){
        result = make_error(func, args, "dirname takes one argument");
        goto FAILURE;
    }

    struct jx *a = jx_array_index(args, 0);
    assert(a);

    if (!jx_istype(a, JX_STRING)) {
        result = make_error(func, args, "dirname takes a string");
        goto FAILURE;
    }
    char *val = xxstrdup(a->u.string_value);
    result = jx_string(dirname(val));
    free(val);

FAILURE:
    jx_delete(args);
    return result;
}

struct jx *jx_function_listdir(struct jx *args) {
    assert(args);
    if (jx_istype(args, JX_ERROR)) return args;
    const char *func = "listdir";
    struct jx *out = NULL;

    int length = jx_array_length(args);
    if (length != 1) {
        out = make_error(func, args, "one argument required, %d given", length);
        goto FAILURE;
    }

    struct jx *a = jx_array_index(args, 0);
    assert(a);

    if (!jx_istype(a, JX_STRING)) {
        out = make_error(func, args, "string path required");
        goto FAILURE;
    }

    DIR *d = opendir(a->u.string_value);
    if (!d) {
        out = make_error(func, args, "%s, %s",
            a->u.string_value,
            strerror(errno));
        goto FAILURE;
    }

    out = jx_array(NULL);
    for (struct dirent *e; (e = readdir(d));) {
        if (!strcmp(".", e->d_name)) continue;
        if (!strcmp("..", e->d_name)) continue;
        jx_array_append(out, jx_string(e->d_name));
    }
    closedir(d);
FAILURE:
    jx_delete(args);
    return out;
}

struct jx *jx_function_escape(struct jx *args) {
    assert(args);
    if (jx_istype(args, JX_ERROR)) return args;
    const char *func = "escape";
    struct jx *result = NULL;

    int length = jx_array_length(args);
    if (length != 1){
        result = make_error(func, args, "escape takes one argument");
        goto FAILURE;
    }

    struct jx *a = jx_array_index(args, 0);
    assert(a);

    if (!jx_istype(a, JX_STRING)) {
        result = make_error(func, args, "escape takes a string");
        goto FAILURE;
    }

    char *val = string_escape_shell(a->u.string_value);
    result = jx_string(val);
    free(val);
FAILURE:
    jx_delete(args);
    return result;
}

static struct jx *expand_template(struct jx *template, struct jx *ctx, struct jx *overrides) {
    const char *func = "template";

    assert(template);
    assert(jx_istype(template, JX_STRING));
    assert(!ctx || jx_istype(ctx, JX_OBJECT));
    assert(!overrides || jx_istype(overrides, JX_OBJECT));

    char *s = template->u.string_value;
    struct jx *out = NULL;

    buffer_t buf;
    buffer_t var;
    buffer_init(&buf);
    buffer_init(&var);

    while (*s) {
        // regular character
        if (*s != '{' && *s != '}') {
            buffer_putlstring(&buf, s, 1);
            s++;
            continue;
        }
        // quoted {
        if (*s == '{' && *(s+1) == '{') {
            buffer_putliteral(&buf, "{");
            s += 2;
            continue;
        }
        // quoted }
        if (*s == '}' && *(s+1) == '}') {
            buffer_putliteral(&buf, "}");
            s += 2;
            continue;
        }

        // got to here, so must be an expression
        if (*s != '{') {
            out = make_error(func, template, "unmatched } in template");
            goto FAILURE;
        }
        s++;
        while (isspace(*s)) s++; // eat leading whitespace

        if (*s == 0) {
            out = make_error(func, template,
                "unterminated template expression");
            goto FAILURE;
        }
        if (!isalpha(*s) && *s != '_') {
            out = make_error(func, template,
                "invalid template; each expression must be a single identifier");
            goto FAILURE;
        }
        buffer_putlstring(&var, s, 1); // copy identifier to buffer
        s++;
        while (isalnum(*s) || *s == '_') {
            buffer_putlstring(&var, s, 1);
            s++;
        }
        while (isspace(*s)) s++; // eat trailing whitespace

        if (*s == 0) {
            out = make_error(func, template,
                "unterminated template expression");
            goto FAILURE;
        }
        if (*s != '}') {
            out = make_error(func, template,
                "invalid template; each expression must be a single identifier");
            goto FAILURE;
        }
        s++;
        struct jx *k = jx_lookup(overrides, buffer_tostring(&var));
        if (!k) {
            k = jx_lookup(ctx, buffer_tostring(&var));
        }
        if (!k) {
            out = make_error(func, template, "undefined symbol in template");
            goto FAILURE;
        }
        switch (k->type) {
            case JX_INTEGER:
            case JX_DOUBLE:
                jx_print_buffer(k, &buf);
                break;
            case JX_STRING:
                buffer_putstring(&buf, k->u.string_value);
                break;
            default:
                out = make_error(func, template,
                    "cannot format expression in template");
                goto FAILURE;
        }
        buffer_rewind(&var, 0);
    }

    out = jx_string(buffer_tostring(&buf));
FAILURE:
    buffer_free(&buf);
    buffer_free(&var);
    return out;
}

struct jx *jx_function_template(struct jx *args, struct jx *ctx) {
    assert(args);
    if (jx_istype(args, JX_ERROR)) return args;
    assert(jx_istype(args, JX_ARRAY));
    assert(!ctx || jx_istype(ctx, JX_OBJECT));

    const char *func = "template";
    struct jx *template = jx_array_index(args, 0);
    struct jx *overrides = jx_array_index(args, 1);
    struct jx *out = NULL;

    switch (jx_array_length(args)) {
    case 0:
        out = make_error(func, args, "template string is required");
        goto FAILURE;
    case 2:
        if (!jx_istype(overrides, JX_OBJECT)) {
            out = make_error(func, args, "overrides must be an object");
            goto FAILURE;
        }
        /* Else falls through. */
    case 1:
        if (!jx_istype(template, JX_STRING)) {
            out = make_error(func, args, "template must be a string");
            goto FAILURE;
        }
        out = expand_template(template, ctx, overrides);
        break;
    default:
        out = make_error(func, args, "at most two arguments are allowed");
        goto FAILURE;
    }

FAILURE:
    jx_delete(args);
    return out;
}

struct jx *jx_function_len(struct jx *args){
    assert(args);
    if (jx_istype(args, JX_ERROR)) return args;
    assert(jx_istype(args, JX_ARRAY));

    const char *func = "len";
    struct jx *out = NULL;

    struct jx* item = jx_array_index(args, 0);
    if (!jx_istype(item, JX_ARRAY)) {
        out = make_error(func, args, "argument must be an array");
        goto FAILURE;
    }

    int length = jx_array_length(item);

    out = jx_integer(length);
FAILURE:
    jx_delete(args);
    return out;

}

struct jx *jx_function_keys(struct jx *args){
    assert(args);
    if (jx_istype(args, JX_ERROR)) return args;
    assert(jx_istype(args, JX_ARRAY));

    const char *func = "keys";
    struct jx *out = NULL;

    if (jx_array_length(args) != 1) {
        out = make_error(func, args, "exactly 1 argument required");
        goto FAILURE;
    }

    struct jx* item = jx_array_index(args, 0);
    if (!jx_istype(item, JX_OBJECT)) {
        out = make_error(func, args, "argument must be an object");
        goto FAILURE;
    }

    out = jx_array(NULL);
    const char *key;
    for (void *i = NULL; (key = jx_iterate_keys(item, &i));) {
        jx_array_insert(out, jx_string(key));
    }

FAILURE:
    jx_delete(args);
    return out;

}

struct jx *jx_function_values(struct jx *args){
    assert(args);
    if (jx_istype(args, JX_ERROR)) return args;
    assert(jx_istype(args, JX_ARRAY));

    const char *func = "keys";
    struct jx *out = NULL;

    if (jx_array_length(args) != 1) {
        out = make_error(func, args, "exactly 1 argument required");
        goto FAILURE;
    }

    struct jx* item = jx_array_index(args, 0);
    if (!jx_istype(item, JX_OBJECT)) {
        out = make_error(func, args, "argument must be an object");
        goto FAILURE;
    }

    out = jx_array(NULL);
    struct jx *value;
    for (void *i = NULL; (value = jx_iterate_values(item, &i));) {
        jx_array_insert(out, jx_copy(value));
    }

FAILURE:
    jx_delete(args);
    return out;

}

struct jx *jx_function_items(struct jx *args){
    assert(args);
    if (jx_istype(args, JX_ERROR)) return args;
    assert(jx_istype(args, JX_ARRAY));

    const char *func = "keys";
    struct jx *out = NULL;

    if (jx_array_length(args) != 1) {
        out = make_error(func, args, "exactly 1 argument required");
        goto FAILURE;
    }

    struct jx* item = jx_array_index(args, 0);
    if (!jx_istype(item, JX_OBJECT)) {
        out = make_error(func, args, "argument must be an object");
        goto FAILURE;
    }

    out = jx_array(NULL);
    const char *key;
    for (void *i = NULL; (key = jx_iterate_keys(item, &i));) {
        struct jx *value = jx_get_value(&i);
        struct jx *t = jx_array(NULL);
        jx_array_insert(t, jx_copy(value));
        jx_array_insert(t, jx_string(key));
        jx_array_insert(out, t);
    }

FAILURE:
    jx_delete(args);
    return out;

}


struct jx *jx_function_fetch(struct jx *args) {
    assert(args);
    if (jx_istype(args, JX_ERROR)) return args;
    const char *func = "fetch";

    struct jx *result = NULL;
    struct jx *val = NULL;

    int length = jx_array_length(args);
    if(length>1){
        result = make_error(func, args, "must pass in one path or one URL");
        goto FAILURE;
    } else if(length<=0){
        result = make_error(func, args, "must pass in a path or URL");
        goto FAILURE;
    }

    val = jx_array_shift(args);

    if (!jx_istype(val, JX_STRING)) {
        result = make_error(func, args, " string argument required");
        goto FAILURE;
    }

    const char *path = val->u.string_value;
    if(string_match_regex(path, "http(s)?://")) {
        //Arbitrary 30 second timeout to perform curl
        char *cmd = string_format("curl -m 30 -s %s", path);
        FILE *stream = popen(cmd,"r");
        free(cmd);
        if(!stream) {
            result = make_error(func, args, "error fetching %s: %s",
                path, strerror(errno));
            goto FAILURE;
        }
        result = jx_parse_stream(stream);
        pclose(stream);
    } else {
        FILE *stream = fopen(path, "r");
        if(!stream) {
            result = make_error(func, args, "error reading %s: %s\n",
                path, strerror(errno));
            goto FAILURE;
        }
        result = jx_parse_stream(stream);
        fclose(stream);
    }

    if(!result) {
        result = make_error(func, args, "error parsing JSON in %s", path);
        goto FAILURE;
    }

FAILURE:
    jx_delete(args);
    jx_delete(val);
    return result;
}

struct jx *jx_function_select(struct jx *args, struct jx *ctx) {
    assert(args);
    assert(jx_istype(args, JX_ARRAY));
    assert(jx_istype(ctx, JX_OBJECT));
    const char *func = "select";

    struct jx *result = NULL;
    struct jx *new_ctx = NULL;
    struct jx *j = NULL;

    struct jx *objlist = jx_array_shift(args);
    struct jx *val = jx_array_shift(args);
    if (jx_array_length(args) != 0) {
        result = make_error(func, args, "2 arguments required");
        goto FAILURE;
    }

    result = jx_eval(objlist, ctx);
    assert(result);
    if (jx_istype(result, JX_ERROR)) {
        goto FAILURE;
    }

    if (!jx_istype(result, JX_ARRAY)) {
        jx_delete(result);
        result = make_error(func, args, "list of objects required");
        goto FAILURE;
    }

    jx_delete(objlist);
    objlist = result;
    result = jx_array(0);

    struct jx *item;
    for(void *i = NULL; (item = jx_iterate_array(objlist, &i));) {
        if (!jx_istype(item, JX_OBJECT)) {
            jx_delete(result);
            result = make_error(func, args, "list of objects required");
            goto FAILURE;
        }
        new_ctx = jx_merge(ctx, item, NULL);
        j = jx_eval(val, new_ctx);
        if (jx_istype(j, JX_ERROR)) {
            jx_delete(result);
            result = j;
            j = NULL;
            goto FAILURE;
        }
        if (!jx_istype(j, JX_BOOLEAN)) {
            jx_delete(result);
            result = make_error(func, args,
                "select expression must use a boolean predicate");
            goto FAILURE;
        }
        if(j->u.boolean_value) jx_array_append(result, jx_copy(item));
        jx_delete(j);
        jx_delete(new_ctx);
        j = NULL;
        new_ctx = NULL;
    }

FAILURE:
    jx_delete(args);
    jx_delete(val);
    jx_delete(objlist);
    jx_delete(new_ctx);
    jx_delete(j);
    return result;
}

struct jx *jx_function_project(struct jx *args, struct jx *ctx) {
    assert(args);
    assert(jx_istype(args, JX_ARRAY));
    assert(jx_istype(ctx, JX_OBJECT));

    const char *func = "project";
    struct jx * result = NULL;
    struct jx *j = NULL;
    struct jx *new_ctx = NULL;

    struct jx *objlist = jx_array_shift(args);
    struct jx *val = jx_array_shift(args);
    if (jx_array_length(args) != 0) {
        result = make_error(func, args, "2 arguments required");
        goto FAILURE;
    }

    result = jx_eval(objlist, ctx);
    assert(result);
    if (jx_istype(result, JX_ERROR)) {
        goto FAILURE;
    }

    if (!jx_istype(result, JX_ARRAY)) {
        jx_delete(result);
        result = make_error(func, args, "list of objects required");
        goto FAILURE;
    }

    jx_delete(objlist);
    objlist = result;
    result = jx_array(0);

    struct jx *item;
    for(void *i = NULL; (item = jx_iterate_array(objlist, &i));) {
        if (!jx_istype(item, JX_OBJECT)) {
            jx_delete(result);
            result = make_error(func, args, "list of objects required");
            goto FAILURE;
        }
        new_ctx = jx_merge(ctx, item, NULL);

        j = jx_eval(val, new_ctx);
        if (jx_istype(j, JX_ERROR)) {
            jx_delete(result);
            result = j;
            j = NULL;
            goto FAILURE;
        }

        jx_array_append(result, j);
        jx_delete(new_ctx);
        new_ctx = NULL;
        j = NULL;
    }

FAILURE:
    jx_delete(args);
    jx_delete(val);
    jx_delete(objlist);
    jx_delete(j);
    jx_delete(new_ctx);
    return result;
}

struct jx *jx_function_schema(struct jx *args) {
    assert(args);
    if (jx_istype(args, JX_ERROR)) return args;
    const char *func = "schema";
    struct jx *result = NULL;

    struct jx *objlist = jx_array_shift(args);
    if (jx_array_length(args) != 0) {
        result = make_error(func, args, "1 argument required");
        goto FAILURE;
    }
    if (!jx_istype(objlist, JX_ARRAY)) {
        result = make_error(func, args, "list of objects required");
        goto FAILURE;
    }

    result = jx_object(0);

    struct jx *item;
    for(void *i = NULL; (item = jx_iterate_array(objlist, &i));) {
        const char *key;
        for(void *j = NULL; (key = jx_iterate_keys(item, &j));) {
            if(!jx_lookup(result, key)) {
                struct jx *lookup = jx_get_value(&j);
                jx_insert(result,
                    jx_string(key),
                    jx_string(jx_type_string(lookup->type)));
            }
        }
    }

FAILURE:
    jx_delete(args);
    jx_delete(objlist);
    return result;
}

struct jx *jx_function_like(struct jx *args) {
    assert(args);
    if (jx_istype(args, JX_ERROR)) return args;
    const char *func = "like";
    struct jx *result = NULL;

    struct jx *obj = jx_array_shift(args);
    struct jx *val = jx_array_shift(args);
    if (!jx_istype(val, JX_STRING)) {
        result = make_error(func, args, "1st argument must be a string");
        goto FAILURE;

    }
    if (!jx_istype(obj, JX_STRING)) {
        result = make_error(func, args, "2nd argument must be a string");
        goto FAILURE;
    }
    if (jx_array_length(args) != 0) {
        result = make_error(func, args, "2 arguments allowed");
        goto FAILURE;
    }


    int match = string_match_regex(obj->u.string_value, val->u.string_value);
    result = jx_boolean(match);

FAILURE:
    jx_delete(args);
    jx_delete(val);
    jx_delete(obj);
    return result;
}

/*vim: set noexpandtab tabstop=8: */
