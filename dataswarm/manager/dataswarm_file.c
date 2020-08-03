
#include "dataswarm_file.h"
#include "helpers.h"

#include <string.h>

int dataswarm_declare_file(struct jx *json){
    
    char *type = NULL;
    int project = 0;
    char *metadata = NULL;
    
    //validate json
    if(validate_json(json, declaration)){
        return 0;
    }

    //get file info from jx struct --> type, project, metadata
    void *i = NULL;
    void *j = NULL;
    const char *key = jx_iterate_keys(json, &j);
    struct jx *value = jx_iterate_values(json, &i);

    while(key != NULL){
        
        if(!strcmp(key, "type")){
            type = value->u.string_value;
        } else if(!strcmp(key, "project")){
            project = value->u.interger_value;
        } else if(!strcmp(key, "metadata")){
            metadata = value->u.string_value;
        }

    }

    //TODO: assign a UUID to the file
    
    //return file UUID (for now just return 1 if successful)
    return 1;

}

struct jx *dataswarm_commit_file(int uuid){

}

struct jx *dataswarm_delete_file(int uuid){

}
