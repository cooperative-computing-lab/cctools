#include "mesos_task.h"

struct mesos_task *mesos_task_create(int task_id, const char *cmd, \
        const char *extra_input_files, const char *extra_output_files)
{
	struct mesos_task *mt = malloc(sizeof(*mt));
	mt->task_id = task_id;
	mt->task_cmd = xxstrdup(cmd);

	if (extra_input_files != NULL) {
	    mt->task_input_files = text_list_load_str(extra_input_files);
		int i = 0;
        int num_input_files = mt->task_input_files->used_length;
		for(i = 0; i < num_input_files; i++) {
			if ((mt->task_input_files->items)[i][0] != '/') {
				char *path_buf = path_getcwd();
				string_combine(path_buf, "/");
				string_combine(path_buf, (mt->task_input_files->items)[i]);
				(mt->task_input_files->items)[i] = path_buf;
			}
		}	
	} else {
		mt->task_input_files = NULL;
	}

	if (extra_output_files != NULL) {
		mt->task_output_files = text_list_load_str(extra_output_files);
	} else {
		mt->task_output_files = NULL;
	}

	return mt;
}

void mesos_task_delete(struct mesos_task *mt) 
{
	free(mt->task_cmd);
	free(mt->task_input_files);	
	free(mt->task_output_files);
	free(mt);
}
