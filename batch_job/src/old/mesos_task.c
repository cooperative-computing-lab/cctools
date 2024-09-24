#include "mesos_task.h"

struct mesos_task *mesos_task_create(int task_id, const char *cmd, 
		const char *extra_input_files, const char *extra_output_files)
{
	struct mesos_task *mt = malloc(sizeof(*mt));
	mt->task_id = task_id;
	mt->task_cmd = xxstrdup(cmd);

	if (extra_input_files != NULL) {
	    mt->task_input_files = text_list_load_str(extra_input_files);
		int i = 0;
        int num_input_files = text_list_size(mt->task_input_files);
		for(i = 0; i < num_input_files; i++) {
			if (text_list_get(mt->task_input_files, i)[0] != '/') {
				char *path_buf = path_getcwd();
				string_combine(path_buf, "/");
				string_combine(path_buf, text_list_get(mt->task_input_files, i));
				text_list_set(mt->task_input_files, path_buf, i);
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

/* vim: set noexpandtab tabstop=8: */
