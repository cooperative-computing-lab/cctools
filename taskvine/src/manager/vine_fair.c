/*
Copyright (C) 2023- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "vine_runtime_dir.h"

#include "debug.h"
#include "jx_pretty_print.h"
#include "rmonitor.h"
#include "rmonitor_poll.h"
#include "xxmalloc.h"

void vine_fair_write_workflow_info(struct vine_manager *m)
{
	struct jx *mi = jx_objectv("@id", jx_string("managerInfo"), "@name", jx_string("Manager description"), NULL);

	if (getlogin()) {
		jx_insert_string(mi, "userId", getlogin());
	}

	if (m->name) {
		jx_insert_string(mi, "managerName", m->name);
	}

	if (m->monitor_mode != VINE_MON_DISABLED) {
		rmonitor_measure_process_update_to_peak(m->measured_local_resources, getpid());

		if (!m->measured_local_resources->exit_type) {
			m->measured_local_resources->exit_type = xxstrdup("normal");
		}

		jx_insert(mi,
				jx_string("managerUsedLocalResources"),
				rmsummary_to_json(m->measured_local_resources, 1));
	}

	struct jx *jv = jx_objectv("@id",
			jx_string("http://ccl.cse.nd.edu/software/taskvine"),
			"@type",
			jx_string("ComputerLanguage"),
			"name",
			jx_string("TaskVine"),
			"identifier",
			jx_objectv("@id", jx_string("http://ccl.cse.nd.edu/software/taskvine"), NULL),
			"url",
			jx_objectv("@id", jx_string("http://ccl.cse.nd.edu/software/taskvine"), NULL),
			NULL);

	struct jx *g = jx_arrayv(jv, mi, NULL);
	struct jx *w = jx_objectv("@context", jx_string("https://w3id.org/ro/crate/1.1/context"), "@graph", g, NULL);

	char *workflow = vine_get_runtime_path_log(m, "workflow.json");
	FILE *info_file = fopen(workflow, "w");
	if (info_file) {
		jx_pretty_print_stream(w, info_file);
		fclose(info_file);
	} else {
		warn(D_VINE, "Could not open monitor log file for writing: '%s'\n", workflow);
	}

	free(workflow);
	jx_delete(w);
}
