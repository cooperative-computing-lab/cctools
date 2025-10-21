#include "progress_bar.h"
#include "list.h"
#include "timestamp.h"
#include <unistd.h>
#include <stdio.h>

int main()
{
	uint64_t total = 1000000;
	struct ProgressBarPart *part1 = progress_bar_create_part("step", total);
	struct ProgressBarPart *part2 = progress_bar_create_part("fetch", total);
	struct ProgressBarPart *part3 = progress_bar_create_part("commit", total);

	struct ProgressBar *bar = progress_bar_init("Compute");
	progress_bar_set_update_interval(bar, 0.5);

	progress_bar_bind_part(bar, part1);
	progress_bar_bind_part(bar, part2);
	progress_bar_bind_part(bar, part3);

	timestamp_t start_time = timestamp_get();
	for (uint64_t i = 0; i < total; i++) {
		progress_bar_update_part(bar, part1, 1);
		progress_bar_update_part(bar, part2, 1);
		progress_bar_update_part(bar, part3, 1);
	}

	progress_bar_finish(bar);
	progress_bar_delete(bar);

	timestamp_t end_time = timestamp_get();
	printf("time taken: %" PRIu64 "\n", end_time - start_time);

	return 0;
}
