package work_queue

import (
	"fmt"
	"log"
	"os"
)

func main() {
	if len(os.Args) < 2 {
		log.Println("work_queue_example <file1> [file2] [file3] ...")
		log.Println("Each file given on the command line will be compressed using a remote worker.")
		return
	}

	gzipPath := "/bin/gzip"
	if _, err := os.Stat(gzipPath); os.IsNotExist(err) {
		log.Fatal("gzip was not found. Please modify the gzip_path variable accordingly. To determine the location of gzip, from the terminal type: which gzip (usual locations are /bin/gzip and /usr/bin/gzip)")
	}

	q := Work_queue_create(9123)
	if q == nil {
		log.Fatal("couldn't listen on port 9123\n")
	}
	log.Println("listening on port 9123...")

	for i := 1; i < len(os.Args); i++ {
		inFile := os.Args[i]
		outFile := fmt.Sprintf("%s.gz", os.Args[i])
		command := fmt.Sprintf("./gzip < %s > %s", inFile, outFile)

		t := Work_queue_task_create(command)
		Work_queue_task_specify_file(t, gzipPath, "gzip", Work_queue_file_type_t(WORK_QUEUE_INPUT), Work_queue_file_flags_t(WORK_QUEUE_CACHE))
		Work_queue_task_specify_file(t, inFile, inFile, Work_queue_file_type_t(WORK_QUEUE_INPUT), Work_queue_file_flags_t(WORK_QUEUE_NOCACHE))
		Work_queue_task_specify_file(t, outFile, outFile, Work_queue_file_type_t(WORK_QUEUE_INPUT), Work_queue_file_flags_t(WORK_QUEUE_NOCACHE))

		taskID := Work_queue_submit(q, t)
		log.Printf("submitted task (id# %d)\n", taskID)
	}

	log.Println("waiting for tasks to complete...")
	for Work_queue_empty(q) == 0 {
		completeTask := Work_queue_wait(q, 5)
		if completeTask != nil {
			log.Println("a task complete")
			Work_queue_task_delete(completeTask)
		}
	}

	log.Println("all tasks complete!")
	Work_queue_delete(q)
}
