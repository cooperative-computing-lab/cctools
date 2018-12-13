package work_queue

import (
	"fmt"
	"log"
	"os"
)

func main() {
	if len(os.Args) < 2 {
		log.Println("work_queue_example <file1> [file2] [file3] ...")
		log.Println(`Each file given on the command line will be compressed 
using a remote worker.`)
		return
	}

	gzipPath := "/bin/gzip"
	if _, err := os.Stat(gzipPath); os.IsNotExist(err) {
		log.Fatal(`gzip was not found. Please modify the gzip_path variable 
accordingly. To determine the location of gzip, from the terminal type: 
which gzip (usual locations are /bin/gzip and /usr/bin/gzip)`)
	}

	q := WorkQueueCreate(9123)
	if q == nil {
		log.Fatal("couldn't listen on port 9123\n")
	}
	log.Println("listening on port 9123...")

	for i := 1; i < len(os.Args); i++ {
		inFile := os.Args[i]
		outFile := fmt.Sprintf("%s.gz", os.Args[i])
		command := fmt.Sprintf("./gzip < %s > %s", inFile, outFile)

		t := WorkQueueTaskCreate(command)
		WorkQueueTaskSpecifyFile(t, gzipPath, "gzip",
			Work_queue_file_type_t(WORKQUEUEINPUT),
			Work_queue_file_flags_t(WORKQUEUECACHE))
		WorkQueueTaskSpecifyFile(t, inFile, inFile,
			Work_queue_file_type_t(WORKQUEUEINPUT),
			Work_queue_file_flags_t(WORKQUEUENOCACHE))
		WorkQueueTaskSpecifyFile(t, outFile, outFile,
			Work_queue_file_type_t(WORKQUEUEINPUT),
			Work_queue_file_flags_t(WORKQUEUENOCACHE))

		taskID := WorkQueueSubmit(q, t)
		log.Printf("submitted task (id# %d)\n", taskID)
	}

	log.Println("waiting for tasks to complete...")
	for WorkQueueEmpty(q) == 0 {
		completeTask := WorkQueueWait(q, 5)
		if completeTask != nil {
			log.Println("a task complete")
			WorkQueueTaskDelete(completeTask)
		}
	}

	log.Println("all tasks complete!")
	WorkQueueDelete(q)
}
