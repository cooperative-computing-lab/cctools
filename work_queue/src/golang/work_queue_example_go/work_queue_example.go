package main

import (
	"fmt"
	"log"
	"os"

	wq "work_queue"
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

	q := wq.WorkQueueCreate(9123)
	if q == nil {
		log.Fatal("couldn't listen on port 9123\n")
	}
	log.Println("listening on port 9123...")

	for i := 1; i < len(os.Args); i++ {
		inFile := os.Args[i]
		outFile := fmt.Sprintf("%s.gz", os.Args[i])
		command := fmt.Sprintf("./gzip < %s > %s", inFile, outFile)

		t := wq.WorkQueueTaskCreate(command)
		wq.WorkQueueTaskSpecifyFile(t, gzipPath, "gzip",
			wq.Work_queue_file_type_t(wq.WORKQUEUEINPUT),
			wq.Work_queue_file_flags_t(wq.WORKQUEUECACHE))
		wq.WorkQueueTaskSpecifyFile(t, inFile, inFile,
			wq.Work_queue_file_type_t(wq.WORKQUEUEINPUT),
			wq.Work_queue_file_flags_t(wq.WORKQUEUENOCACHE))
		wq.WorkQueueTaskSpecifyFile(t, outFile, outFile,
			wq.Work_queue_file_type_t(wq.WORKQUEUEINPUT),
			wq.Work_queue_file_flags_t(wq.WORKQUEUENOCACHE))

		taskID := wq.WorkQueueSubmit(q, t)
		log.Printf("submitted task (id# %d)\n", taskID)
	}

	log.Println("waiting for tasks to complete...")
	for wq.WorkQueueEmpty(q) == 0 {
		completeTask := wq.WorkQueueWait(q, 5)
		if completeTask != nil {
			log.Println("a task complete")
			wq.WorkQueueTaskDelete(completeTask)
		}
	}

	log.Println("all tasks complete!")
	wq.WorkQueueDelete(q)
}
