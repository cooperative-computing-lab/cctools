package main

import (
	"fmt"
	"log"
	"os"

	ds "work_queue"
)

func main() {
	if len(os.Args) < 2 {
		log.Println("ds_example <file1> [file2] [file3] ...")
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

	q := ds.DataSwarmCreate(9123)
	if q == nil {
		log.Fatal("couldn't listen on port 9123\n")
	}
	log.Println("listening on port 9123...")

	for i := 1; i < len(os.Args); i++ {
		inFile := os.Args[i]
		outFile := fmt.Sprintf("%s.gz", os.Args[i])
		command := fmt.Sprintf("./gzip < %s > %s", inFile, outFile)

		t := ds.DataSwarmTaskCreate(command)
		ds.DataSwarmTaskSpecifyFile(t, gzipPath, "gzip",
			ds.Work_queue_file_type_t(ds.WORKQUEUEINPUT),
			ds.Work_queue_file_flags_t(ds.WORKQUEUECACHE))
		ds.DataSwarmTaskSpecifyFile(t, inFile, inFile,
			ds.Work_queue_file_type_t(ds.WORKQUEUEINPUT),
			ds.Work_queue_file_flags_t(ds.WORKQUEUENOCACHE))
		ds.DataSwarmTaskSpecifyFile(t, outFile, outFile,
			ds.Work_queue_file_type_t(ds.WORKQUEUEINPUT),
			ds.Work_queue_file_flags_t(ds.WORKQUEUENOCACHE))

		taskID := ds.DataSwarmSubmit(q, t)
		log.Printf("submitted task (id# %d)\n", taskID)
	}

	log.Println("waiting for tasks to complete...")
	for ds.DataSwarmEmpty(q) == 0 {
		completeTask := ds.DataSwarmWait(q, 5)
		if completeTask != nil {
			log.Println("a task complete")
			ds.DataSwarmTaskDelete(completeTask)
		}
	}

	log.Println("all tasks complete!")
	ds.DataSwarmDelete(q)
}
