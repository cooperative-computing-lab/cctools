# load work_queue module 
par<-normalizePath("../")
dyn.load(paste(par, "/work_queue", .Platform$dynlib.ext, sep=""))
source("../work_queue.R")
cacheMetaData(1)

# util functions 
isNullTask <- function(pointer) {
    emptytask <- new("_p_work_queue_task")
    isTRUE(all.equal(pointer, emptytask))
}

printf <- function(...) {
    invisible(cat(sprintf(...)))
}

exitInErr <- function(err_msg) {
    printf("%s\n", err_msg)
    quit(save="no", status=1, runLast=FALSE)
}

main <- function() {
    # check command args
    args <- commandArgs(trailingOnly = TRUE)
    if (length(args) <= 0) {
        exitInErr("please specify input files")
    }
    
    gzip_path <- "/bin/gzip"
    if (!file.exists(gzip_path)) {
        gzip_path <- "/usr/bin/gzip"
        if (!file.exists(gzip_path)) {
            exitInErr("gzip was not found. Please modify the gzip_path variable 
                      accordingly. To determine the location of gzip, from the 
                      terminal type: which gzip (usual locations are /bin/gzip 
                                                 and /usr/bin/gzip)")
        }
    }
    
    # the default port of work queue master
    default_port <- 9123
    
    # create a work_queue instance
    wq <- work_queue_create(default_port)
    for (t_inp in args) {
        t_oup <- paste(t_inp, ".gz", sep="")
        t_cmd <- paste("./gzip <", t_inp, ">", t_oup, sep=" ")
        # reate task
        t <- work_queue_task_create(t_cmd)
        # specify task's files
        work_queue_task_specify_input_file(t, t_inp, t_inp)
        work_queue_task_specify_output_file(t, t_oup, t_oup)
        # submit the task
        t_id <- work_queue_submit(wq, t)
        printf("submitted task (id# %d): %s\n", t_id, t_cmd)
    }
    
    # wait for task to complete
    while(!work_queue_empty(wq)) {
        # Application specific code goes here ...
    
        # work_queue_wait waits at most 5 seconds for some task to return.
        complete_t <- work_queue_wait(wq, 5)
         
        if (!isNullTask(complete_t)) {
            complete_t_id <- work_queue_task_taskid_get(complete_t)
            complete_t_cmd <- work_queue_task_command_line_get(complete_t)
            complete_t_ret_status <- work_queue_task_return_status_get(complete_t)
            printf("task (id# %d) complete: %s (return code %d)\n", 
                complete_t_id, complete_t_cmd, complete_t_ret_status)
            if(complete_t_ret_status != 0) {
                #  The task failed.Error handling (e.g.,resubmit with new parameters) here. 
            }
            # delete complete task from work queue
            work_queue_task_delete(complete_t) 
        }
    }
    
    printf("all tasks complete!\n")
    # delete work queue
    invisible(work_queue_delete(wq))
}

if(!interactive()) {
    main()
}
