
/*
 Copyright (C) 2008- The University of Notre Dame
 This software is distributed under the GNU General Public License.
 See the file COPYING for details.
 */

extern "C" {
#include "work_queue.h"
}
#include <stdio.h>
#include <stdlib.h>
#include <cstring>
#include <errno.h>
#include <vector>
#include <signal.h>
#include <termios.h>
#include <unistd.h>
#include "Utils.h"
#include <iostream>


/* MySQL Connector/C++ specific headers */
#include <cppconn/driver.h>
#include <cppconn/connection.h>
#include <cppconn/statement.h>
#include <cppconn/prepared_statement.h>
#include <cppconn/resultset.h>
#include <cppconn/metadata.h>
#include <cppconn/resultset_metadata.h>
#include <cppconn/exception.h>
#include <cppconn/warning.h>

using namespace std;
using namespace sql;

//MySQL Connection Parameters
string server="cvrl-sql.crc.nd.edu";
string schema="workqueue";
string user="workqueue";
string mysqlport="3306";
string password="master";
string name="Athena";

//MySQL Objects
Driver *driver;
Connection *con;
Statement *stmt;
PreparedStatement *complete_stmt;
ResultSet *res;

//Master Parameters
#define LOCAL 1
#define REMOTE 0

string machine="cvrl.cse.nd.edu";
string condor_script="/afs/nd.edu/user25/cbauschk/cctools/bin/condor_submit_workers";
int numlocal=0;
int numremote=0;
bool killqueue=false;
bool create=false;

//WorkQueue Objects
struct work_queue_task *t;
struct work_queue *q;
int port = 9600;

//Debug
bool debug=false;
bool printdebug=false;



void createDatabase(){
    string dbcreation="CREATE SCHEMA `"+schema+"`;";
    string cmcreation="CREATE TABLE `"+schema+"`.`commands` (`command_id` int(11) NOT NULL auto_increment,`username` varchar(45) default NULL,`personal_id` int(11) default NULL,`name` varchar(45) default NULL, `command` mediumtext,`status` enum('Queueing','Available','Processing','Submitted','Completed') default 'Queueing',`stdout` longtext, `env` varchar(256), PRIMARY KEY  (`command_id`),KEY `status_name_idx` (`status`,`name`)) ENGINE=MyISAM AUTO_INCREMENT=1 DEFAULT CHARSET=utf8;";
    string fcreation="CREATE TABLE `"+schema+"`.`files` (`fileid` int(11) NOT NULL auto_increment,`command_id` int(11) NOT NULL,`local_path` varchar(256) default NULL,`remote_path` varchar(256) default NULL,`type` enum('INPUT','OUTPUT') default 'INPUT',`flags` enum('NOCACHE','CACHE','SYMLINK','THIRDGET','THIRDPUT') default 'NOCACHE',PRIMARY KEY  (`fileid`),KEY `command_id_idx` (`command_id`)) ENGINE=MyISAM AUTO_INCREMENT=1 DEFAULT CHARSET=utf8;";


    if(printdebug) println("\tCreating the new database");
    //if(printdebug) println(query);
    if(printdebug) println("\tCreating the Schema");
    stmt -> execute (dbcreation);
    if(printdebug) println("\tCreating the commands table");
    stmt -> execute (cmcreation);
    if(printdebug) println("\tCreating the new files table");
    stmt -> execute (fcreation);



}



int numberOfWorkers(){
    return atoi(exec("/afs/nd.edu/user37/condor/software/bin/condor_q | tail -n 1 | cut -d ' ' -f 1").c_str());
}

int numberOfLocalWorkers(){
    int result=atoi(exec("ps aux | grep work_queue_worker | wc -l").c_str())-1;
    if(result<=0){
        return 0;
    }else{
        return result;
    }
}


void submitWorkers(){


    if(numlocal>0){
        int number=numlocal;

        int curWork=numberOfLocalWorkers();


        if(curWork<number){
        number-=curWork;
        string command="/afs/nd.edu/user25/cbauschk/cctools/bin/work_queue_worker "+machine+" "+intToStr(port)+" &";
        for(int i=0; i<number; i++){
            if(debug){
                if(printdebug) print("\tSubmitted Workers: ");
                if(printdebug) println(command);
            }else{
                if(printdebug) print("\tSubmitted Workers: ");
                if(printdebug) println(command);
                system(command.c_str());
            }

        }

        }

    }

    if(numremote>0){
        int number=numremote;

        int curWork=numberOfWorkers();

        if(curWork<number){
            number-=curWork;
            if(printdebug) println("Launching "+intToStr(number)+" more workers");
            string command=condor_script+" "+machine+" "+intToStr(port)+" "+intToStr(number)+" &";
            if(debug){
                if(printdebug) print("\tSubmitted Workers: ");
                if(printdebug) println(command);
            }else{
                if(printdebug) print("\tSubmitted Workers: ");
                if(printdebug) println(command);
                system(command.c_str());
            }

        }



    }
}



void retrieve_data_and_print (ResultSet *rs, int col) {

	/* retrieve the row count in the result set */
	cout << "\nRetrieved " << rs -> rowsCount() << " row(s)." << endl;

	cout << "\nCommand" << endl;
	cout << "--------" << endl;

	/* fetch the data : retrieve all the rows in the result set */
	while (rs->next()) {

        cout << rs -> getString(col) << endl;

	} // while

	cout << endl;

}

void addJobsToQueue(ResultSet* rs){
    int currentJob=-1;

    if(printdebug) print("\tProcessing Results: ");
    if(printdebug) println((int)(rs -> rowsCount()));
    //While there are still entries in the queue
    while (rs->next()) {

        int type=WORK_QUEUE_INPUT;
        int flags=WORK_QUEUE_CACHE;

        if(stringEquals(rs->getString(9), "OUTPUT")){
            type=WORK_QUEUE_OUTPUT;
        }

        if(stringEquals(rs->getString(10), "NOCACHE")){
            flags=WORK_QUEUE_NOCACHE;
        }



        if(debug){
            if(printdebug) println("*************DEBUGGING:  NO JOBS ACTUALLY SUBMITTED*************");
            if(rs->getInt(1)!=currentJob){
                if(printdebug) println("Submit the old task if there is one");
                if(currentJob!=-1){
                    if(printdebug) println("\tSubmitting Previous Task");
                }


                if(printdebug) println("Create the task");
                if(printdebug) print("\tMaking job for: ");
                if(printdebug) println(rs->getString(4));
                currentJob=(int)(rs->getInt(1));

            }

            if(printdebug) println("Add files to job (We must have already created the job)");
            if(printdebug) print("\t\tAdding file: ");
            if(printdebug) print(rs->getString(6));
            if(printdebug) print(",");
            if(printdebug) print(rs->getString(7));
            if(printdebug) print(",");
            if(printdebug) print(type);
            if(printdebug) print(",");
            if(printdebug) println(flags);

        }else{
            if(printdebug) println("*************Submitting Jobs*************");
            if(printdebug) print("Current Job ID: ");
            if(printdebug) print(currentJob);
            if(printdebug) print(" New Job ID: ");
            if(printdebug) println(rs->getInt(1));
            if(rs->getInt(1)!=currentJob){

                if(currentJob!=-1){
                    if(printdebug) println("Submit the old task");
                    work_queue_submit(q, t);
                }

                // environments/cvrl/env.sh; cmd
                if(printdebug) println("Create the task");
                if (rs->getString(6) != "") {
                    printf("Environment specified, executing %s/env.shi\n", rs->getString(6));
                    char fullcmd[256];
                    sprintf(fullcmd, "bash %s/env.sh; %s", rs->getString(6).c_str(), rs->getString(4).c_str());
                    t = work_queue_task_create(fullcmd);
                }
                else {
                    t = work_queue_task_create(rs->getString(4).c_str());
                }

                if(printdebug) println("Setting the tag");
                work_queue_task_specify_tag(t, intToStr(rs->getInt(1)).c_str());
                currentJob=(int)(rs->getInt(1));

            }

            if(printdebug) println("Add files to job (We must have already created the job)");
            work_queue_task_specify_file(t, rs->getString(7).c_str(), rs->getString(8).c_str(), type, flags);

        }



	}

    if(currentJob!=-1){
        if(printdebug) println("Submit the last task");
        work_queue_submit(q, t);
    }

    if(printdebug) println("\tFinished Adding Jobs to Queue");

}

void initializeConnection(){
    driver = get_driver_instance();

    /* create a database connection using the Driver */
    con = driver -> connect(server, user, password);
    con -> setAutoCommit(1);





    /* Set up update statement so it is ready to go */
    /* create a statement object */
    stmt = con -> createStatement();

    if(create){
        createDatabase();
    }

    /* select appropriate database schema */
    con -> setSchema(schema);


    complete_stmt = con->prepareStatement("update commands set status='Completed', stdout=? where command_id=?");



}

void markCompleted(string id, string stdout){


    complete_stmt->setString(1, stdout);
    complete_stmt->setInt(2, atoi(id.c_str()));
    complete_stmt->executeUpdate();


    //string update="update commands set status='Completed', stdout='"+stdout+"' where command_id="+id;
    if(printdebug) println("Completing task "+id);
    //stmt -> executeUpdate(update);
}

int getJobs(int number, ResultSet* jobinfo){

    //Update server

    string update="update commands set status='Processing',name='"+name+"' where (name='"+name+"' or name is null) and status='Available' limit "+intToStr(number);
    if(printdebug) println("\tGrabbing jobs from queue");
    //if(printdebug) println(update);
    int changed=stmt -> executeUpdate(update);

    //Make query
    string query="select c.command_id as command_id, c.username as username, c.personal_id as personal_id, c.command as command, c.status as status, c.env as env, f.local_path as local_path, f.remote_path as remote_path, f.type as type, f.flags as flags from (select * from commands where status='Processing') c join files f on c.command_id=f.command_id where c.name='"+name+"' order by c.command_id";

    if(printdebug) println("\tFinding currently owned jobs");
    //if(printdebug) println(query);
    res = stmt -> executeQuery (query);

    return changed;

}

void markJobsAsSubmitted(){
    //Update server

    string update="update commands set status='Submitted',name='"+name+"' where name='"+name+"' and status='Processing'";
    if(printdebug) println("\tSubmitting");
    //if(printdebug) println(update);
    stmt -> executeUpdate(update);

}

void markJobsAsAvailable(){
    //Update server

    string update="update commands set status='Available',name='"+name+"' where name='"+name+"' and status='Processing'";
    if(printdebug) println("\tResetting Job Status for Debug");
    stmt -> executeUpdate(update);
}

void initializeWorkQueue(){
    q = work_queue_create(port);

	if(printdebug) printf("\tListening on port %d...\n", port);
}

void deleteWorkQueue(sig_t s){

    system("killall -9 work_queue_worker &");
    system("/afs/nd.edu/user37/condor/software/bin/condor_rm `whoami` &");
    work_queue_delete(q);

    //If jobs havent finished, return them to the queue
    markJobsAsAvailable();

    exit(0);

}

int jobCycle(){
    int number=200;
    if(numlocal+numremote>0){
        number=numlocal+numremote;
    }


    if(printdebug) println("Getting Jobs");
    int jobs=getJobs(number, res);

    if(jobs<=0){
        return jobs;
    }

    if(printdebug) println("Adding jobs to queue");
    addJobsToQueue(res);


    if(printdebug) println("Marking Jobs as submitted");
    markJobsAsSubmitted();

    if(printdebug) println("Clearing Results");
    res=NULL;

    return jobs;

}

void printHelpMessage(bool end){
    printBlockHeader("MyWorkQueue: Help");
    printBlockParameter("-hostname <string>", "WorkQueue Master Server");
    printBlockParameter("-port <int>", "WorkQueue Master Port");
    printBlockParameter("-name <string>", "WorkQueue Master Name");
    printBlockParameter("-condor_script <string>", "Use a different script for launching workers via condor");
    printBlockParameter("-local <int>", "Number of local workers to run");
    printBlockParameter("-remote <int>", "Number of remote workers to run");
    printBlockParameter("-kill", "Kill WorkQueue Master when there are no more jobs in the database");
    printBlockParameter("-mysql_host <string>", "MySQL Server");
    printBlockParameter("-mysql_port <int>", "MySQL Port");
    printBlockParameter("-mysql_schema <string>", "MySQL Schema");
    printBlockParameter("-user <string>", "MySQL Username");
    printBlockParameter("-p || -password", "Prompt for password");
    printBlockParameter("-create", "Create the database");
    printBlockParameter("-v || -verbose", "Print steps");
    printBlockParameter("-h || -help", "Show this help message");
    printBlockFooter();




    if(end){
        exit(0);
    }
}



void processCommandLine(int argc, char *argv[]){
    //machine port server mysqlport



    for(int i=1; i<argc; i++){
        if(stringEquals(argv[i], "-hostname")){
            machine.assign(argv[i+1]);
            i++;
        }else if(stringEquals(argv[i], "-condor_script")){
            condor_script.assign(argv[i+1]);
            i++;
        }else if(stringEquals(argv[i], "-port")){
            port=atoi(argv[i+1]);
            i++;
        }else if(stringEquals(argv[i], "-local")){
            numlocal=atoi(argv[i+1]);
            i++;
        }else if(stringEquals(argv[i], "-remote")){
            numremote=atoi(argv[i+1]);
            i++;
        }else if(stringEquals(argv[i], "-kill")){
            killqueue=true;
        }else if(stringEquals(argv[i], "-mysql_host")){
            server.assign(argv[i+1]);
            i++;
        }else if(stringEquals(argv[i], "-mysql_port")){
            mysqlport=atoi(argv[i+1]);
            i++;
        }else if(stringEquals(argv[i], "-user")|| stringEquals(argv[i], "-u")){
           user.assign(argv[i+1]);
            i++;
        }else if(stringEquals(argv[i], "-p") || stringEquals(argv[i], "-password")){
            password.assign(getPassword());
        }else if(stringEquals(argv[i], "-name")){
            name.assign(argv[i+1]);
            i++;
        }else if(stringEquals(argv[i], "-help")|| stringEquals(argv[i], "-h")){
            printHelpMessage(true);
        }else if(stringEquals(argv[i], "-create")){
            create=true;
        }else if(stringEquals(argv[i], "-v")|| stringEquals(argv[i], "-verbose")){
            printdebug=true;
        }else if(stringEquals(argv[i], "-schema")|| stringEquals(argv[i], "-mysql_schema")){
            schema.assign(argv[i+1]);
            i++;
        }else{
		std::cout << "Unrecognized command: " << argv[i] << std::endl;
		std::cout << "Continue? [y/N] ";
		string option;
		getline(cin, option);
		if(!stringEquals(option, "y")){
			exit(0);

		}

	}
    }
}

int main(int argc, char *argv[])
{
    if(argc<=1){
        printHelpMessage(true);
    }

    processCommandLine(argc, argv);



    if(printdebug) println("Initializing Connection");
    initializeConnection();

    if(create){

        exit(0);
    }

    //Catch signals and shut down work queue
	signal(SIGINT, (void(*)(int))deleteWorkQueue);
    signal(SIGABRT, (void(*)(int))deleteWorkQueue);

    if(printdebug) println("Initializing WorkQueue on port "+intToStr(port));
    initializeWorkQueue();

    if(!q) {
		if(printdebug) printf("couldn't listen on port %d: %s\n", port, strerror(errno));
		return 1;
	}


    if(printdebug) println("Submitting Workers");



    submitWorkers();






    int checkOnWorkers=0;

    while(true){
        int num=0;
        checkOnWorkers++;

        if(checkOnWorkers % 100 ==0){
            checkOnWorkers=0;
            submitWorkers();

        }

        if(debug){
            if(work_queue_hungry(q)){
                if(printdebug) println("Had Room for jobs");
                if(jobCycle()<=0){
                    if(printdebug) println("No Jobs Retrieved");
                    break;
                }else{
                    if(printdebug) println("Retrieved Jobs");
                }
            }
            break;
        }else if(work_queue_hungry(q)){
            if(printdebug) println("Hungry");
            num=jobCycle();
            if(num<=0){
                if(printdebug) println("No Jobs Retrieved");
            }else{
                if(printdebug) println("Retrieved Jobs");
            }

        }else{
            if(printdebug) println("Sated");
        }

        if(!work_queue_empty(q)){
            if(printdebug) println("Full with Jobs");

            struct work_queue_task *waiting = work_queue_wait(q, 5);

            if(waiting) {
                if(printdebug) printf("task complete: %s (return code %d)\n", waiting->output, waiting->return_status);

                markCompleted(waiting->tag, waiting->output);

                work_queue_task_delete(waiting);
            }
        }else if(num<=0){
            if(printdebug) println("No work left to do");
            if(killqueue){
                break;
            }
            sleep(10);
        }


    }




    if(printdebug) println("Deleting WorkQueue");
    deleteWorkQueue(NULL);



	return 0;
}
