#include "batch_job_internal.h"
#include "process.h"
#include "batch_job.h"
#include "debug.h"

#include <stdio.h>
#include <string.h>
#include <unistd.h>

char *amazon_ec2_script ="\
#!/bin/sh\n\
# Runs makeflow directions on ec2 instance\n\
#\n\
# Invocation:\n\
# $ ./amazon_ec2.sh $AWS_ACCESS_KEY $AWS_SECRET_KEY\n\
set -e\n\
#OUTPUT_FILES_DESTINATION=\"/tmp/test_amazon_makeflow\"\n\
OUTPUT_FILES_DESTINATION=\".\"\n\
EC2_TOOLS_DIR=\"$EC2_HOME/bin\"\n\
INSTANCE_TYPE=\"t1.micro\"\n\
USERNAME=\"ubuntu\"\n\
KEYPAIR_NAME=\"$(uuidgen)\"\n\
SECURITY_GROUP_NAME=\"$(uuidgen)\"\n\
\n\
# Flags\n\
INSTANCE_CREATED=0\n\
\n\
cleanup () {\n\
    if [ $INSTANCE_CREATED -eq 1 ]\n\
    then\n\
        echo \"Terminating EC2 instance...\"\n\
        $EC2_TOOLS_DIR/ec2-terminate-instances $INSTANCE_ID\n\
\n\
        # Instance must be shut down in order to delete keypair\n\
        echo \"Waiting for EC2 instance to shutdown...\"\n\
        INSTANCE_SHUTTING_DOWN=1\n\
        while [ $INSTANCE_SHUTTING_DOWN -eq 1 ]\n\
        do\n\
            $EC2_TOOLS_DIR/ec2-describe-instances | \\\n\
                grep \"shutting-down\" | grep -v \"RESERVATION\" \\\n\
                >/dev/null || INSTANCE_SHUTTING_DOWN=0\n\
        done\n\
    fi\n\
\n\
\n\
    echo \"Deleting temporary security group...\"\n\
    $EC2_TOOLS_DIR/ec2-delete-group $SECURITY_GROUP_NAME > /dev/null\n\
    echo \"Temporary security group deleted.\"\n\
\n\
    echo \"Deleting temporary keypair...\"\n\
    $EC2_TOOLS_DIR/ec2-delete-keypair $KEYPAIR_NAME > /dev/null\n\
    rm -f $KEYPAIR_NAME.pem\n\
    echo \"Temporary keypair deleted.\"\n\
}\n\
\n\
run_ssh_cmd () {\n\
    ssh -o StrictHostKeyChecking=no -i $KEYPAIR_NAME.pem $USERNAME@$PUBLIC_DNS \\\n\
            $1\n\
}\n\
\n\
get_file_from_server_to_destination () {\n\
    echo \"Copying file to $2\"\n\
    scp -o StrictHostKeyChecking=no -i $KEYPAIR_NAME.pem \\\n\
        $USERNAME@$PUBLIC_DNS:~/\"$1\" $2\n\
}\n\
\n\
copy_file_to_server () {\n\
    scp -o StrictHostKeyChecking=no -i $KEYPAIR_NAME.pem \\\n\
        $* $USERNAME@$PUBLIC_DNS:~\n\
}\n\
\n\
generate_temp_keypair () {\n\
    # Generate temp key pair and save\n\
    echo \"Generating temporary keypair...\"\n\
    $EC2_TOOLS_DIR/ec2-create-keypair $KEYPAIR_NAME | sed \'s/.*KEYPAIR.*//\' > $KEYPAIR_NAME.pem\n\
    echo \"Keypair generated.\"\n\
}\n\
\n\
create_temp_security_group () {\n\
    # Create temp security group\n\
    echo \"Generating temporary security group...\"\n\
    $EC2_TOOLS_DIR/ec2-create-group $SECURITY_GROUP_NAME -d \"$SECURITY_GROUP_NAME\"\n\
    echo \"Security group generated.\"\n\
}\n\
\n\
authorize_port_22_for_ssh_access () {\n\
    echo \"Authorizing port 22 on instance for SSH access...\"\n\
    $EC2_TOOLS_DIR/ec2-authorize $SECURITY_GROUP_NAME -p 22\n\
}\n\
\n\
trap cleanup EXIT\n\
\n\
if [ \"$#\" -lt 3 ]; then\n\
    echo \"Incorrect arguments passed to program\"\n\
    echo \"Usage: $0 AWS_ACCESS_KEY AWS_SECRET_KEY INPUT_FILES OUTPUT_FILES\" >&2\n\
    exit 1\n\
fi\n\
\n\
# No inputs passed\n\
if [ \"$#\" -eq 4 ]; then\n\
    export AWS_ACCESS_KEY=$1\n\
    export AWS_SECRET_KEY=$2\n\
    CMD=$3\n\
    AMI_IMAGE=$4\n\
    INPUT_FILES=\"\"\n\
    OUTPUT_FILES=$5\n\
else\n\
    export AWS_ACCESS_KEY=$1\n\
    export AWS_SECRET_KEY=$2\n\
    CMD=$3\n\
    AMI_IMAGE=$4\n\
    INPUT_FILES=$5\n\
    OUTPUT_FILES=$6\n\
fi\n\
\n\
\n\
generate_temp_keypair\n\
create_temp_security_group\n\
authorize_port_22_for_ssh_access\n\
\n\
echo \"Starting EC2 instance...\"\n\
INSTANCE_ID=$($EC2_TOOLS_DIR/ec2-run-instances \\\n\
    $AMI_IMAGE \\\n\
    -t $INSTANCE_TYPE \\\n\
    -k $KEYPAIR_NAME \\\n\
    -g $SECURITY_GROUP_NAME \\\n\
    | grep \"INSTANCE\" | awk \'{print $2}\')\n\
INSTANCE_CREATED=1\n\
\n\
INSTANCE_STATUS=\"pending\"\n\
while [ \"$INSTANCE_STATUS\" = \"pending\" ]; do\n\
    INSTANCE_STATUS=$($EC2_TOOLS_DIR/ec2-describe-instances $INSTANCE_ID \\\n\
    | grep \"INSTANCE\" | awk \'{print $5}\')\n\
done\n\
\n\
PUBLIC_DNS=$($EC2_TOOLS_DIR/ec2-describe-instances $INSTANCE_ID \\\n\
| grep \"INSTANCE\" | awk \'{print $4\'})\n\
\n\
chmod 400 $KEYPAIR_NAME.pem\n\
\n\
# Try for successful ssh connection\n\
tries=30\n\
SUCCESSFUL_SSH=-1\n\
while [ $tries -ne 0 ]\n\
do\n\
    run_ssh_cmd \"echo \'Connection to remote server successful\'\" \\\n\
        && SUCCESSFUL_SSH=0 && break\n\
    tries=$(expr $tries-1)\n\
    sleep 1\n\
done\n\
\n\
# Run rest of ssh commands\n\
if [ $SUCCESSFUL_SSH -eq 0 ]\n\
then\n\
    # Pass input files\n\
    if ! [ -z \"$INPUT_FILES\" ]; then\n\
        INPUTS=\"$(echo $INPUT_FILES | sed \'s/,/ /g\')\"\n\
        copy_file_to_server $INPUTS\n\
    fi\n\
\n\
    # Run command\n\
    run_ssh_cmd \"$CMD\"\n\
\n\
    # Get output files\n\
    OUTPUTS=\"$(echo $OUTPUT_FILES | sed \'s/,/ /g\')\"\n\
    get_file_from_server_to_destination $OUTPUTS $OUTPUT_FILES_DESTINATION\n\
fi\n\
";

char *amazon_script_filename = "_temp_amazon_ec2_script.sh";

static batch_job_id_t batch_job_amazon_submit (struct batch_queue *q, const char *cmd, const char *extra_input_files, const char *extra_output_files, struct nvpair *envlist )
{
    int jobid;
    struct batch_job_info *info = malloc(sizeof(*info));
    memset(info, 0, sizeof(*info));

    char *amazon_credentials_filepath = hash_table_lookup(
        q->options,
        "amazon-credentials-filepath"
    );
    if (amazon_credentials_filepath == NULL) {
        fatal("No amazon credentials passed. Please pass file containing amazon credentials using --amazon-credentials-filepath flag");
    }
    char *ami_image_id = hash_table_lookup(
        q->options,
        "ami-image-id"
    );
    if (ami_image_id == NULL) {
        fatal("No ami image id passed. Please pass file containing ami image id using --ami-image-id flag");
    }


    // Parse credentials file
    /* Credentials file format
    [Credentials]
    aws_access_key_id = supersecretkey
    aws_secret_access_key = supersecretkey
    */
    FILE *credentials_file = fopen(amazon_credentials_filepath, "r");
    char first_line[200];
    char aws_access_key_id[200];
    char aws_secret_access_key[200];
    // Ignore first line
    if (credentials_file == NULL) {
        fatal("Amazon credentials file could not be opened");
    }
    fscanf(credentials_file, "%s", first_line);
    if (strcmp("[Credentials]", first_line) != 0) {
        fatal("Credentials file not in the correct format");
    }
    fscanf(credentials_file, "%s %s %s", aws_access_key_id, aws_access_key_id, aws_access_key_id);
    fscanf(credentials_file, "%s %s %s", aws_secret_access_key, aws_secret_access_key, aws_secret_access_key);

    // Write amazon ec2 script to file
    FILE *f = fopen(amazon_script_filename, "w");
    fprintf(f, amazon_ec2_script);
    fclose(f);
    // Execute permissions
    char mode[] = "0755";
    int i = strtol(mode, 0, 8);
    chmod(amazon_script_filename, i);


    // Run ec2 script
    char shell_cmd[200];
    sprintf(
        shell_cmd,
        "./%s %s %s '%s' %s %s %s",
        amazon_script_filename,
        aws_access_key_id,
        aws_secret_access_key,
        cmd,
        ami_image_id,
        extra_input_files,
        extra_output_files
    );
    debug(D_BATCH, "Forking EC2 script process...");
    // Fork process and spin off shell script
    jobid = fork();
    if (jobid > 0) // parent
    {
        info->submitted = time(0);
        info->started = time(0);
        itable_insert(q->job_table, jobid, info);
        return jobid;
    }
    else { // child
	    execlp(
            "sh",
            "sh",
            "-c",
            shell_cmd,
            (char *) 0
	    );
    }
}

static batch_job_id_t batch_job_amazon_wait (struct batch_queue * q, struct batch_job_info * info_out, time_t stoptime)
{
    while (1) {
        int timeout = 5;
        struct process_info *p = process_wait(timeout);
		if(p) {
			struct batch_job_info *info = itable_remove(q->job_table, p->pid);
			if(!info) {
				process_putback(p);
				return -1;
			}

			info->finished = time(0);
			if(WIFEXITED(p->status)) {
				info->exited_normally = 1;
				info->exit_code = WEXITSTATUS(p->status);
			} else {
				info->exited_normally = 0;
				info->exit_signal = WTERMSIG(p->status);
			}

			memcpy(info_out, info, sizeof(*info));

			int jobid = p->pid;
			free(p);
			free(info);
			return jobid;
		}
    }
}

static int batch_job_amazon_remove (struct batch_queue *q, batch_job_id_t jobid)
{
    struct batch_job_info *info =  itable_lookup(q->job_table, jobid);
    printf("Job started at: %d\n", (int)info->started);
	info->finished = time(0);
	info->exited_normally = 0;
	info->exit_signal = 0;
    return 0;
}


batch_queue_stub_create(amazon);
batch_queue_stub_free(amazon);
batch_queue_stub_port(amazon);
batch_queue_stub_option_update(amazon);

batch_fs_stub_chdir(amazon);
batch_fs_stub_getcwd(amazon);
batch_fs_stub_mkdir(amazon);
batch_fs_stub_putfile(amazon);
batch_fs_stub_stat(amazon);
batch_fs_stub_unlink(amazon);

const struct batch_queue_module batch_queue_amazon = {
	BATCH_QUEUE_TYPE_AMAZON,
	"amazon",

	batch_queue_amazon_create,
	batch_queue_amazon_free,
	batch_queue_amazon_port,
	batch_queue_amazon_option_update,

	{
		batch_job_amazon_submit,
		batch_job_amazon_wait,
		batch_job_amazon_remove,
	},

	{
		batch_fs_amazon_chdir,
		batch_fs_amazon_getcwd,
		batch_fs_amazon_mkdir,
		batch_fs_amazon_putfile,
		batch_fs_amazon_stat,
		batch_fs_amazon_unlink,
	},
};
