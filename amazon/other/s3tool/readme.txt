For questions or suggestions, please contact me at cjameshuff@gmail.com.

Requirements:
An Amazon S3 account:
http://aws.amazon.com/s3/

libcurl: http://curl.haxx.se/
libcurlpp: http://curlpp.org/
openssl: http://www.openssl.org/

If you're on Mac OS X, you already have libcurl and openssl. libcurlpp is the C++ wrapper for libcurl.


CHANGELOG:
Version 0.1:
Fixed incorrect usage help call that kept s3tool from compiling.
Added version number
Added genidx command


Installation:

Compiling:
Build the program:
make

Copy the program to its destination, pick whichever you prefer, cd there, and run install:

cp s3tool ~/bin/s3tool
cd ~/bin/

or:

cp s3tool /usr/local/bin/s3tool
cd /usr/local/bin/

and then:

./s3tool install

This will set up symlinks for the various s3 tools. The tool is usable without doing this step, but the commands will have to be accessed as "s3tool ls" instead of "s3ls", "s3tool put" instead of "s3put", etc.

You will also need to set up a credentials file. This is a plain text file with three lines:
key ID
secret key
a name (any name, not used for S3, just to identify the credentials file)

For example (non-working example credentials):
0PN5J17HBGZHT7JJ3X82
uV3F3YluFJax1cknvbcGwgjvx4QpvB+leU8dUj2o
johnsmith

The file must be named .s3credentials, and may be either in the current working directory or in the user directory. If one exists in the current working directory, it will take precedence over the one in the user directory. Alternatively, you could use a credentials file located anywhere, with any name, using the -c flag.


Usage:
General options:
For commands where a bucket is specified, -i will cause the index.html object to be generated or updated.

Commands:
List all buckets:
	s3ls
List contents of bucket or object from bucket:
	s3ls BUCKET_NAME [OBJECT_KEY]
alias s3ls

Upload file to S3:
	s3put BUCKET_NAME OBJECT_KEY [FILE_PATH] [-pPERMISSION] [-tTYPE] -mMETADATA
PERMISSION: a canned ACL:
	private
	public-read
	public-read-write
	authenticated-read
TYPE: a MIME content-type
METADATA: a HTML header and data string, multiple metadata may be specified
"s3wput" can be used as a shortcut for "s3put -ppublic-read"

Get object from S3:
	s3get BUCKET_NAME OBJECT_KEY [FILE_PATH]

Get object metadata:
	s3getmeta BUCKET_NAME OBJECT_KEY

Move S3 object:
	s3mv SRC_BUCKET_NAME SRC_OBJECT_KEY DST_OBJECT_KEY
	s3mv SRC_BUCKET_NAME SRC_OBJECT_KEY DST_BUCKET_NAME DST_OBJECT_KEY

Copy S3 object:
	s3cp SRC_BUCKET_NAME SRC_OBJECT_KEY DST_OBJECT_KEY
	s3cp SRC_BUCKET_NAME SRC_OBJECT_KEY DST_BUCKET_NAME DST_OBJECT_KEY

Remove object:
	s3 rm BUCKET_NAME OBJECT_KEY

Remove bucket:
	s3mkbkt BUCKET_NAME

Remove bucket:
	s3rmbkt BUCKET_NAME

Set access to bucket or object with canned ACL:
	s3setbktacl BUCKET_NAME PERMISSION
	s3setacl BUCKET_NAME OBJECT_KEY PERMISSION
where PERMISSION is a canned ACL:
	private
	public-read
	public-read-write
	authenticated-read

Set access to bucket or object with full ACL:
	s3setbktacl BUCKET_NAME
	s3setacl BUCKET_NAME OBJECT_KEY
	With ACL definition piped to STDIN.

Get ACL for bucket or object:
	s3getacl BUCKET_NAME [OBJECT_KEY]

Generate index.html for bucket (currently all objects are listed regardless of permissions):
	s3genidx BUCKET_NAME
