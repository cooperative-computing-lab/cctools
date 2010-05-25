#!/usr/bin/perl

open S3PWD, ">s3passwd.h";
print S3PWD
"#ifndef S3PASSWD_H_
#define S3PASSWD_H_

// WARNING: THESE CREDENTIALS ARE NON-FUNCTIONAL!  REPLACE WITH YOUR OWN BEFORE USE!
char userid[] = \"0PN5J17HBGZHT7JJ3X82\";
char key[] = \"uV3F3YluFJax1cknvbcGwgjvx4QpvB+leU8dUj2o\";

#endif
";

close S3PWD;
