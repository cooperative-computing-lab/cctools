
/*
 *
 * Author: Vlad Korolev,  <vlad[@]v-lad.org >
 * Modified s3_get and s3_put. Also added s3_check: Nicholas Potteiger
 *
 */


void aws_init ();
void aws_set_key ( char * const str );
void aws_set_keyid ( char * const str );
void aws_set_debug (int d);


void s3_set_bucket ( char * const str );
int s3_get ( FILE * b, char * const file );
int s3_put ( FILE * b, char * const file );
int s3_check ( char * const file );
void s3_set_host ( char * const str );
void s3_set_mime ( char * const str );
void s3_set_acl ( char * const str );
