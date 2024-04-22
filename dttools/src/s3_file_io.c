/*
 *
 * Author: Vlad Korolev,  <vlad[@]v-lad.org >
 * Modified s3_get and s3_put. Also added s3_check: Nicholas Potteiger
 *
*/

/*!
  \mainpage

  This is a small library that provides Amazon Web Services binding
  for C programs.  
  
  The s3_file_io leverages CURL and OPENSSL libraries for HTTP transfer and 
  cryptographic functions.

  The \ref todo list is here.

  The \ref bug list is here.

*/

#include <stdlib.h>
#include <stdio.h>
#include <stdarg.h>
#include <string.h>
#include <unistd.h>
#include <sys/stat.h>
#include <curl/curl.h>
#include <openssl/hmac.h>
#include <openssl/evp.h>
#include <openssl/bio.h>
#include <openssl/buffer.h>

#include "s3_file_io.h"


/*!
  \defgroup internal Internal Functions
  \{
*/

static int debug = 0;   /// <flag to control debugging options
static int useRrs = 0;  /// <Use reduced redundancy storage
static char * awsKeyID = NULL;  /// <AWS Key ID
static char * awsKey   = NULL;  /// <AWS Key Material
static char * S3Host     = "s3.amazonaws.com";     /// <AWS S3 host
static char * Bucket   = NULL;
static char * MimeType = NULL;
static char * AccessControl = NULL;

static void __debug ( char *fmt, ... ) ;
static char * __aws_get_httpdate ();
static int s3_do_get ( FILE *b, char * const signature, 
			  char * const date, char * const resource );
static int s3_do_put ( FILE *b, char * const signature, 
			  char * const date, char * const resource );
static int s3_do_check ( char * const signature,
                          char * const date, char * const resource );
static char* __aws_sign ( char * const str );


/// Encode a binary into base64 buffer
/// \param input binary data  text
/// \param length length of the input text
/// \internal
/// \return a newly allocated buffer with base64 encoded data 
static char *__b64_encode(const unsigned char *input, int length)
{
  BIO *bmem, *b64;
  BUF_MEM *bptr;

  b64 = BIO_new(BIO_f_base64());
  bmem = BIO_new(BIO_s_mem());
  b64 = BIO_push(b64, bmem);
  BIO_write(b64, input, length);
  if(BIO_flush(b64)) {} /* make gcc 4.1.2 happy */
  BIO_get_mem_ptr(b64, &bptr);

  char *buff = (char *)malloc(bptr->length);
  memcpy(buff, bptr->data, bptr->length-1);
  buff[bptr->length-1] = 0;

  BIO_free_all(b64);

  return buff;
}

/// Handles reception of the data
/// \param ptr pointer to the incoming data
/// \param size size of the data member
/// \param nmemb number of data memebers
/// \param stream pointer to I/O buffer
/// \return number of bytes processed
static size_t writefunc ( void * ptr, size_t size, size_t nmemb, void * stream )
{
    size_t written = fwrite(ptr, size, nmemb, stream);
    return written;
}

/// Print debug output
/// \internal
/// \param fmt printf like formating string
static void __debug ( char *fmt, ... ) {
  /// If debug flag is not set we won't print anything
  if ( ! debug ) return ;
  /// Otherwise process the arguments and print the result
  va_list args;
  va_start( args, fmt );
  fprintf( stderr, "DBG: " );
  vfprintf( stderr, fmt, args );
  fprintf( stderr, "\n" );
  va_end( args );
}


/// Get Request Date
/// \internal
/// \return date in HTTP format
static char * __aws_get_httpdate ()
{
  static char dTa[256];
  time_t t = time(NULL);
  struct tm * gTime = gmtime ( & t );
  memset ( dTa, 0 , sizeof(dTa));
  strftime ( dTa, sizeof(dTa), "%a, %d %b %Y %H:%M:%S +0000", gTime );
  __debug ( "Request Time: %s", dTa );
  return dTa;
}

/// Get S3 Request signature
/// \internal
/// \param resource -- URI of the object
/// \param resSize --  size of the resoruce buffer
/// \param date -- HTTP date
/// \param method -- HTTP method
/// \param bucket -- bucket 
/// \param file --  file
/// \return fills up resource and date parameters, also 
///         returns request signature to be used with Authorization header
static char * GetStringToSign ( char * resource,  int resSize, 
			     char ** date,
			     char * const method,
			     char * const bucket,
			     char * const file )
{
  char  reqToSign[2048];
  char  acl[32];
  char  rrs[64];

  /// \todo Change the way RRS is handled.  Need to pass it in
  
  * date = __aws_get_httpdate();

  memset ( resource,0,resSize);
  if ( bucket != NULL )
    snprintf ( resource, resSize,"%s/%s", bucket, file );
  else
    snprintf ( resource, resSize,"%s", file );

  if (AccessControl)
    snprintf( acl, sizeof(acl), "x-amz-acl:%s\n", AccessControl);
  else
    acl[0] = 0;

  if (useRrs)
    strncpy( rrs, "x-amz-storage-class:REDUCED_REDUNDANCY\n", sizeof(rrs));  
  else
    rrs[0] = 0;


  snprintf ( reqToSign, sizeof(reqToSign),"%s\n\n%s\n%s\n%s%s/%s",
	     method,
	     MimeType ? MimeType : "",
	     *date,
	     acl,
	     rrs,
	     resource );

  // EU: If bucket is in virtual host name, remove bucket from path
  if (bucket && strncmp(S3Host, bucket, strlen(bucket)) == 0)
    snprintf ( resource, resSize,"%s", file );

  return __aws_sign(reqToSign);
}

/*!
  \defgroup conf Configuration Functions
  \{
*/

/// Initialize  the library 
void aws_init () { curl_global_init (CURL_GLOBAL_ALL); }

/// Set debuging output
/// \param d  when non-zero causes debugging output to be printed
void aws_set_debug (int d)
{
  debug = d;
}

/// Set AWS account access key
/// \param key new AWS authentication key
void aws_set_key ( char * const key )   
{ awsKey = key == NULL ? NULL : strdup(key); }

/// Set AWS account access key ID
/// \param keyid new AWS key ID
void aws_set_keyid ( char * const keyid ) 
{ awsKeyID = keyid == NULL ? NULL :  strdup(keyid);}

/*!
  \defgroup s3 S3 Interface Functions
  \{
*/


/// Select current S3 bucket
/// \param str bucket ID
void s3_set_bucket ( char * const str ) 
{ Bucket = str == NULL ? NULL : strdup(str); }

/// Set S3 host
void s3_set_host ( char * const str )  
{ S3Host = str == NULL ? NULL :  strdup(str); }

/// Set S3 MimeType
void s3_set_mime ( char * const str )
{ MimeType = str ? strdup(str) : NULL; }

/// Set S3 AccessControl
void s3_set_acl ( char * const str )
{ AccessControl = str ? strdup(str) : NULL; }


/// Upload the file into currently selected bucket
/// \param FILE b
/// \param file filename (can be renamed to a different name in s3 bucket this way)
// file needs to be open before using fopen with param 'rb'
// Ex. FILE *fp = fopen("test.txt","rb");
//     s3_put(fp,"test.txt"); 
int s3_put ( FILE * b, char * const file )
{
  char * const method = "PUT";
  char  resource [1024];
  char * date = NULL;

  char * signature = GetStringToSign ( resource, sizeof(resource), 
				       &date, method, Bucket, file ); 
  int sc = s3_do_put( b, signature, date, resource ); 
  free ( signature );
  return sc;

}


/// Download the file from the current bucket
/// \param FILE b
/// \param file filename 
// file (can be any file name of choice) needs to be open before using fopen with param 'wb'
// Ex. FILE *fp = fopen("test.txt","wb");
//     s3_get(fp,"test.txt);
int s3_get ( FILE * b, char * const file )
{
  char * const method = "GET";
  
  char  resource [1024];
  char * date = NULL;

  
  char * signature = GetStringToSign ( resource, sizeof(resource), 
				       &date, method, Bucket, file ); 
  int sc = s3_do_get( b, signature, date, resource ); 
  free ( signature );
  return sc;
}

///Checks to see if file exists in S3 bucket
/// \param file filename
int s3_check ( char * const file )
{
  char * const method = "HEAD";
  
  char  resource [1024];
  char * date = NULL;
  char * signature = GetStringToSign ( resource, sizeof(resource), 
				       &date, method, Bucket, file );
  int sc = s3_do_check( signature, date, resource ); 
  free ( signature );

  return sc;

}



static int s3_do_put ( FILE *b, char * const signature, 
		       char * const date, char * const resource )
{
  char Buf[1024];
  struct stat file_info;
  CURL* ch =  curl_easy_init( );
  struct curl_slist *slist=NULL;
  
  if(fstat(fileno(b), &file_info) != 0)
    return 1; /* can't continue */ 
  
  if (MimeType) {
    snprintf ( Buf, sizeof(Buf), "Content-Type: %s", MimeType );
    slist = curl_slist_append(slist, Buf );
  }

  if (AccessControl) {
    snprintf ( Buf, sizeof(Buf), "x-amz-acl: %s", AccessControl );
    slist = curl_slist_append(slist, Buf );
  }

  if (useRrs) {
    strncpy ( Buf, "x-amz-storage-class: REDUCED_REDUNDANCY", sizeof(Buf) );
    slist = curl_slist_append(slist, Buf );  }



  snprintf ( Buf, sizeof(Buf), "Date: %s", date );
  slist = curl_slist_append(slist, Buf );
  snprintf ( Buf, sizeof(Buf), "Authorization: AWS %s:%s", awsKeyID, signature );
  slist = curl_slist_append(slist, Buf );

  snprintf ( Buf, sizeof(Buf), "http://%s/%s", S3Host , resource );
  curl_easy_setopt ( ch, CURLOPT_HTTPHEADER, slist);
  curl_easy_setopt ( ch, CURLOPT_URL, Buf );
  curl_easy_setopt ( ch, CURLOPT_READDATA, b );
  curl_easy_setopt ( ch, CURLOPT_UPLOAD, 1L );
  curl_easy_setopt ( ch, CURLOPT_INFILESIZE,(curl_off_t)file_info.st_size);
  //curl_easy_setopt ( ch, CURLOPT_VERBOSE, 1L );
  //curl_easy_setopt ( ch, CURLOPT_FOLLOWLOCATION, 1 );

  int  sc  = curl_easy_perform(ch);
  /** \todo check the return code  */
  __debug ( "Return Code: %d ", sc );
  
  curl_slist_free_all(slist);
  curl_easy_cleanup(ch);

  return sc;

}


static int s3_do_get ( FILE *b, char * const signature, 
		       char * const date, char * const resource )
{
  char Buf[1024];

  CURL* ch =  curl_easy_init( );
  struct curl_slist *slist=NULL;
  snprintf ( Buf, sizeof(Buf), "Date: %s", date );
  slist = curl_slist_append(slist, Buf );
  snprintf ( Buf, sizeof(Buf), "Authorization: AWS %s:%s", awsKeyID, signature );
  slist = curl_slist_append(slist, Buf );

  snprintf ( Buf, sizeof(Buf), "http://%s/%s", S3Host, resource );
  curl_easy_setopt ( ch, CURLOPT_HTTPHEADER, slist);
  curl_easy_setopt ( ch, CURLOPT_URL, Buf );
  curl_easy_setopt ( ch, CURLOPT_WRITEFUNCTION, writefunc );
  curl_easy_setopt ( ch, CURLOPT_WRITEDATA, b );

  int  sc  = curl_easy_perform(ch);
  long response_code;
  curl_easy_getinfo(ch, CURLINFO_RESPONSE_CODE, &response_code);
  /** \todo check the return code  */
  __debug ( "Return Code: %d ", sc );
  
  curl_slist_free_all(slist);
  curl_easy_cleanup(ch);

  if(response_code == 200){
        printf("FILE EXISTS\n");
        return 0;
  }
  else{
        printf("FILE DOES NOT EXIST\n");
        return 1;
  }

}

static int s3_do_check ( char * const signature, 
		       char * const date, char * const resource )
{
  char Buf[1024];

  CURL* ch =  curl_easy_init( );
  struct curl_slist *slist=NULL;
 
  snprintf ( Buf, sizeof(Buf), "Date: %s", date );
  slist = curl_slist_append(slist, Buf );
  snprintf ( Buf, sizeof(Buf), "Authorization: AWS %s:%s", awsKeyID, signature );
  slist = curl_slist_append(slist, Buf ); 

  snprintf ( Buf, sizeof(Buf), "http://%s/%s", S3Host, resource );
  curl_easy_setopt ( ch, CURLOPT_HTTPHEADER, slist);
  curl_easy_setopt ( ch, CURLOPT_URL, Buf );
  curl_easy_setopt(ch, CURLOPT_NOBODY, 1);

  CURLcode  sc  = curl_easy_perform(ch);
  /** \todo check the return code  */
  __debug ( "Return Code: %d ", sc );
  long response_code;
  curl_easy_getinfo(ch, CURLINFO_RESPONSE_CODE, &response_code);
  curl_slist_free_all(slist);
  curl_easy_cleanup(ch);
  if(response_code == 200){
	printf("FILE EXISTS\n");
	return 1;
  }
  else{
	printf("FILE DOES NOT EXIST\n");
	return 0;
  }
}

static char* __aws_sign ( char * const str )
{
  HMAC_CTX *ctx;
  unsigned char MD[256];
  unsigned len;

  __debug("StrToSign:%s", str );

  ctx = HMAC_CTX_new();
  HMAC_Init(ctx, awsKey, strlen(awsKey), EVP_sha1());
  HMAC_Update(ctx,(unsigned char*)str, strlen(str));
  HMAC_Final(ctx,(unsigned char*)MD,&len);
  HMAC_CTX_free(ctx);

  char * b64 = __b64_encode (MD,len);
  __debug("Signature:  %s", b64 );

  return b64;
}



