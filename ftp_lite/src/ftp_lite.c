/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2022 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifdef HAS_GLOBUS_GSS
#include "globus_gss_assist.h"
#endif

#include "ftp_lite.h"
#include "error.h"
#include "radix.h"
#include "network.h"

#include "stringtools.h"
#include "debug.h"
#include "full_io.h"

#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <errno.h>
#include <string.h>
#include <stdarg.h>
#include <unistd.h>
#include <fcntl.h>
#include <ctype.h>
#include <unistd.h>
#include <signal.h>
#include <sys/types.h>

int ftp_lite_data_channel_authentication = 0;

static int ftp_lite_send_command_gss( struct ftp_lite_server *s, const char *outbuffer );
static int ftp_lite_get_response_gss( struct ftp_lite_server *s, char *outbuffer );
static int ftp_lite_data_channel_auth( struct ftp_lite_server *s, FILE *stream );

struct ftp_lite_server {
	FILE *command;
	FILE *response;
	char *hostname;
	int broken;
	int went_binary;
	enum { PLAIN, GLOBUS_GSS } authtype;
	int auth_done;
	int data_channel_authentication;
	#ifdef HAS_GLOBUS_GSS
		gss_ctx_id_t context;
		gss_ctx_id_t data_context;
		gss_cred_id_t credential;
	#endif
};

static int ftp_lite_send_command_raw( struct ftp_lite_server *s, const char *line )
{
	char buf[FTP_LITE_LINE_MAX];
	int length;
	length = snprintf(buf,FTP_LITE_LINE_MAX,"%s\r\n",line);
	return(full_fwrite(s->command,buf,length)==length);
}

static int ftp_lite_get_response_raw( struct ftp_lite_server *s, char *line )
{
	char *result;

	while(1) {
		result = fgets(line,FTP_LITE_LINE_MAX,s->response);
		if(result) {
			string_chomp(line);
			return 1;
		} else {
			if(errno==EINTR) {
				continue;
			} else {
				errno = ECONNRESET;
				return 0;
			}
		}
	}
}

static int ftp_lite_send_command( struct ftp_lite_server *s, const char *fmt, ... )
{
	char buffer[FTP_LITE_LINE_MAX];
	va_list args;

	va_start(args,fmt);
	vsprintf(buffer,fmt,args);
	va_end(args);

	if(!strncmp(buffer,"PASS",4)) {
		debug(D_FTP,"%s PASS ******\n",s->hostname);
	} else {
		debug(D_FTP,"%s %s\n",s->hostname,buffer);
	}

	switch(s->authtype) {
		case PLAIN:
			return ftp_lite_send_command_raw(s,buffer);
		case GLOBUS_GSS:
			return ftp_lite_send_command_gss(s,buffer);
		default:
			errno = ENOTSUP;
			return 0;
	}
}

static int ftp_lite_get_response( struct ftp_lite_server *s, int accept_note, char *buffer )
{
	int c;
	char dash;
	int result;
	int response;
	int fields;
	int do_message = 0;

	while(1) {
		switch(s->authtype) {
			case PLAIN:
				result = ftp_lite_get_response_raw(s,buffer);
				break;
			case GLOBUS_GSS:
				/*
				Depending on the server, some responses are
				encrypted and some are not, even once the secure
				channel has been established.
				*/
				do {
					errno = 0;
					c = fgetc(s->response);
				} while(c==EOF && errno==EINTR);
				ungetc(c,s->response);
				if(!isdigit(c)) {
					result = 0;
					errno = ECONNRESET;
				} else if(c=='6') {
					result = ftp_lite_get_response_gss(s,buffer);
				} else {
					result = ftp_lite_get_response_raw(s,buffer);
				}
				break;
			default:
				errno = ENOTSUP;
				return 0;
		}

		if(!result) return 0;

		string_chomp(buffer);

		debug(D_FTP,"%s %s\n",s->hostname,buffer);

		if(!isdigit((int)(buffer[0]))) continue;

		fields = sscanf(buffer,"%d%c",&response,&dash);
		if(fields!=2) {
			continue;
		} else {
			if( do_message ) {
				if( (dash==' ') && (response==do_message) ) {
					do_message = 0;
				} else {
					continue;
				}
			} else {
				if( dash=='-' ) {
					do_message = response;
					continue;
				}
			}

			if( (response/100)==1 ) {
				if(accept_note) {
					return response;
				} else {
					continue;
				}
			} else {
				return response;
			}
		}
	}
	return response;
}

static int ftp_lite_parse_passive( const char *buffer, char *addr, int *port )
{
	int response, fields;
	int hi,lo;
	int a,b,c,d;

	fields = sscanf(buffer,"%d %*[^(] (%d,%d,%d,%d,%d,%d)",&response,&a,&b,&c,&d,&hi,&lo);
	if(fields!=7) return 0;

	*port = hi*256+lo;
	sprintf(addr,"%d.%d.%d.%d",a,b,c,d);

	return 1;
}

static int ftp_lite_send_active( struct ftp_lite_server *s, char *addr, int port )
{
	char buffer[FTP_LITE_LINE_MAX];
	int a,b,c,d;
	int fields;

	fields = sscanf(addr,"%d.%d.%d.%d",&a,&b,&c,&d);
	if(fields!=4) return 0;

	sprintf(buffer,"PORT %d,%d,%d,%d,%d,%d",a,b,c,d,port/256,port&0xff);
	return ftp_lite_send_command(s,buffer);
}

struct ftp_lite_server * ftp_lite_open_and_auth( const char *host, int port )
{
	struct ftp_lite_server *s;
	int save_errno;
	int gss_port = FTP_LITE_GSS_DEFAULT_PORT;
	int normal_port = FTP_LITE_DEFAULT_PORT;

	if(port) {
		gss_port = port;
		normal_port = port;
	}

	debug(D_FTP,"*** attempting secure connection to %s port %d\n",host,gss_port);

	s = ftp_lite_open(host,gss_port);
	if(s) {
		if(ftp_lite_auth_globus(s)) {
			return s;
		}
		ftp_lite_close(s);
	}

	debug(D_FTP,"*** attempting insecure connection to %s port %d\n",host,normal_port);

	s = ftp_lite_open(host,normal_port);
	if(s) {
		char name[FTP_LITE_LINE_MAX];
		char pass[FTP_LITE_LINE_MAX];

		if(ftp_lite_login(host,name,FTP_LITE_LINE_MAX,pass,FTP_LITE_LINE_MAX)) {
			if(ftp_lite_auth_userpass(s,name,pass)) {
				memset(pass,0,strlen(pass));
				return s;
			}
			memset(pass,0,strlen(pass));
		}
		save_errno = errno;
		ftp_lite_close(s);
		errno = save_errno;
	}

	return 0;
}

struct ftp_lite_server * ftp_lite_open( const char *host, int port )
{
	char buffer[FTP_LITE_LINE_MAX];
	struct ftp_lite_server *s;
	int response;
	int save_errno;
	int net=-1;

	s = malloc(sizeof(*s));
	if(!s) return 0;

	s->authtype = PLAIN;
	s->auth_done = 0;
	s->hostname = strdup(host);
	s->went_binary = 0;
	s->data_channel_authentication = 0;

	if(!s->hostname) {
		free(s);
		return 0;
	}

	#ifdef HAS_GLOBUS_GSS
		s->context = GSS_C_NO_CONTEXT;
		s->data_context = GSS_C_NO_CONTEXT;
		s->credential = GSS_C_NO_CREDENTIAL;
	#endif

	net = network_connect(host,port);
	if(net<0) {
		save_errno = errno;
		free(s->hostname);
		free(s);
		errno = save_errno;
		return 0;
	}

	s->command = fdopen(net,"r+");
	if(!s->command) {
		save_errno = errno;
		network_close(net);
		free(s->hostname);
		free(s);
		errno = save_errno;
		return 0;
	}

	s->response = fdopen(net,"r");
	if(!s->response) {
		save_errno = errno;
		fclose(s->command);
		free(s->hostname);
		free(s);
		errno = save_errno;
		return 0;
	}

	setbuf(s->command,0);
	setbuf(s->response,0);

	response = ftp_lite_get_response(s,0,buffer);
	if((response/100)!=2) {
		fclose(s->response);
		fclose(s->command);
		free(s->hostname);
		free(s);
		errno = ftp_lite_error(response);
		return 0;
	}

	/* Most servers send 220, but promiscuous servers send 230 */
	if(response==230) {
		s->auth_done=1;
	}

	return s;
}

int ftp_lite_auth_anonymous( struct ftp_lite_server *s )
{
	return ftp_lite_auth_userpass(s,"anonymous","anonymous");
}

int ftp_lite_auth_userpass( struct ftp_lite_server *s, const char *user, const char *pass )
{
	int r1, r2;
	char buffer[FTP_LITE_LINE_MAX];

	if(s->auth_done) return 1;

	ftp_lite_send_command(s,"USER %s",user);
	ftp_lite_send_command(s,"PASS %s",pass);

	r1 = ftp_lite_get_response(s,0,buffer);
	r2 = ftp_lite_get_response(s,0,buffer);

	if( ((r1/100)!=3) && ((r1/100)!=2) ) {
		errno = ftp_lite_error(r1);
		return 0;
	}

	if((r2/100)!=2) {
		errno = ftp_lite_error(r2);
		return 0;
	}

	s->auth_done = 1;

	return 1;
}

static FILE * ftp_lite_xfer_setup( struct ftp_lite_server *s, char *command )
{
	char buffer[FTP_LITE_LINE_MAX];
	char host[NETWORK_ADDR_MAX];
	int passive;
	FILE *stream;
	int net;
	int port;
	int response;
	int save_errno;

	if(!s->went_binary) {
		ftp_lite_send_command(s,"TYPE I");
		response = ftp_lite_get_response(s,0,buffer);
		if((response/100)!=2) return 0;
		s->went_binary = 1;
	}

	ftp_lite_send_command(s,"PASV");
	response = ftp_lite_get_response(s,0,buffer);

	if((response/100)!=2) {
		network_address caddr, paddr;
		int cport, pport;

		network_address_local(fileno(s->command),&caddr,&cport);

		passive = network_serve(0);
		if(passive<0) return 0;

		network_address_local(passive,&paddr,&pport);
		network_address_to_string(caddr,host);

		ftp_lite_send_active(s,host,pport);
		response = ftp_lite_get_response(s,0,buffer);
		if((response/100)!=2) {
			network_close(passive);
			errno = ftp_lite_error(response);
			return 0;
		}
	} else {
		passive = -1;
		if(!ftp_lite_parse_passive(buffer,host,&port)) {
			errno = EINVAL;
			return 0;
		}
	}

	ftp_lite_send_command(s,command);

	if(passive>=0) {
		while(1) {
			if(network_sleep(passive,100000)) {
				net = network_accept(passive);
				if(net<0) {
					save_errno = errno;
					network_close(passive);
					errno = save_errno;
					return 0;
				} else {
					network_close(passive);
					break;
				}
			} else {
				if(network_sleep(fileno(s->response),0)) {
					response = ftp_lite_get_response(s,1,buffer);
					if((response/100)==1) {
						continue;
					} else {
						network_close(passive);
						errno = ftp_lite_error(response);
						return 0;
					}
				}
			}
		}
	} else {
		net = network_connect(host,port);
		if(net<0) return 0;

		/*
		This is ridiculous.
		When data channel authentication is enabled,
		if the STOR or RETR fails because of a filesystem error
		on the server side, then the server sends an error code
		AFTER the network connection, but before the authentication.
		If the command will succeed, then the server sends a
		100 code AFTER authentication.  Arg!  So, we sit here and
		wait for 100ms to see if a response comes back.
		Fortunately, we have already done a round trip to make
		the connection, so we don't have to measure that time.
		*/

		if(network_sleep(fileno(s->response),10000)) {
			response = ftp_lite_get_response(s,1,buffer);
			if((response/100)==1) {
				/* keep going */
			} else {
				network_close(net);
				errno = ftp_lite_error(response);
				return 0;
			}
		}
	}

	stream = fdopen(net,"r+");
	if(!stream) {
		network_close(net);
		return 0;
	}

	if(ftp_lite_data_channel_auth(s,stream)) {
		return stream;
	} else {
		fclose(stream);
		return 0;
	}
}

FILE * ftp_lite_get( struct ftp_lite_server *s, const char *path, ftp_lite_off_t offset )
{
	char buffer[FTP_LITE_LINE_MAX];
	int response;
	FILE *data;

	if(offset!=0) {
		ftp_lite_send_command(s,"REST %d",offset);
		response = ftp_lite_get_response(s,0,buffer);
		if(response/100!=3) {
			errno = ftp_lite_error(response);
			return 0;
		}
	}

	sprintf(buffer,"RETR %s",path);
	data = ftp_lite_xfer_setup(s,buffer);
	if(data) {
		return data;
	} else {
		return 0;
	}
}

FILE * ftp_lite_put( struct ftp_lite_server *s, const char *path, ftp_lite_off_t offset, ftp_lite_size_t size )
{
	char buffer[FTP_LITE_LINE_MAX];
	int response;
	FILE *data;

	if( offset!=0 ) {
		if( size==FTP_LITE_WHOLE_FILE ) {
			ftp_lite_send_command(s,"REST %lld",(long long)offset);
			response = ftp_lite_get_response(s,0,buffer);
			if(response/100!=3) {
				errno = ftp_lite_error(response);
				return 0;
			}
			sprintf(buffer,"STOR %s",path);
		} else {
			sprintf(buffer,"ESTO A %lld %s",(long long)offset,path);
		}
	} else {
		sprintf(buffer,"STOR %s",path);
	}

	data = ftp_lite_xfer_setup(s,buffer);
	if(data) {
		return data;
	} else {
		return 0;
	}
}

FILE *ftp_lite_list( struct ftp_lite_server *s, const char *dir )
{
	char buffer[FTP_LITE_LINE_MAX];
	FILE *data;

	sprintf(buffer,"NLST %s",dir);

	data = ftp_lite_xfer_setup(s,buffer);
	if(data) {
		return data;
	} else {
		return 0;
	}
}

int ftp_lite_done( struct ftp_lite_server *s )
{
	char buffer[FTP_LITE_LINE_MAX];
	int response;

	response = ftp_lite_get_response(s,0,buffer);
	if(response/100!=2) {
		errno = ftp_lite_error(response);
		return 0;
	} else {
		return 1;
	}
}

ftp_lite_size_t ftp_lite_size( struct ftp_lite_server *s, const char *path )
{
	char buffer[FTP_LITE_LINE_MAX];
	ftp_lite_size_t size;
	int response;
	int fields;

	ftp_lite_send_command(s,"SIZE %s",path);
	response = ftp_lite_get_response(s,0,buffer);
	if(response/100!=2) {
		errno = ftp_lite_error(response);
		return -1;
	}

	fields = sscanf(buffer,"%d %lld",&response,&size);
	if(fields!=2) {
		errno = EINVAL;
		return -1;
	}

	return size;
}

int ftp_lite_delete( struct ftp_lite_server *s, const char *path )
{
	char buffer[FTP_LITE_LINE_MAX];
	int response;

	ftp_lite_send_command(s,"DELE %s",path);
	response = ftp_lite_get_response(s,0,buffer);
	if(response/100!=2) {
		errno = ftp_lite_error(response);
		return 0;
	}

	return 1;
}

int ftp_lite_rename( struct ftp_lite_server *s, const char *oldname, const char *newname )
{
	char buffer[FTP_LITE_LINE_MAX];
	int r1, r2;

	ftp_lite_send_command(s,"RNFR %s",oldname);
	ftp_lite_send_command(s,"RNTO %s",newname);

	r1 = ftp_lite_get_response(s,0,buffer);
	r2 = ftp_lite_get_response(s,0,buffer);

	if( r1/100 != 3 ) {
		errno = ftp_lite_error(r1);
		return 0;
	}

	if( r2/100 != 2 ) {
		errno = ftp_lite_error(r2);
		return 0;
	}

	return 1;
}

int ftp_lite_current_dir( struct ftp_lite_server *s, char *dir )
{
	char buffer[FTP_LITE_LINE_MAX];
	int response;

	ftp_lite_send_command(s,"PWD");
	response = ftp_lite_get_response(s,0,buffer);
	if(response/100!=2) {
		errno = ftp_lite_error(response);
		return 0;
	} else {
		if(sscanf(buffer,"%d \"%[^\"]\"",&response,dir)==2) {
			return 1;
		} else {
			debug(D_FTP,"couldn't parse response from PWD!");
			return 0;
		}
	}
}

int ftp_lite_change_dir( struct ftp_lite_server *s, const char *dir )
{
	char buffer[FTP_LITE_LINE_MAX];
	int response;

	ftp_lite_send_command(s,"CWD %s",dir);
	response = ftp_lite_get_response(s,0,buffer);
	if(response/100!=2) {
		errno = ftp_lite_error(response);
		return 0;
	}

	return 1;
}

int ftp_lite_make_dir( struct ftp_lite_server *s, const char *dir )
{
	char buffer[FTP_LITE_LINE_MAX];
	int response;

	ftp_lite_send_command(s,"MKD %s",dir);
	response = ftp_lite_get_response(s,0,buffer);
	if(response/100!=2) {
		errno = ftp_lite_error(response);
		return 0;
	}

	return 1;
}

int ftp_lite_delete_dir( struct ftp_lite_server *s, const char *dir )
{
	char buffer[FTP_LITE_LINE_MAX];
	int response;

	ftp_lite_send_command(s,"RMD %s",dir);
	response = ftp_lite_get_response(s,0,buffer);
	if(response/100!=2) {
		errno = ftp_lite_error(response);
		return 0;
	}

	return 1;
}

int ftp_lite_nop( struct ftp_lite_server *s )
{
	char buffer[FTP_LITE_LINE_MAX];
	int response;

	ftp_lite_send_command(s,"NOOP");
	response = ftp_lite_get_response(s,0,buffer);
	if(response/100!=2) {
		errno = ftp_lite_error(response);
		return 0;
	}

	return 1;
}

static int ftp_lite_third_party_setup( struct ftp_lite_server *source, struct ftp_lite_server *target )
{
	char buffer[FTP_LITE_LINE_MAX];
	int response;
	int port;
	char host[NETWORK_ADDR_MAX];

	ftp_lite_send_command(source,"PASV");
	response = ftp_lite_get_response(source,0,buffer);
	if(response/100!=2) {
		errno = ftp_lite_error(response);
		return 0;
	}


	if(!ftp_lite_parse_passive(buffer,host,&port)) {
		errno = EINVAL;
		return 0;
	}

	ftp_lite_send_active(target,host,port);
	response = ftp_lite_get_response(target,0,buffer);
	if(response/100!=2) {
		errno = ftp_lite_error(response);
		return 0;
	}

	return 1;
}


int ftp_lite_third_party_transfer( struct ftp_lite_server *source, const char *source_file, struct ftp_lite_server *target, const char *target_file )
{
	char buffer[FTP_LITE_LINE_MAX];
	int response;

	if(!source->went_binary) {
		ftp_lite_send_command(source,"TYPE I");
		response = ftp_lite_get_response(source,0,buffer);
		if((response/100)!=2) {
			errno = ftp_lite_error(response);
			return 0;
		}
		source->went_binary = 1;
	}

	if(!target->went_binary) {
		ftp_lite_send_command(target,"TYPE I");
		response = ftp_lite_get_response(target,0,buffer);
		if((response/100)!=2) {
			errno = ftp_lite_error(response);
			return 0;
		}
		target->went_binary = 1;
	}

	if(!ftp_lite_third_party_setup(source,target)) {
		if(!ftp_lite_third_party_setup(target,source)) {
			return 0;
		}
	}

	ftp_lite_send_command(target,"STOR %s",target_file);
	ftp_lite_send_command(source,"RETR %s",source_file);

	while(1) {
		if(network_sleep(fileno(target->response),10000)) {
			response = ftp_lite_get_response(target,1,buffer);
			if(response/100==1) {
				continue;
			} else if(response/100==2) {
				response = ftp_lite_get_response(source,0,buffer);
				if(response/100==2) {
					return 1;
				} else {
					errno = ftp_lite_error(response);
					return 0;
				}
			} else {
				ftp_lite_send_command(source,"ABOR");
				ftp_lite_get_response(source,0,buffer);
				errno = ftp_lite_error(response);
				return 0;
			}
		}
		if(network_sleep(fileno(source->response),10000)) {
			response = ftp_lite_get_response(source,1,buffer);
			if(response/100==1) {
				continue;
			} else if(response/100==2) {
				response = ftp_lite_get_response(target,0,buffer);
				if(response/100==2) {
					return 1;
				} else {
					errno = ftp_lite_error(response);
					return 0;
				}
			} else {
				ftp_lite_send_command(target,"ABOR");
				ftp_lite_get_response(target,0,buffer);
				errno = ftp_lite_error(response);
				return 0;
			}
		}
	}
}

void ftp_lite_close( struct ftp_lite_server *s )
{
	#ifdef HAS_GLOBUS_GSS
		OM_uint32 major;

		if(s->context!=GSS_C_NO_CONTEXT) {
			gss_delete_sec_context(&major,&s->context,GSS_C_NO_BUFFER);
		}
		if(s->credential!=GSS_C_NO_CREDENTIAL) {
			gss_release_cred(&major,&s->credential);
		}
	#endif

	network_close(fileno(s->command));
	fclose(s->command);
	fclose(s->response);
	free(s->hostname);
	free(s);
}

#ifdef HAS_GLOBUS_GSS

static int ftp_lite_send_gss( void *arg, void *buffer, size_t length )
{
	struct ftp_lite_server *s = (struct ftp_lite_server *)arg;
	char line[FTP_LITE_LINE_MAX];
	int ilength = length;

	sprintf(line,"MIC ");
	if(!ftp_lite_radix_encode(buffer,(unsigned char*)&line[4],&ilength)) return -1;
	line[ilength+4] = 0;

	if(ftp_lite_send_command_raw(s,line)) {
		return 0;
	} else {
		return -1;
	}
}

static int ftp_lite_send_command_gss( struct ftp_lite_server *s, const char *buffer )
{
	OM_uint32 minor;
	int token;

	return globus_gss_assist_wrap_send(&minor,s->context,(char *)buffer,strlen(buffer),&token,ftp_lite_send_gss,s,stderr)==GSS_S_COMPLETE;
}

static int ftp_lite_get_gss( void *arg, void **buffer, size_t *length )
{
	char line[FTP_LITE_LINE_MAX];
	struct ftp_lite_server *s = (struct ftp_lite_server *)arg;

	if(!ftp_lite_get_response_raw(s,line)) return -1;
	*length = strlen(line)-4;

	*buffer = malloc(*length+1);
	if(!*buffer) return -1;

	int ilength = *length;
	if(!ftp_lite_radix_decode((unsigned char*)&line[4],*buffer,&ilength)) return -1;
	*length = ilength;

	return 0;
}

static int ftp_lite_get_response_gss( struct ftp_lite_server *s, char *outbuffer )
{
	OM_uint32 major, minor;
	int token;
	size_t length;
	char *buffer;

	major = globus_gss_assist_get_unwrap(&minor,s->context,&buffer,&length,&token,ftp_lite_get_gss,s,stderr);
	if(major==GSS_S_COMPLETE) {
		strcpy(outbuffer,buffer);
		outbuffer[length] = 0;
		return 1;
	} else {
		errno = ECONNRESET;
		return 0;
	}
}

static int ftp_lite_get_adat( void *arg, void **outbuf, size_t *length )
{
	char buffer[FTP_LITE_LINE_MAX];
	struct ftp_lite_server *s = (struct ftp_lite_server *)arg;
	int response;

	response = ftp_lite_get_response(s,0,buffer);
	if(response/100!=3) return -1;

	if(strncmp(buffer,"335 ADAT=",9)) return -1;

	*length = strlen(buffer)-9;
	*outbuf = malloc(*length+1);
	if(!*outbuf) return -1;

	int ilength = *length;
	if(!ftp_lite_radix_decode((unsigned char*)&buffer[9],(unsigned char *)*outbuf,&ilength)) {
		free(*outbuf);
		return -1;
	}

	*length = ilength;

	return 0;
}

static int ftp_lite_put_adat( void *arg, void *buf, size_t size )
{
	char buffer[FTP_LITE_LINE_MAX];
	struct ftp_lite_server *s = (struct ftp_lite_server *)arg;

	int ilength = size;
	if(!ftp_lite_radix_encode((unsigned char *)buf,(unsigned char*)buffer,&ilength)) return -1;

	ftp_lite_send_command(s,"ADAT %s",buffer);
	return 0;
}

int ftp_lite_data_channel_auth( struct ftp_lite_server *s, FILE *data )
{
	OM_uint32 major, minor, flags=0;
	int token;

	if(!s->data_channel_authentication) return 1;

	setbuf(data,0);

	debug(D_FTP,"data channel authentication in progress...");

	major = globus_gss_assist_init_sec_context( &minor, s->credential, &s->data_context, 0, 0, &flags, &token, globus_gss_assist_token_get_fd, data, globus_gss_assist_token_send_fd, data );
	if( major!=GSS_S_COMPLETE ) {
		char *reason;
		globus_gss_assist_display_status_str(&reason,"",major,minor,token);
		if(!reason) reason = strdup("unknown error");
		debug(D_FTP,"data channel authentication failed: %s",reason);
		free(reason);
		errno = EACCES;
		return 0;
	}

	gss_delete_sec_context(&major,&s->data_context,GSS_C_NO_BUFFER);

	debug(D_FTP,"data channel authentication succeeded");

	return 1;

}

int ftp_lite_auth_globus( struct ftp_lite_server *s )
{
	char buffer[FTP_LITE_LINE_MAX];
	char principal_buf[FTP_LITE_LINE_MAX];
	char *principal;
	OM_uint32 major, minor, flags=0;
	network_address addr;
	int token,port;
	int response;

	if(s->auth_done) return 1;

	major = globus_gss_assist_acquire_cred(&minor,GSS_C_INITIATE,&s->credential);
	if(major!=GSS_S_COMPLETE) {
		errno = EACCES;
		return 0;
	}

	ftp_lite_send_command(s,"AUTH GSSAPI");
	response = ftp_lite_get_response(s,0,buffer);

	if(response/100==2) {
		/* Promiscuous servers respond with 200 here */
		return 1;
	}

	if(response/100!=3) {
		errno = ftp_lite_error(response);
		return 0;
	}

	principal = getenv("FTP_LITE_PRINCIPAL");
	if(!principal) {
		strcpy(principal_buf,"ftp@");
		if(!network_address_remote(fileno(s->command),&addr,&port)) return 0;
		if(!network_address_to_name(addr,&principal_buf[4])) return 0;
		principal = principal_buf;
	}

	major = globus_gss_assist_init_sec_context( &minor, s->credential, &s->context, principal, GSS_C_MUTUAL_FLAG | GSS_C_DELEG_FLAG, &flags, &token, ftp_lite_get_adat, s, ftp_lite_put_adat, s );
	if( major!=GSS_S_COMPLETE ) {
		gss_release_cred( &major, &s->credential );
		errno = EACCES;
		return 0;
	}

	debug(D_FTP,"*** secure channel established\n");

	s->authtype = GLOBUS_GSS;

	response = ftp_lite_get_response(s,0,buffer);
	if((response/100)!=2) {
		errno = ftp_lite_error(response);
		return 0;
	}

	if(ftp_lite_auth_userpass(s,":globus-mapping:","nothing")) {
		if(ftp_lite_data_channel_authentication) {
			ftp_lite_send_command(s,"DCAU A");
			response = ftp_lite_get_response(s,0,buffer);
			s->data_channel_authentication = (response==200);
		} else {
			ftp_lite_send_command(s,"DCAU N");
			response = ftp_lite_get_response(s,0,buffer);
			s->data_channel_authentication = 0;
		}
		return 1;
	} else {
		return 0;
	}
}

#else

int ftp_lite_data_channel_auth( struct ftp_lite_server *s, FILE *data )
{
	return 1;
}

static int ftp_lite_send_command_gss( struct ftp_lite_server *s, const char *buffer )
{
	errno = ENOSYS;
	return 0;
}

static int ftp_lite_get_response_gss( struct ftp_lite_server *s, char *outbuffer )
{
	errno = ENOSYS;
	return 0;
}

int ftp_lite_auth_globus( struct ftp_lite_server *s )
{
	errno = ENOSYS;
	return 0;
}

#endif


/* vim: set noexpandtab tabstop=8: */
