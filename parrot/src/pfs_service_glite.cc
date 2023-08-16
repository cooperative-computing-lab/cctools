/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2022 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/
/*
Copyright (c) Members of the EGEE Collaboration. 2004.
See http://eu-egee.org/partners/ for details on the copyright holders.
For license conditions see the license file or
http://eu-egee.org/license.html
Author: Peter Kunszt
Date:   5.10.2004
*/

#ifdef HAS_GLITE

#include "pfs_service.h"
#include "glite/data/catalog/fireman/cpp/fireman.nsmap"
#include "glite/data/catalog/fireman/cpp/firemanFiremanCatalogSoapBindingProxy.h"

extern "C" {
#include "glite/data/io/client/ioclient.h"
#include "glite/data/io/client/ioerrors.h"
#include "path.h"
#include "debug.h"
}

#include "stdsoap2.h"   					    // autogen
struct Namespace namespaces[] ={{NULL, NULL}}; // autogen

#include <unistd.h>
#include <string.h>
#include <stdio.h>
#include <fcntl.h>
#include <errno.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <sys/statfs.h>

extern char *pfs_ccurl;

class pfs_file_glite : public pfs_file
{
private:
		glite_handle gh;
		pfs_name *gname;
		fireman::FiremanCatalogSoapBinding *m_cc;

		void convert_cc_name( pfs_name *name, char * path ) {
				char tmp[PFS_PATH_MAX];
		sprintf(tmp,"/%s/%s",name->host,name->rest);
		path_collapse(tmp, path, 1);
		if(strlen(path)==0){
			path[0] = '/';
			path[1] = 0;
		}
		debug(D_GLITE,"CC1: glite using file name:%s",path);
	}
		fireman::FiremanCatalogSoapBinding * getCatalog() {
		debug(D_GLITE,"Getting the fireman catalog : %s",pfs_ccurl);
			if(!m_cc) {
			if(pfs_ccurl) {
			m_cc = new fireman::FiremanCatalogSoapBinding();
			m_cc->endpoint = pfs_ccurl;
			} else {
			debug(D_GLITE,"the catalog URL is mandatory for glite usage, use -E or PARROT_GLITE_CCURL");
			return 0;
			}
		}
		debug(D_GLITE,"Got the fireman catalog:%s",m_cc->endpoint);
		return m_cc;
	}

public:
	pfs_file_glite( pfs_name *n, glite_handle g ) : pfs_file(n) {
		gh = g;
		gname = new pfs_name;
		memcpy(gname,n,sizeof(pfs_name));
		m_cc = 0;
	}

	virtual int close() {
		int res = glite_close(gh);
		debug(D_GLITE,"close file %s  : %d",gname->path, res);
		return res;
	}

	virtual pfs_ssize_t read( void *data, pfs_size_t length, pfs_off_t offset ) {
			int seek=0;
		seek = glite_lseek(gh, offset, SEEK_SET);
		debug(D_GLITE,"read: seek to %d bytes - offset = %d, length = %d",seek,offset,length);
		if ( seek < 0 ) {
			return seek;
		}
		int r = 0;
		r = glite_read(gh, data, length);
		if(r < 0) {
		  int e = glite_error((int)gh);
		  debug(D_GLITE,"error number : %d",e);
		  return 0;
		}
		debug(D_GLITE,"read %d bytes",r);
		return r;
	}

	virtual pfs_ssize_t write( const void *data, pfs_size_t length, pfs_off_t offset ) {
			int s=0;
		s = glite_lseek(gh, offset, SEEK_SET);
		if ( s < 0 ) {
			return s;
		}
		return glite_write(gh, data, length);
	}

	virtual int fstat( struct pfs_stat *buf ) {

				fireman::FiremanCatalogSoapBinding *cc = getCatalog();
			if (! cc ) {
			errno = EFAULT;
			return -1;
		}

				char glite_name[PFS_PATH_MAX];
		convert_cc_name( gname, glite_name );
		debug(D_GLITE,"fstat: %s",glite_name);
				fireman::ArrayOf_USCOREsoapenc_USCOREstring l;
		debug(D_GLITE,"trace 1");
		fireman::fireman__getLfnStatResponse out;
		debug(D_GLITE,"trace 2");

		l.__size = 1;
		debug(D_GLITE,"trace 3");
		l.__ptr = (char **) soap_malloc(cc->soap,sizeof(char*));
		debug(D_GLITE,"trace 4");
		l.__ptr[0] = soap_strdup(cc->soap,glite_name);

		debug(D_GLITE,"calling getLfnStat(%s)",l.__ptr[0]);
		if(SOAP_OK != cc->fireman__getLfnStat(&l,out)){
			errno = ENOENT;
			if(cc->soap == 0)
			debug(D_GLITE,"soap struct NULL in fireman catalog");
			else if(cc->soap->fault == 0)
			debug(D_GLITE,"Failed to get LFN stat - NULL fault object");
			else {
			debug(D_GLITE,"Failed to get LFN stat: %s - %s",cc->soap->fault->faultcode,cc->soap->fault->faultstring);
			if(strncmp("Connection refused",cc->soap->fault->faultstring,18)==0)
			   errno = EFAULT;
			}
			return -1;
		}

		pfs_service_emulate_stat(gname,buf);
		buf->st_ctime= out._getLfnStatReturn->__ptr[0]->lfnStat->creationTime;
		buf->st_mtime= out._getLfnStatReturn->__ptr[0]->lfnStat->modifyTime;
		buf->st_size= out._getLfnStatReturn->__ptr[0]->lfnStat->size;

		int type = buf->st_mtime= out._getLfnStatReturn->__ptr[0]->lfnStat->type;
		switch(type) {
		case 0: 		    // normal file
			buf->st_mode = S_IFREG;
			break;
		case 1:		    	    // directory
			buf->st_mode = S_IFDIR;
			break;
		case 2:		    	    // symlink
			buf->st_mode = S_IFLNK;
			break;
		default: // what else?
			errno = EFAULT;
			return -1;
		}

		if( out._getLfnStatReturn->__ptr[0]->permission->userPerm->read )
			buf->st_mode |= S_IRUSR;
		if( out._getLfnStatReturn->__ptr[0]->permission->userPerm->write )
			buf->st_mode |= S_IWUSR;
		if( out._getLfnStatReturn->__ptr[0]->permission->userPerm->execute )
			buf->st_mode |= S_IXUSR;

		if( out._getLfnStatReturn->__ptr[0]->permission->groupPerm->read )
			buf->st_mode |= S_IRGRP;
		if( out._getLfnStatReturn->__ptr[0]->permission->groupPerm->write )
			buf->st_mode |= S_IWGRP;
		if( out._getLfnStatReturn->__ptr[0]->permission->groupPerm->execute )
			buf->st_mode |= S_IXGRP;

		if( out._getLfnStatReturn->__ptr[0]->permission->otherPerm->read )
			buf->st_mode |= S_IROTH;
		if( out._getLfnStatReturn->__ptr[0]->permission->otherPerm->write )
			buf->st_mode |= S_IWOTH;
		if( out._getLfnStatReturn->__ptr[0]->permission->otherPerm->execute )
			buf->st_mode |= S_IXOTH;

		debug(D_GLITE,"got stat for %s - guid = %s",glite_name,out._getLfnStatReturn->__ptr[0]->guid);
		return 0;

	}

	virtual pfs_ssize_t get_size() {
			int s=0;
		struct glite_stat gstat;
		s = glite_fstat( gh,  &gstat);
		if ( s < 0 ) {
			return s;
		}
		return gstat.size;
	}

};

class pfs_service_glite : public pfs_service {
private:
		fireman::FiremanCatalogSoapBinding *m_cc;

		void convert_file_name( pfs_name *name, char * path ) {
				char tmp[PFS_PATH_MAX];
				char tmp2[PFS_PATH_MAX];
		sprintf(tmp,"/%s/%s",name->host,name->rest);
		path_collapse(tmp, tmp2, 1);
		if(strlen(tmp2)==0){
			tmp2[0] = '/';
			tmp2[1] = 0;
		}
		sprintf(path,"lfn://%s",tmp2);
		debug(D_GLITE,"FILE glite using file name:%s",path);
	}

		void convert_cc_name( pfs_name *name, char * path ) {
				char tmp[PFS_PATH_MAX];
		sprintf(tmp,"/%s/%s",name->host,name->rest);
		path_collapse(tmp, path, 1);
		if(strlen(path)==0){
			path[0] = '/';
			path[1] = 0;
		}
		debug(D_GLITE,"CC2 glite using file name:%s",path);
	}
		fireman::FiremanCatalogSoapBinding * getCatalog() {
			if(!m_cc) {
			if(pfs_ccurl) {
			m_cc = new fireman::FiremanCatalogSoapBinding();
			m_cc->endpoint = pfs_ccurl;
			} else {
			debug(D_GLITE,"the catalog URL is mandatory for glite usage, use -E or PARROT_GLITE_CCURL");
			return 0;
			}
		}
		debug(D_GLITE,"Got the fireman catalog:%s",m_cc->endpoint);
		return m_cc;
	}
public:
		pfs_service_glite() : m_cc(0) {
		//TODO fail here if configuration is not correct
	}

	virtual pfs_file * open( pfs_name *name, int flags, mode_t mode ) {
		glite_handle gh;
		glite_result result;
		if((flags&O_ACCMODE)!=O_RDONLY &&
		   (flags&O_ACCMODE)!=O_WRONLY &&
		   (flags&O_ACCMODE)!=O_CREAT) {
			   errno = ENOTSUP;
			   return 0;
		}

		// undo the mangling the pfs_table::resolve_name has done - glite has
		// its own mechanism for host lookup throught the config file of glite-io
				char glite_name[PFS_PATH_MAX];
		convert_file_name( name, glite_name );
		debug(D_GLITE,"open: %s",glite_name);
		gh = glite_open( glite_name, flags, mode, 0, &result);
		if(gh == GLITE_NULL_HANDLE) {
			debug(D_GLITE,"open error: %d ",result);
			switch(result) {
			case GLITE_IO_CONFIGERROR:
			errno = E2BIG; // silly but didn't find anything better
			break;
			case GLITE_IO_INVALIDNAME:
			errno = EINVAL;
			break;
			case GLITE_IO_NOTIMPLEMENTED:
			errno = ENOSYS;
			break;
			case GLITE_IO_OPENERROR:
			errno = EIO;
			break;
			}
			return 0;
		}
		return new pfs_file_glite(name,gh);
	}

	virtual pfs_dir * getdir( pfs_name *name )
	{
		pfs_dir *dirob;

		debug(D_GLITE,"opendir %s",name->path);
				fireman::FiremanCatalogSoapBinding *cc = getCatalog();
			if (! cc ) {
			errno = EFAULT;
			return 0;
		}

				char glite_name[PFS_PATH_MAX];
		convert_cc_name( name, glite_name );
		fireman::fireman__readDirResponse out;

		if(SOAP_OK != cc->fireman__readDir(glite_name,true,out)){
			if(cc->soap->fault != NULL)
			debug(D_GLITE,"Failed to do readDir stat: %s - %s",cc->soap->fault->faultcode,cc->soap->fault->faultstring);
			else
			debug(D_GLITE,"Failed to do readDir. Fault returned was NULL");
			errno = EBADF;
			return 0;
		}

		dirob = new pfs_dir(name);
		debug(D_GLITE,"readDir");
		for(int i = 0; i < out._readDirReturn->__size; i++) {
			dirob->append(out._readDirReturn->__ptr[i]->lfn);
		}
		return dirob;
	}

	virtual int stat( pfs_name *name, struct stat *buf ) {
				fireman::FiremanCatalogSoapBinding *cc = getCatalog();
			if (! cc ) {
			errno = EFAULT;
			return -1;
		}

				char glite_name[PFS_PATH_MAX];
		convert_cc_name( name, glite_name );
		debug(D_GLITE,"stat: %s",glite_name);
				fireman::ArrayOf_USCOREsoapenc_USCOREstring l;
		fireman::fireman__getLfnStatResponse out;

		l.__size = 1;
		l.__ptr = (char **) soap_malloc(cc->soap,sizeof(char*));
		l.__ptr[0] = soap_strdup(cc->soap,glite_name);

		debug(D_GLITE,"calling getLfnStat(%s)",l.__ptr[0]);
		if(SOAP_OK != cc->fireman__getLfnStat(&l,out)){
			errno = ENOENT;
			if(cc->soap == 0)
			debug(D_GLITE,"soap struct NULL in fireman catalog");
			else if(cc->soap->fault == 0)
			debug(D_GLITE,"Failed to get LFN stat - NULL fault object");
			else {
			debug(D_GLITE,"Failed to get LFN stat: %s - %s",cc->soap->fault->faultcode,cc->soap->fault->faultstring);
			if(strncmp("Connection refused",cc->soap->fault->faultstring,18)==0)
			   errno = EFAULT;
			}
			return -1;
		}

		pfs_service_emulate_stat(name,buf);
		buf->st_ctime= out._getLfnStatReturn->__ptr[0]->lfnStat->creationTime;
		buf->st_mtime= out._getLfnStatReturn->__ptr[0]->lfnStat->modifyTime;
		buf->st_size= out._getLfnStatReturn->__ptr[0]->lfnStat->size;

		int type = buf->st_mtime= out._getLfnStatReturn->__ptr[0]->lfnStat->type;
		switch(type) {
		case 0: 		    // normal file
			buf->st_mode = S_IFREG;
			break;
		case 1:		    	    // directory
			buf->st_mode = S_IFDIR;
			break;
		case 2:		    	    // symlink
			buf->st_mode = S_IFLNK;
			break;
		default: // what else?
			errno = EFAULT;
			return -1;
		}

		if( out._getLfnStatReturn->__ptr[0]->permission->userPerm->read )
			buf->st_mode |= S_IRUSR;
		if( out._getLfnStatReturn->__ptr[0]->permission->userPerm->write )
			buf->st_mode |= S_IWUSR;
		if( out._getLfnStatReturn->__ptr[0]->permission->userPerm->execute )
			buf->st_mode |= S_IXUSR;

		if( out._getLfnStatReturn->__ptr[0]->permission->groupPerm->read )
			buf->st_mode |= S_IRGRP;
		if( out._getLfnStatReturn->__ptr[0]->permission->groupPerm->write )
			buf->st_mode |= S_IWGRP;
		if( out._getLfnStatReturn->__ptr[0]->permission->groupPerm->execute )
			buf->st_mode |= S_IXGRP;

		if( out._getLfnStatReturn->__ptr[0]->permission->otherPerm->read )
			buf->st_mode |= S_IROTH;
		if( out._getLfnStatReturn->__ptr[0]->permission->otherPerm->write )
			buf->st_mode |= S_IWOTH;
		if( out._getLfnStatReturn->__ptr[0]->permission->otherPerm->execute )
			buf->st_mode |= S_IXOTH;

		debug(D_GLITE,"got stat for %s - guid = %s",glite_name,out._getLfnStatReturn->__ptr[0]->guid);
		return 0;
	}

	virtual int lstat( pfs_name *name, struct pfs_stat *buf ) {
		return this->stat(name,buf);
	}

	virtual int unlink( pfs_name *name ) {
				fireman::FiremanCatalogSoapBinding *cc = getCatalog();
			if (! cc ) {
			errno = EFAULT;
			return -1;
		}

				char glite_name[PFS_PATH_MAX];
		convert_cc_name( name, glite_name );
				fireman::ArrayOf_USCOREsoapenc_USCOREstring l;
		fireman::fireman__removeResponse out;
		debug(D_GLITE,"unlink: %s",glite_name);

		char *tmp[1];
		tmp[0] = glite_name;
		l.__size = 1;
		l.__ptr = tmp;
		if(SOAP_OK != cc->fireman__remove(&l,out)){
			debug(D_GLITE,"Failed to remove LFN: %s - %s",cc->soap->fault->faultcode,cc->soap->fault->faultstring);
			errno = EFAULT;
			return -1;
		}
		return 0;
	}


	virtual int mkdir( pfs_name *name, mode_t mode ) {
				fireman::FiremanCatalogSoapBinding *cc = getCatalog();
			if (! cc ) {
			errno = EFAULT;
			return -1;
		}

				char glite_name[PFS_PATH_MAX];
		convert_cc_name( name, glite_name );

		debug(D_GLITE,"mkdir: %s",glite_name);
				fireman::ArrayOf_USCOREsoapenc_USCOREstring l;
		fireman::fireman__mkdirResponse out;
		debug(D_GLITE,"mkdir: %s",glite_name);

		char *tmp[1];
		tmp[0] = glite_name;
		l.__size = 1;
		l.__ptr = tmp;
		if(SOAP_OK != cc->fireman__mkdir(&l,false,out)){
			// TODO handle errors once they're coming
			errno = ENOENT;
			if(cc->soap == 0)
			debug(D_GLITE,"soap struct NULL in fireman catalog");
			else if(cc->soap->fault == 0)
			debug(D_GLITE,"Failed to get LFN stat - NULL fault object");
			else {
			debug(D_GLITE,"Failed to mkdir: %s - %s",cc->soap->fault->faultcode,cc->soap->fault->faultstring);
			if(strncmp("Connection refused",cc->soap->fault->faultstring,18)==0)
			   errno = EFAULT;
			}
			return -1;
		}
		return 0;
	}

	virtual int rmdir( pfs_name *name ) {
				fireman::FiremanCatalogSoapBinding *cc = getCatalog();
			if (! cc ) {
			errno = EFAULT;
			return -1;
		}

				char glite_name[PFS_PATH_MAX];
		convert_cc_name( name, glite_name );

				fireman::ArrayOf_USCOREsoapenc_USCOREstring l;
		fireman::fireman__rmdirResponse out;
		debug(D_GLITE,"rmdir: %s",glite_name);

		char *tmp[1];
		tmp[0] = glite_name;
		l.__size = 1;
		l.__ptr = tmp;
		if(SOAP_OK != cc->fireman__rmdir(&l,false,out)){
			debug(D_GLITE,"Failed to rmdir: %s - %s",cc->soap->fault->faultcode,cc->soap->fault->faultstring);
			errno = ENOENT;
			return -1;
		}
		return 0;
	}

	virtual int rename( pfs_name *name, pfs_name *newname ) {
				fireman::FiremanCatalogSoapBinding *cc = getCatalog();
			if (! cc ) {
			errno = EFAULT;
			return -1;
		}

		char nname[PFS_PATH_MAX];
				char glite_name[PFS_PATH_MAX];
		convert_cc_name( name, glite_name );
		convert_cc_name( newname, nname );
				fireman::ArrayOf_USCOREtns1_USCOREStringPair l;
		fireman::fireman__mvResponse out;
		debug(D_GLITE,"rename: %s  to  %s",glite_name,nname);


		l.__size = 1;
		l.__ptr = (fireman::glite__StringPair **) soap_malloc(cc->soap,sizeof(fireman::glite__StringPair*));
		fireman::glite__StringPair sp;
		sp.string1 = glite_name;
		sp.string2 = nname;
		l.__ptr[0] = &sp;

		if(SOAP_OK != cc->fireman__mv(&l,out)){
			debug(D_GLITE,"Failed to rename: %s - %s",cc->soap->fault->faultcode,cc->soap->fault->faultstring);
			errno = ENOENT;
			return -1;
		}
		return 0;
	}

	virtual int chdir( pfs_name *name, char *newpath ) {
		int result;
		struct pfs_stat buf;
		result = this->stat(name,&buf);
		if(result>=0) {
			if(S_ISDIR(buf.st_mode)) {
				if(buf.st_mode & S_IXUSR ) {
					strcpy(newpath,name->path);
				} else {
					result = -1;
					errno = EACCES;
				}
			} else {
				result = -1;
				errno = ENOTDIR;
			}
		}
		return result;
	}

	virtual int get_default_port() {
		return 0;
	}

	virtual int is_seekable() {
		return 1;
	}

};

static pfs_service_glite pfs_service_glite_instance;
pfs_service *pfs_service_glite = &pfs_service_glite_instance;


#endif

/* vim: set noexpandtab tabstop=8: */
