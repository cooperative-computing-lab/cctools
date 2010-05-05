//    Copyright (c) 2009, Christopher James Huff <cjameshuff@gmail.com>
//    All rights reserved.
//
//    Redistribution and use in source and binary forms, with or without
//    modification, are permitted provided that the following conditions are met:
//        * Redistributions of source code must retain the above copyright
//          notice, this list of conditions and the following disclaimer.
//        * Redistributions in binary form must reproduce the above copyright
//          notice, this list of conditions and the following disclaimer in the
//          documentation and/or other materials provided with the distribution.
//        * Neither the name of the <organization> nor the
//          names of its contributors may be used to endorse or promote products
//          derived from this software without specific prior written permission.
//
//    THIS SOFTWARE IS PROVIDED BY <copyright holder> ''AS IS'' AND ANY
//    EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
//    WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
//    DISCLAIMED. IN NO EVENT SHALL <copyright holder> BE LIABLE FOR ANY
//    DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
//    (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
//    LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
//    ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
//    (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
//    SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

#include "aws_s3.h"
#include "aws_s3_misc.h"
#include "multidict.h"

#include <cmath>

#include <exception>
#include <stdexcept>

#include <set>

#include <unistd.h>
#include <pwd.h>

using namespace std;
// TODO: better error handling, retries, etc.
// TODO: tests
// TODO: documentation
// TODO: --version, --help
// TODO: option for "don't copy metadata" for cp
// TODO: force bucket delete...clear out contents, then delete
// TODO: parse bucket name from path, allow BUCKET_NAME/OBJECT_KEY


// Thrown due to connection failure, etc...operation was valid, may be retried.
struct Recoverable_Error: public std::runtime_error {
	Recoverable_Error(const std::string & msg = ""): std::runtime_error(msg) {}
};

// Fatal error, can not be retried.
struct Fatal_Error: public std::runtime_error {
	Fatal_Error(const std::string & msg = ""): std::runtime_error(msg) {}
};

string HumanSize(size_t size)
{
    std::ostringstream strm;
    if(size > 1024*1024*1024) {
        double s = (double)size/(1024*1024*1024);
        //floor(log10(s)) + 1 is number of digits
        strm.precision(floor(log10(s)) + 3);
        strm << s << " GB";
    }
    else if(size > 1024) {
        double s = (double)size/(1024*1024);
        strm.precision(floor(log10(s)) + 3);
        strm << s << " MB";
    }
    else if(size > 1024) {
        double s = (double)size/(1024);
        strm.precision(floor(log10(s)) + 3);
        strm << s << " KB";
    }
    else strm << size << " B";
    return strm.str();
}

void PrintObject(const AWS_S3_Object & object, bool longFormat = false)
{
    if(longFormat)
    {
        cout << object.key << endl;
        cout << "  Last modified: " << object.lastModified << endl;
        cout << "  eTag: " << object.eTag << endl;
        cout << "  Size: " << HumanSize(object.GetSize()) << endl;
        cout << "  OwnerID: " << object.ownerID << endl;
        cout << "  OwnerName: " << object.ownerDisplayName << endl;
        cout << "  Storage class: " << object.storageClass << endl;
    }
    else
    {
        cout << object.key;
        cout << " " << object.lastModified;
        cout << ", " << HumanSize(object.GetSize());
        cout << " " << object.ownerDisplayName;
        cout << " " << object.storageClass;
    }
}

void PrintBucket(const AWS_S3_Bucket & bucket)
{
    cout << "Bucket: " << bucket.name << endl;
    list<AWS_S3_Object>::const_iterator obj;
    for(obj = bucket.objects.begin(); obj != bucket.objects.end(); ++obj) {
        cout << "  ";
        PrintObject(*obj);
        cout << endl;
    }
}

struct CommandLine {
    AWS_MultiDict opts;
    std::vector<string> words;
    
    // flags that have parameter values
    // -X value or -Xvalue, where -x is a flag from this set.
    // Flags that have parameter values must always take that value, defaults are not
    // supported.
    std::set<string> flagParams;
    
    bool FlagSet(const string & flag) const {return opts.Exists(flag);}
    
    void Parse(int argc, char * argv[])
    {
        int j = 0;
        while(j < argc)
        {
            if(argv[j][0] == '-') {
                string flag = string(argv[j], 0, 2), value;
                if(flagParams.find(flag) != flagParams.end()) {
                    // is flag with parameter
                    if(strlen(argv[j]) == 2 && argc >= j+1)
                        opts.Insert(flag, argv[++j]);// value is next string, insert and skip
                    else// value is part of string, cut out
                        opts.Insert(flag, string(argv[j], 2, strlen(argv[j]) - 2));
                }
                else {
                    // is plain flag
                    opts.Insert(flag, "");
                }
            }
            else {
                words.push_back(argv[j]);
            }
            ++j;
        }
    }
};

void LoadMetadata(AWS_IO & io, const CommandLine & cmdln);
int Command_s3genidx(size_t wordc, CommandLine & cmds, AWS & aws);

typedef int (*Command)(size_t wordc, CommandLine & cmds, AWS & aws);
static std::map<string, Command> commands;
void InitCommands();
void PrintUsage();

static int verbosity = 1;

int main(int argc, char * argv[])
{
    InitMimeTypes();
    InitCommands();
    
    CommandLine cmds;
    cmds.flagParams.insert("-v");// verbosity level
    cmds.flagParams.insert("-c");// credentials file
    cmds.flagParams.insert("-p");// permissions (canned ACL)
    cmds.flagParams.insert("-t");// type (Content-Type)
    cmds.flagParams.insert("-m");// metadata
    cmds.Parse(argc, argv);
    size_t wordc = cmds.words.size();
    
    string keyID, secret, name;
    
    if(cmds.FlagSet("-v")) {
        verbosity = cmds.opts.GetWithDefault("-v", 2);
        if(verbosity > 0)
            cout << "Verbose output level " << verbosity << endl;
    }
    
    if(cmds.FlagSet("-c")) {
        string credFile = cmds.opts.GetWithDefault("-c", "");
        ifstream cred(credFile.c_str());
        if(cred) {
            getline(cred, keyID);
            getline(cred, secret);
            getline(cred, name);
            if(verbosity >= 1)
                cout << "using credentials from " << credFile << ", name: " << name << endl;
        }
        else {
            cerr << "Error: Could not load specified credentials file: " << credFile << endl;
            cerr << "Credentials file should consist of three lines: key ID, secret key, and a name" << endl;
            return EXIT_FAILURE;
        }
    }
    else {
        // credentials not specified. Try pwd/.credentials
        char pwd[PATH_MAX];
        getcwd(pwd, PATH_MAX);
        string localCred(string(pwd) + "/.s3_credentials");
        
        struct passwd * ent = getpwnam(getlogin());
        string userCred(string(ent->pw_dir) + "/.s3_credentials");
        
        ifstream cred;
        
        // Try ${PWD}/.credentials first
        string credFile = localCred;
        cred.open(credFile.c_str());
        
        // Try ~/.s3_credentials
        if(!cred) {
            cred.clear();
            credFile = userCred;
            cred.open(credFile.c_str());
        }
        
        if(cred) {
            getline(cred, keyID);
            getline(cred, secret);
            getline(cred, name);
            if(verbosity >= 1)
                cout << "using credentials from " << credFile << ", name: " << name << endl;
        }
        else {
            cerr << "Error: Could not load credentials file." << endl;
            cerr << "Make sure a .s3_credentials file is present in the home directory" << endl;
            cerr << "or in the current directory" << endl;
            cerr << "Credentials file should consist of three lines: key ID, secret key, and a name" << endl;
            return EXIT_FAILURE;
        }
    }
    
    // Create and configure AWS instance
    AWS aws(keyID, secret);
    aws.SetVerbosity(verbosity);
    
    // Remove executable name if called directly with commands, otherwise show usage
    // If symlinked, use the executable name to determine the desired operation
    // Trim to just command name
    cmds.words[0] = cmds.words[0].substr(cmds.words[0].find_last_of('/') + 1);
    if(cmds.words[0] == "s3tool") {
        if(cmds.words.size() < 2) {
            PrintUsage();
            return EXIT_SUCCESS;
        }
        cmds.words.erase(cmds.words.begin());
        --wordc;
    }
    
    // Perform command
    int result = EXIT_SUCCESS;
    if(commands.find(cmds.words[0]) != commands.end())
    {
        try {
            result = commands[cmds.words[0]](wordc, cmds, aws);
        }
        //catch(Recoverable_Error & err) {
        catch(std::runtime_error & err) {
            cerr << "ERROR: " << err.what() << endl;
            return EXIT_FAILURE;
        }
        
        // Regenerate index file?
        // TODO: add more checking here...or copy to all the commands that need it.
        if(wordc >= 2 && cmds.FlagSet("-i")) {
            Command_s3genidx(wordc, cmds, aws);
        }
    }
    else {
        cerr << "Did not understand command \"" << cmds.words[0] << "\"" << endl;
        return EXIT_FAILURE;
    }
    
    return result;
}

//TODO: get metadata from standard input
void LoadMetadata(AWS_IO & io, const CommandLine & cmdln)
{
    std::pair<AWS_MultiDict::const_iterator, AWS_MultiDict::const_iterator> range;
    AWS_MultiDict::const_iterator md;
    range = cmdln.opts.equal_range("-m");
    for(md = range.first; md != range.second; ++md) {
        string entry = md->second;
        string::size_type colon = entry.find_first_of(':');
        string::size_type valueStart = entry.find_first_not_of(' ', colon + 1);
        if(colon != string::npos) {
            string header(entry.substr(0, colon));
            string data(entry.substr(valueStart, entry.length() - valueStart));
            io.sendHeaders.Set(header, data);
            if(verbosity >= 2)
                cout << header << ": " << data << endl;
        }
        else {
            throw Fatal_Error("Bad metadata format");
        }
    }
}

//MARK: s3install
int Command_s3install(size_t wordc, CommandLine & cmds, AWS & aws)
{
    char pwd[PATH_MAX];
    getcwd(pwd, PATH_MAX);
    std::ostringstream cmdstrm;
    cmdstrm << "ln -s " << pwd << "/s3tool s3ls";
    cmdstrm << " && ln -s " << pwd << "/s3tool s3put";
    cmdstrm << " && ln -s " << pwd << "/s3tool s3wput";
    cmdstrm << " && ln -s " << pwd << "/s3tool s3get";
    cmdstrm << " && ln -s " << pwd << "/s3tool s3getmeta";
    cmdstrm << " && ln -s " << pwd << "/s3tool s3putmeta";
    cmdstrm << " && ln -s " << pwd << "/s3tool s3mv";
    cmdstrm << " && ln -s " << pwd << "/s3tool s3cp";
    cmdstrm << " && ln -s " << pwd << "/s3tool s3rm";
    cmdstrm << " && ln -s " << pwd << "/s3tool s3mkbkt";
    cmdstrm << " && ln -s " << pwd << "/s3tool s3rmbkt";
    cmdstrm << " && ln -s " << pwd << "/s3tool s3setacl";
    cmdstrm << " && ln -s " << pwd << "/s3tool s3getacl";
    cmdstrm << " && ln -s " << pwd << "/s3tool s3genidx";
    cout << cmdstrm.str() << endl;
    return system(cmdstrm.str().c_str());
}


//MARK: ls
void PrintUsage_s3ls() {
    cout << "List all buckets:" << endl;
    cout << "\ts3tool ls" << endl;
    cout << "List contents of bucket or object from bucket:" << endl;
    cout << "\ts3tool ls BUCKET_NAME [OBJECT_KEY]" << endl;
    cout << "alias s3ls" << endl;
    cout << endl;
}

int Command_s3ls(size_t wordc, CommandLine & cmds, AWS & aws)
{
    if(wordc == 1) {
        // list all buckets
        AWS_Connection * conn = NULL;
        list<AWS_S3_Bucket> & buckets = aws.GetBuckets(false, true, &conn);
        list<AWS_S3_Bucket>::iterator bkt;
        for(bkt = buckets.begin(); bkt != buckets.end(); ++bkt) {
            aws.GetBucketContents(*bkt, &conn);
            PrintBucket(*bkt);
        }
        delete conn;
    }
    else if(wordc == 2) {
        // list specific bucket
        AWS_S3_Bucket bucket(cmds.words[1], "");
        aws.GetBucketContents(bucket);
        PrintBucket(bucket);
    }
    else if(wordc == 3)
    {
        // List specific object in bucket
        AWS_S3_Bucket bucket(cmds.words[1], "");
        aws.GetBucketContents(bucket);
        list<AWS_S3_Object>::const_iterator obj;
        for(obj = bucket.objects.begin(); obj != bucket.objects.end(); ++obj) {
            if(obj->key == cmds.words[2]) {
                PrintObject(*obj, true);//long format, all details
                cout << endl;
                break;
            }
        }
    }
    else {
        PrintUsage_s3ls();
    }
    return EXIT_SUCCESS;
}

//MARK: put
void PrintUsage_s3put() {
    cout << "Upload file to S3:" << endl;
    cout << "\ts3tool put BUCKET_NAME OBJECT_KEY [FILE_PATH] [-pPERMISSION] [-tTYPE] -mMETADATA" << endl;
    cout << "PERMISSION: a canned ACL:\n"
         << "\tprivate, public-read, public-read-write, or authenticated-read" << endl;
    cout << "TYPE: a MIME content-type" << endl;
    cout << "METADATA: a HTML header and data string, multiple metadata may be specified" << endl;
    cout << "\"s3wput\" can be used as a shortcut for \"s3put -ppublic-read\"" << endl;
    cout << endl;
}

int Command_s3put(size_t wordc, CommandLine & cmds, AWS & aws)
{
    if(wordc == 3 || wordc == 4) {
        AWS_IO io;
        string bucketName = cmds.words[1];
        string objectKey = cmds.words[2];
        string filePath = (wordc == 4)? cmds.words[3] : objectKey;
        string acl;
        
        if((cmds.words[0] == "wput") || (cmds.words[0] == "s3wput"))
            acl = "public-read";
        
        LoadMetadata(io, cmds);
        
        cmds.opts.Get("-p", acl);//get acl if specified
        
        if(cmds.opts.Exists("-t"))
            io.sendHeaders.Set("Content-Type", cmds.opts.GetWithDefault("-t", ""));
        else
            io.sendHeaders.Set("Content-Type", MatchMimeType(filePath));
        
        io.ostrm = &cout;
        io.printProgress = true;
        aws.PutObject(bucketName, objectKey, acl, filePath, io);
        if(io.Failure()) {
            cerr << "ERROR: failed to put object" << endl;
            cerr << "response:\n" << io << endl;
            cerr << "response body:\n" << io.response.str() << endl;
            return EXIT_FAILURE;
        }
    }
    else {
        PrintUsage_s3put();
    }
    return EXIT_SUCCESS;
}

//MARK: get
void PrintUsage_s3get() {
    cout << "Download file from S3:" << endl;
    cout << "\ts3tool get BUCKET_NAME OBJECT_KEY [FILE_PATH]" << endl;
    cout << endl;
}

int Command_s3get(size_t wordc, CommandLine & cmds, AWS & aws)
{
    if(wordc == 3 || wordc == 4) {
        string bucketName = cmds.words[1];
        string objectKey = cmds.words[2];
        string filePath = (wordc == 4)? cmds.words[3] : objectKey;
        ofstream fout(filePath.c_str());
        
        AWS_IO objinfo_io;
        aws.GetObjectMData(bucketName, objectKey, objinfo_io);
        
        AWS_IO io(NULL, &fout);
        io.printProgress = (verbosity >= 1);
        io.bytesToGet = objinfo_io.headers.GetWithDefault("Content-Length", 0);
        aws.GetObject(bucketName, objectKey, io);
        if(io.Failure()) {
            cerr << "ERROR: failed to put object" << endl;
            cerr << "response:\n" << io << endl;
            //TODO: grab response body, delete incorrect local file
            //cerr << "response body:\n" << io.response.str() << endl;
            return EXIT_FAILURE;
        }
    }
    else {
        PrintUsage_s3get();
    }
    return EXIT_SUCCESS;
}

//MARK: getmeta
void PrintUsage_s3getmeta() {
    cout << "Get object metadata:" << endl;
    cout << "\ts3tool getmeta BUCKET_NAME OBJECT_KEY" << endl;
    cout << endl;
}

int Command_s3getmeta(size_t wordc, CommandLine & cmds, AWS & aws)
{
    if(wordc == 3) {
        string bucketName = cmds.words[1];
        string objectKey = cmds.words[2];
        AWS_IO io(NULL, NULL);
        aws.GetObjectMData(bucketName, objectKey, io);
        AWS_MultiDict::iterator i;
        for(i = io.headers.begin(); i != io.headers.end(); ++i)
            cout << i->first << ": " << i->second << endl;
    }
    else {
        PrintUsage_s3getmeta();
    }
    return EXIT_SUCCESS;
}

// MARK: putmeta
void PrintUsage_s3putmeta() {
    cout << "Get object metadata:" << endl;
    cout << "\ts3tool putmeta BUCKET_NAME OBJECT_KEY -tTYPE -mMETA..." << endl;
    cout << endl;
}

int Command_s3putmeta(size_t wordc, CommandLine & cmds, AWS & aws)
{
    // putmeta: copy to temp, delete, copy to original with overrides
    // TODO: read metadata and only override specified items
    if(wordc == 3)
    {
        string bucketName = cmds.words[1];
        string objectKey = cmds.words[2];
        string tmpObjectKey = objectKey + "_putmetatmp";
        AWS_IO io;
        
        // Make copy to temp object, discarding metadata
        aws.CopyObject(bucketName, objectKey, bucketName, tmpObjectKey, false, io);
        if(io.Failure()) {
            cerr << "ERROR: putmeta: failed to make temp copy of object" << endl;
            cerr << "response:\n" << io << endl;
            cerr << "response body:\n" << io.response.str() << endl;
            return EXIT_FAILURE;
        }
        
        // Get ACL of original
        io.Reset();
        string acl = aws.GetACL(bucketName, objectKey, io);
        if(io.Failure()) {
            cerr << "ERROR: putmeta: failed to get ACL." << endl;
            cerr << "response:\n" << io << endl;
            cerr << "response body:\n" << io.response.str() << endl;
            return EXIT_FAILURE;
        }
        
        // Delete original
        io.Reset();
        aws.DeleteObject(bucketName, objectKey, io);
        if(io.Failure()) {
            cerr << "ERROR: putmeta: failed to delete original copy of object" << endl;
            cerr << "response:\n" << io << endl;
            cerr << "response body:\n" << io.response.str() << endl;
            return EXIT_FAILURE;
        }
        
        // Copy temp object back to original, setting metadata
        io.Reset();
        if(cmds.FlagSet("-t"))
            io.sendHeaders.Set("Content-Type", cmds.opts.GetWithDefault("-t", ""));
        LoadMetadata(io, cmds);
        aws.CopyObject(bucketName, tmpObjectKey, bucketName, objectKey, false, io);
        if(io.Failure()) {
            cerr << "ERROR: putmeta: failed to make new copy of object" << endl;
            cerr << "response:\n" << io << endl;
            cerr << "response body:\n" << io.response.str() << endl;
            return EXIT_FAILURE;
        }
        
        // Restore ACL of object
        io.Reset();
        aws.SetACL(bucketName, objectKey, acl, io);
        if(io.Failure()) {
            cerr << "ERROR: putmeta: failed to set ACL" << endl;
            cerr << "response:\n" << io << endl;
            cerr << "response body:\n" << io.response.str() << endl;
            return EXIT_FAILURE;
        }
        
        // Delete copy
        io.Reset();
        aws.DeleteObject(bucketName, tmpObjectKey, io);
        if(io.Failure()) {
            cerr << "ERROR: putmeta: failed to delete temporary copy of object" << endl;
            cerr << "response:\n" << io << endl;
            cerr << "response body:\n" << io.response.str() << endl;
            return EXIT_FAILURE;
        }
    }
    else {
        PrintUsage_s3putmeta();
    }
    return EXIT_SUCCESS;
}

//MARK: cp
void PrintUsage_s3cp() {
    cout << "Copy S3 object:" << endl;
    cout << "\ts3tool cp SRC_BUCKET_NAME SRC_OBJECT_KEY DST_OBJECT_KEY" << endl;
    cout << "\ts3tool cp SRC_BUCKET_NAME SRC_OBJECT_KEY DST_BUCKET_NAME DST_OBJECT_KEY" << endl;
    cout << endl;
}

int Command_s3cp(size_t wordc, CommandLine & cmds, AWS & aws)
{
    if(wordc == 4 || wordc == 5) {
        string srcBucketName = cmds.words[1];
        string srcObjectKey = cmds.words[2];
        string dstBucketName = (wordc == 5)? cmds.words[3] : srcBucketName;
        string dstObjectKey = (wordc == 5)? cmds.words[4] : cmds.words[3];
        AWS_IO io;
        
        bool copyMD = true;
        if(cmds.opts.Exists("-t")) {
            io.sendHeaders.Set("Content-Type", cmds.opts.GetWithDefault("-t", ""));
            copyMD = false;
        }
        LoadMetadata(io, cmds);
        aws.CopyObject(srcBucketName, srcObjectKey, dstBucketName, dstObjectKey, copyMD, io);
        if(io.Failure()) {
            cerr << "ERROR: failed to copy object" << endl;
            cerr << "response:\n" << io << endl;
            cerr << "response body:\n" << io.response.str() << endl;
            return EXIT_FAILURE;
        }
        
        io.Reset();
        string acl = aws.GetACL(srcBucketName, srcObjectKey, io);
        if(io.Failure()) {
            cerr << "ERROR: failed to get ACL. Object was copied, and has default ACL." << endl;
            cerr << "response:\n" << io << endl;
            cerr << "response body:\n" << io.response.str() << endl;
            return EXIT_FAILURE;
        }
        
        io.Reset();
        aws.SetACL(dstBucketName, dstObjectKey, acl, io);
        if(io.Failure()) {
            cerr << "ERROR: failed to set ACL" << endl;
            cerr << "response:\n" << io << endl;
            cerr << "response body:\n" << io.response.str() << endl;
            return EXIT_FAILURE;
        }
    }
    else {
        PrintUsage_s3cp();
    }
    return EXIT_SUCCESS;
}

//MARK: mv
void PrintUsage_s3mv() {
    cout << "Move S3 object:" << endl;
    cout << "\ts3tool mv SRC_BUCKET_NAME SRC_OBJECT_KEY DST_OBJECT_KEY" << endl;
    cout << "\ts3tool mv SRC_BUCKET_NAME SRC_OBJECT_KEY DST_BUCKET_NAME DST_OBJECT_KEY" << endl;
    cout << endl;
}

int Command_s3mv(size_t wordc, CommandLine & cmds, AWS & aws)
{
    if(wordc == 4 || wordc == 5) {
        string srcBucketName = cmds.words[1];
        string srcObjectKey = cmds.words[2];
        string dstBucketName = (wordc == 5)? cmds.words[3] : srcBucketName;
        string dstObjectKey = (wordc == 5)? cmds.words[4] : cmds.words[3];
        AWS_IO io;
        
        bool copyMD = true;
        if(cmds.FlagSet("-t")) {
            io.sendHeaders.Set("Content-Type", cmds.opts.GetWithDefault("-t", ""));
            copyMD = false;
        }
        LoadMetadata(io, cmds);
        aws.CopyObject(srcBucketName, srcObjectKey, dstBucketName, dstObjectKey, copyMD, io);
        if(io.Failure()) {
            cerr << "ERROR: mv: failed to copy object" << endl;
            cerr << "response:\n" << io << endl;
            cerr << "response body:\n" << io.response.str() << endl;
            return EXIT_FAILURE;
        }
        
        io.Reset();
        string acl = aws.GetACL(srcBucketName, srcObjectKey, io);
        if(io.Failure()) {
            cerr << "ERROR: s3mv: failed to get ACL. Object was copied, and has default ACL." << endl;
            cerr << "response:\n" << io << endl;
            cerr << "response body:\n" << io.response.str() << endl;
            return EXIT_FAILURE;
        }
        
        io.Reset();
        aws.SetACL(dstBucketName, dstObjectKey, acl, io);
        if(io.Failure()) {
            cerr << "ERROR: s3mv: failed to set ACL" << endl;
            cerr << "response:\n" << io << endl;
            cerr << "response body:\n" << io.response.str() << endl;
            return EXIT_FAILURE;
        }
        
        io.Reset();
        aws.DeleteObject(srcBucketName, srcObjectKey, io);
        if(io.Failure()) {
            cerr << "ERROR: s3mv: failed to delete old copy of object" << endl;
            cerr << "response:\n" << io << endl;
            cerr << "response body:\n" << io.response.str() << endl;
            return EXIT_FAILURE;
        }
    }
    else {
        PrintUsage_s3mv();
    }
    return EXIT_SUCCESS;
}

//MARK: rm
void PrintUsage_s3rm() {
    cout << "Remove object:" << endl;
    cout << "\ts3 rm BUCKET_NAME OBJECT_KEY" << endl;
    cout << endl;
}

int Command_s3rm(size_t wordc, CommandLine & cmds, AWS & aws)
{
    if(wordc == 3) {
        string bucketName = cmds.words[1];
        string objectKey = cmds.words[2];
        AWS_IO io;
        aws.DeleteObject(bucketName, objectKey, io);
        if(io.Failure()) {
            cerr << "ERROR: s3rm: failed to delete object\n" << io << endl;
            cerr << "response:\n" << io << endl;
            cerr << "response body:\n" << io.response.str() << endl;
            return EXIT_FAILURE;
        }
    }
    else {
        PrintUsage_s3rm();
    }
    return EXIT_SUCCESS;
}

//MARK: mkbkt
void PrintUsage_s3mkbkt() {
    cout << "Create bucket:" << endl;
    cout << "\ts3tool mkbkt BUCKET_NAME" << endl;
    cout << endl;
}

int Command_s3mkbkt(size_t wordc, CommandLine & cmds, AWS & aws)
{
    if(wordc == 2) {
        string bucketName = cmds.words[1];
        AWS_IO io;
        LoadMetadata(io, cmds);
        aws.CreateBucket(bucketName, io);
        if(io.Failure()) {
            cerr << "ERROR: failed to create bucket" << endl;
            cerr << "response:\n" << io << endl;
            cerr << "response body:\n" << io.response.str() << endl;
            return EXIT_FAILURE;
        }
    }
    else {
        PrintUsage_s3mkbkt();
    }
    return EXIT_SUCCESS;
}

//MARK: rmbkt
void PrintUsage_s3rmbkt() {
    cout << "Remove bucket:" << endl;
    cout << "\ts3tool rmbkt BUCKET_NAME" << endl;
    cout << endl;
}

int Command_s3rmbkt(size_t wordc, CommandLine & cmds, AWS & aws)
{
    if(wordc == 2) {
        string bucketName = cmds.words[1];
        AWS_IO io;
        aws.DeleteBucket(bucketName, io);
        if(io.Failure()) {
            cerr << "ERROR: failed to delete object" << endl;
            cerr << "response:\n" << io << endl;
            cerr << "response body:\n" << io.response.str() << endl;
            return EXIT_FAILURE;
        }
    }
    else {
        PrintUsage_s3rmbkt();
    }
    return EXIT_SUCCESS;
}

//MARK: setacl, setbktacl
void PrintUsage_s3setacl() {
    // Set access to bucket or object
    cout << "Set access to bucket or object with canned ACL:" << endl;
    cout << "\ttool setbktacl BUCKET_NAME PERMISSION" << endl;
    cout << "\ttool setacl BUCKET_NAME OBJECT_KEY PERMISSION" << endl;
    cout << "where PERMISSION is a canned ACL:\n"
         << "\tprivate, public-read, public-read-write, or authenticated-read" << endl;
    cout << "Set access to bucket or object with full ACL:" << endl;
    cout << "\ttool setbktacl BUCKET_NAME" << endl;
    cout << "\ttool setacl BUCKET_NAME OBJECT_KEY\n"
         << "\tWith ACL definition piped to STDIN." << endl;
    cout << endl;
}

int Command_s3setbktacl(size_t wordc, CommandLine & cmds, AWS & aws)
{
    if(wordc == 2) {// set full acl (via stdin) on bucket
        string bucketName = cmds.words[1];
        istreambuf_iterator<char> begin(cin), end;
        string acl(begin, end);
        AWS_IO io;
        aws.SetACL(bucketName, acl, io);
        if(io.Failure()) {
            cerr << "ERROR: failed to set bucket ACL" << endl;
            cerr << "response:\n" << io << endl;
            cerr << "response body:\n" << io.response.str() << endl;
            return EXIT_FAILURE;
        }
    }
    else if(wordc == 3) {// set canned acl on bucket
        string bucketName = cmds.words[1];
        string acl = cmds.words[2];
        AWS_IO io;
        aws.SetCannedACL(bucketName, acl, io);
        if(io.Failure()) {
            cerr << "ERROR: failed to set bucket ACL" << endl;
            cerr << "response:\n" << io << endl;
            cerr << "response body:\n" << io.response.str() << endl;
            return EXIT_FAILURE;
        }
    }
    else {
        PrintUsage_s3setacl();
    }
    return EXIT_SUCCESS;
}

int Command_s3setacl(size_t wordc, CommandLine & cmds, AWS & aws)
{
    if(wordc == 3) {// set full acl (via stdin) on object
        string bucketName = cmds.words[1];
        string objectKey = cmds.words[2];
        istreambuf_iterator<char> begin(cin), end;
        string acl(begin, end);
        AWS_IO io;
        aws.SetACL(bucketName, objectKey, acl, io);
        if(io.Failure()) {
            cerr << "ERROR: failed to set object ACL" << endl;
            cerr << "response:\n" << io << endl;
            cerr << "response body:\n" << io.response.str() << endl;
            return EXIT_FAILURE;
        }
    }
    else if(wordc == 4) {//set canned acl on object
        string bucketName = cmds.words[1];
        string objectKey = cmds.words[2];
        string acl = cmds.words[3];
        AWS_IO io;
        aws.SetCannedACL(bucketName, objectKey, acl, io);
        if(io.Failure()) {
            cerr << "ERROR: failed to set object ACL" << endl;
            cerr << "response:\n" << io << endl;
            cerr << "response body:\n" << io.response.str() << endl;
            return EXIT_FAILURE;
        }
    }
    else {
        PrintUsage_s3setacl();
    }
    return EXIT_SUCCESS;
}

//MARK: getacl
void PrintUsage_s3getacl() {
    cout << "Get ACL for bucket or object:" << endl;
    cout << "\ttool getacl BUCKET_NAME [OBJECT_KEY]" << endl;
}

int Command_s3getacl(size_t wordc, CommandLine & cmds, AWS & aws)
{
    AWS_IO io;
    if(wordc == 2)
        cout << aws.GetACL(cmds.words[1], io) << endl;
    else if(wordc == 3)
        cout << aws.GetACL(cmds.words[1], cmds.words[2], io) << endl;
    else PrintUsage_s3getacl();
    return EXIT_SUCCESS;
}

//MARK: genidx
void PrintUsage_s3genidx() {
    cout << "Generate index for public-readable items in bucket:" << endl;
    cout << "\ttool genidx BUCKET_NAME" << endl;
}

int Command_s3genidx(size_t wordc, CommandLine & cmds, AWS & aws)
{
    string bucketName = cmds.words[1];
    
    AWS_S3_Bucket bucket(bucketName, "");
    aws.GetBucketContents(bucket);
    cout << "Generating index for bucket:" << bucket.name << endl;
    std::ostringstream strm;
    strm << "<!DOCTYPE HTML PUBLIC \"-//W3C//DTD HTML 3.2 Final//EN\">\n";
    strm << "<html>\n";
    strm << " <head>\n";
    strm << "  <title>Index of " << bucket.name << "</title>\n";
    strm << " </head>\n";
    strm << " <body>\n";
    strm << "<h1>Index of " << bucket.name << "</h1>\n";
    
    strm << "<table>\n";
    strm << "<tr><th>Name</th><th>Last modified</th><th>Size</th><th>eTag</th></tr>\n";
    strm << "<tr><th colspan=\"4\"><hr></th></tr>\n";
    list<AWS_S3_Object>::const_iterator obj;
    for(obj = bucket.objects.begin(); obj != bucket.objects.end(); ++obj) {
        // TODO: check permissions on object and only list public-readable objects
        strm << "<tr>";
        strm << "<td><a href=\"http://" << bucket.name << "/" << obj->key << "\">"
             << obj->key << "</a></td>";
        strm << "<td>" << obj->lastModified << "</td>";
        strm << "<td>" << HumanSize(obj->GetSize()) << "</td>";
        strm << "<td>" << obj->eTag << "</td>";
        strm << "</tr>\n";
    }
    strm << "</table>\n";
    strm << "</body>\n";
    strm << "</html>" << endl;
    
    // Upload index
    //cout << strm.str() << endl;
    std::istringstream istrm(strm.str());
    AWS_IO io;
    io.ostrm = &cout;
    io.istrm = &istrm;
    io.printProgress = true;
    io.sendHeaders.Set("Content-Type", "text/html");
    aws.PutObject(bucketName, "index.html", "public-read", io);
    if(io.Failure()) {
        cerr << "ERROR: failed to put index object" << endl;
        cerr << "response:\n" << io << endl;
        cerr << "response body:\n" << io.response.str() << endl;
        return EXIT_FAILURE;
    }
    return EXIT_SUCCESS;
}

//MARK: md5
int Command_s3md5(size_t wordc, CommandLine & cmds, AWS & aws)
{
    ifstream fin(cmds.words[1].c_str(), ios_base::binary | ios_base::in);
    uint8_t md5[EVP_MAX_MD_SIZE];
    size_t mdLen = ComputeMD5(md5, fin);
    cout << "md5: \"" << EncodeB64(md5, mdLen) << "\"" << endl;
    return EXIT_SUCCESS;
}

//MARK: mime
int Command_s3mime(size_t wordc, CommandLine & cmds, AWS & aws)
{
    cout << "Content-Type: \"" << MatchMimeType(cmds.words[1]) << "\"" << endl;
    return EXIT_SUCCESS;
}

void InitCommands()
{
    commands["install"] = Command_s3install;
    
    commands["s3ls"] = Command_s3ls;
    commands["ls"] = Command_s3ls;
    
    commands["s3wput"] = Command_s3put;
    commands["s3put"] = Command_s3put;
    commands["wput"] = Command_s3put;
    commands["put"] = Command_s3put;
    
    commands["s3get"] = Command_s3get;
    commands["get"] = Command_s3get;
    
    commands["s3getmeta"] = Command_s3getmeta;
    commands["getmeta"] = Command_s3getmeta;
    
    commands["s3putmeta"] = Command_s3putmeta;
    commands["putmeta"] = Command_s3putmeta;
    
    commands["s3cp"] = Command_s3cp;
    commands["cp"] = Command_s3cp;
    commands["s3mv"] = Command_s3mv;
    commands["mv"] = Command_s3mv;
    commands["s3rm"] = Command_s3rm;
    commands["rm"] = Command_s3rm;
    
    commands["s3mkbkt"] = Command_s3mkbkt;
    commands["mkbkt"] = Command_s3mkbkt;
    
    commands["s3rmbkt"] = Command_s3rmbkt;
    commands["rmbkt"] = Command_s3rmbkt;
    
    commands["s3setbktacl"] = Command_s3setbktacl;
    commands["setbktacl"] = Command_s3setbktacl;
    
    commands["s3setacl"] = Command_s3setacl;
    commands["setacl"] = Command_s3setacl;
    
    commands["s3getacl"] = Command_s3getacl;
    commands["getacl"] = Command_s3getacl;
    
    commands["s3genidx"] = Command_s3genidx;
    commands["genidx"] = Command_s3genidx;
    
    commands["md5"] = Command_s3md5;
    commands["mime"] = Command_s3mime;
}

void PrintUsage() {
    cout << "Usage:" << endl;
    PrintUsage_s3ls();
    PrintUsage_s3put();
    PrintUsage_s3get();
    PrintUsage_s3getmeta();
    PrintUsage_s3putmeta();
    PrintUsage_s3mv();
    PrintUsage_s3cp();
    PrintUsage_s3rm();
    PrintUsage_s3mkbkt();
    PrintUsage_s3rmbkt();
    PrintUsage_s3setacl();
    PrintUsage_s3getacl();
    PrintUsage_s3genidx();
}
