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

#ifndef AWS_S3_H
#define AWS_S3_H

#include <iostream>
#include <fstream>
#include <string>
#include <vector>
#include <list>
#include <map>
#include <sstream>

#include <curlpp/Easy.hpp>
#include "multidict.h"

typedef cURLpp::Easy AWS_Connection;


// AWS_IO objects specify data and headers to send,
// and collect the data and headers of the response.
// TODO: this has grown a bit...make a class, add accessors.
// 
// Commonly used headers:
// x-amz-meta-*: 
// Content-Type: 
// Content-Disposition: 
// Content-Encoding: 
// Expires: 
// Cache-Control: 
// 
// Content-MD5 is computed by AWS::PutObject().
// Content-Size for PUT operations is set by libcurl using bytesToPut. Do not specify as a header.
struct AWS_IO {
    std::string httpDate;// Timestamp, set by AWS::Send()
    AWS_MultiDict sendHeaders;// Headers for request
    
    std::string result;// Result code for response, minus the leading "HTTP/1.1"
    int numResult;// numeric result code for response
    AWS_MultiDict headers;// Headers from response
    
    std::ostringstream response;// default output stream, contains body of response
    std::istream * istrm;
    std::ostream * ostrm;
    
    size_t bytesToGet;// used only for progress reporting
    size_t bytesReceived;
    size_t bytesToPut;
    size_t bytesSent;
    
    bool printProgress;
    bool error;
    
    AWS_IO() {Reset();}
    AWS_IO(std::istream * i) {Reset(i, NULL);}
    AWS_IO(std::ostream * o) {Reset(NULL, o);}
    AWS_IO(std::istream * i, std::ostream * o) {Reset(i, o);}
    
    void Reset(std::istream * i = NULL, std::ostream * o = NULL) {
        sendHeaders.Clear();
        headers.Clear();
        response.clear();
        httpDate = "";
        result = "";
        numResult = 0;
        istrm = NULL;
        ostrm = (o == NULL)? &response : o;
        bytesToGet = 0; bytesReceived = 0;
        bytesToPut = 0; bytesSent = 0;
        printProgress = false;
        error = false;
    }
    
    //"200 OK", or some other 20x message
    bool Success() const {return result[0] == '2' && !error;}
    bool Failure() const {return !Success();}
    
    // Called prior to performing action
    virtual void WillStart();
    
    // Called after action is complete
    virtual void DidFinish();
    
    // Handler for data received by libcurl
    virtual size_t Write(char * buf, size_t size, size_t nmemb);
    
    // Handler for data requested by libcurl for transmission
    virtual size_t Read(char * buf, size_t size, size_t nmemb);
    
    // Handler for headers: overrides must call if other functionality of
    // AWS_IO is to be used.
    virtual size_t HandleHeader(char * buf, size_t size, size_t nmemb);
    
    friend std::ostream & operator<<(std::ostream & ostrm, AWS_IO & io);
};

// Instances of this class represent objects stored on Amazon S3.
struct AWS_S3_Object {
    std::string key;
    std::string lastModified;
    std::string eTag;
    std::string size;
    
    std::string ownerID;
    std::string ownerDisplayName;
    
    std::string storageClass;
    
    AWS_S3_Object() {}
    
    size_t GetSize() const {return strtol(size.c_str(), NULL, 0);}
};

// Instances of this class represent buckets on Amazon S3.
struct AWS_S3_Bucket {
    std::string name;
    std::string creationDate;
    
    std::list<AWS_S3_Object> objects;
    //TODO: object map
//    std::map<std::string, AWS_S3_Object *> objects;
    
    AWS_S3_Bucket(const std::string & nm, const std::string & dt): name(nm), creationDate(dt) {}
};

class AWS {
    std::string keyID, secret;
    int verbosity;
    std::list<AWS_S3_Bucket> buckets;
    
    std::string GenRequestSignature(const AWS_IO & io, const std::string & uri, const std::string & mthd);
    
    void Send(const std::string & url, const std::string & uri,
              const std::string & method, AWS_IO & io, AWS_Connection ** conn);
    
    
    static void ParseBucketsList(std::list<AWS_S3_Bucket> & buckets, const std::string & xml);
    static void ParseObjectsList(std::list<AWS_S3_Object> & objects, const std::string & xml);
    
  public:
    AWS(const std::string & kid, const std::string & sk);
    ~AWS();
    
    void SetVerbosity(int v) {verbosity = v;}
    
    std::list<AWS_S3_Bucket> & GetBuckets(bool getContents, bool refresh,
                                          AWS_Connection ** conn = NULL);
    void RefreshBuckets(bool getContents, AWS_Connection ** conn = NULL);
    
    void GetBucketContents(AWS_S3_Bucket & bucket, AWS_Connection ** conn = NULL);
    
//    void GetObjectInfo(std::string & bktName, std::string & key,
//                       AWS_S3_Object & bucket, AWS_Connection ** conn = NULL) {
//        AWS_S3_Bucket bucket(bktName, "");
//        aws.GetBucketContents(bucket);
//    }
    
    // To perform multiple operations on the same connection, provide a pointer to
    // a pointer to an AWS_Connection as the last parameter, initialized to NULL:
    // AWS_Connection * conn = NULL;
    // PutObject("bucket", "key", io, &conn);
    // PutObject("bucket", "key2", io, &conn);
    // The first operation will create the AWS_Connection. The user should delete
    // the connection themselves after they are done.
    
    // Upload object
    void PutObject(const std::string & bkt, const std::string & key, const std::string & acl,
                   AWS_IO & io, AWS_Connection ** reqPtr = NULL);
    void PutObject(const std::string & bkt, const std::string & key,
                   const std::string & acl, const std::string & localpath,
                   AWS_IO & io, AWS_Connection ** reqPtr = NULL);
    
    // Get object data (GET /key)
    void GetObject(const std::string & bkt, const std::string & key,
                   AWS_IO & io, AWS_Connection ** reqPtr = NULL);
    
    // Get meta-data on object (HEAD)
    // Headers are same as for GetObject(), but no data is retrieved.
    void GetObjectMData(const std::string & bkt, const std::string & key,
                        AWS_IO & io, AWS_Connection ** reqPtr = NULL);
    
    // Delete object (DELETE)
    void DeleteObject(const std::string & bkt, const std::string & key,
                      AWS_IO & io, AWS_Connection ** reqPtr = NULL);
    
    // Copy object (COPY)
    //TODO: copy ACL option
    void CopyObject(const std::string & srcbkt, const std::string & srckey,
                    const std::string & dstbkt, const std::string & dstkey, bool copyMD,
                    AWS_IO & io, AWS_Connection ** reqPtr = NULL);
    
    
    // List buckets (s3.amazonaws.com GET /)
    void ListBuckets(AWS_IO & io, AWS_Connection ** reqPtr = NULL);
    
    //TODO: requestPayment stuff
    //TODO: bucket location
    
    // Create bucket (bucket.s3.amazonaws.com PUT /)
    void CreateBucket(const std::string & bkt, AWS_IO & io, AWS_Connection ** reqPtr = NULL);
    
    // List bucket (bucket.s3.amazonaws.com GET /)
    void ListBucket(const std::string & bkt, AWS_IO & io, AWS_Connection ** reqPtr = NULL);
    
    // Delete bucket (bucket.s3.amazonaws.com DELETE /)
    void DeleteBucket(const std::string & bkt, AWS_IO & io, AWS_Connection ** reqPtr = NULL);
    
    
    std::string GetACL(const std::string & bkt, const std::string & key,
                       AWS_IO & io, AWS_Connection ** reqPtr = NULL);
    std::string GetACL(const std::string & bkt, AWS_IO & io,
                       AWS_Connection ** reqPtr = NULL);
    
    void SetACL(const std::string & bkt, const std::string & key, const std::string & acl,
                AWS_IO & io, AWS_Connection ** reqPtr = NULL);
    void SetACL(const std::string & bkt, const std::string & acl,
                AWS_IO & io, AWS_Connection ** reqPtr = NULL);
    
    // Set using a canned ACL: "private", "public-read", "public-read-write", "authenticated-read"
    void SetCannedACL(const std::string & bkt, const std::string & key, const std::string & acl,
                      AWS_IO & io, AWS_Connection ** reqPtr = NULL);
    void SetCannedACL(const std::string & bkt, const std::string & acl,
                      AWS_IO & io, AWS_Connection ** reqPtr = NULL);
};

#endif //AWS_S3_H
