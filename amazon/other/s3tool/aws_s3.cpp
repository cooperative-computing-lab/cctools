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



#include <iostream>
#include <fstream>
#include <string>
#include <vector>
#include <map>
#include <sstream>

#include "aws_s3.h"
#include "aws_s3_misc.h"

#include <curlpp/cURLpp.hpp>
#include <curlpp/Options.hpp>

using namespace std;


//************************************************************************************************
// AWS_IO
//************************************************************************************************

void AWS_IO::WillStart()
{
//    if(printProgress)
//        cout << endl;
}

void AWS_IO::DidFinish()
{
    if(printProgress)
        cout << endl;
    
    if(Failure())
        cerr << "#### ERROR: Operation failed:\n" << *this << endl;
}


size_t AWS_IO::Write(char * buf, size_t size, size_t nmemb)
{
    if(ostrm) {
        ostrm->write(buf, size*nmemb);
        bytesReceived += size*nmemb;
        if(printProgress) {
            if(bytesToGet == 0)
                cout << "received " << bytesReceived << " bytes, content size unknown";
            else
                cout << "received " << bytesReceived << " bytes, " << 100*bytesReceived/bytesToGet << "%";
            cout << "                        \r";
            cout.flush();
        }
    }
    return size*nmemb;
}

size_t AWS_IO::Read(char * buf, size_t size, size_t nmemb)
{
    streamsize count = 0;
    if(istrm) {
        istrm->read(buf, size*nmemb);
        count = istrm->gcount();
        bytesSent += count;
        if(printProgress) {
            if(bytesToPut == 0)
                cout << "sent " << bytesSent << " bytes, content size unknown";
            else
                cout << "sent " << bytesSent << " bytes, " << 100*bytesSent/bytesToPut << "%";
            cout << "                        \r";
            cout.flush();
        }
    }
    return count;
}

size_t AWS_IO::HandleHeader(char * buf, size_t size, size_t nmemb)
{
    size_t length = size*nmemb;
//        cout << "#### HeaderCB, Header received: " << string(buf, length);
    if(length >= 8 && strncmp(buf, "HTTP/1.1", 8) == 0) {
        result = string(buf + 9, length - 9);
        numResult = strtol(result.c_str(), NULL, 0);
    }
    else if(length == 2 && strncmp(buf, "\r\n", 2) == 0) {
        // ignore
    }
    else {
        // Find first occurrence of ':'
        size_t c = 0;
        while(c < length && buf[c] != ':')
            ++c;
        
        if(c < length) {
            string header(buf, c);
            string data(buf+c+2, length - c - 2);
            if(data[data.length()-1] == '\n')
                data.erase(data.length()-1);
//                cout << "#### HeaderCB, parsed header: " << header << endl;
//                cout << "#### HeaderCB, parsed header data: " << data << endl;
            headers.Set(header, data);
        }
        else {
            cerr << "#### ERROR: HeaderCB, unknown header received: " << string(buf, length);
            cerr << "#### length: " << length << endl;
            for(size_t j = 0; j < length; ++j)
                cerr << (int)buf[j] << " ";
            cerr << endl;
        }
    }
    return length;
}

std::ostream & operator<<(std::ostream & ostrm, AWS_IO & io)
{
    ostrm << "result: " << io.result << std::endl;
    ostrm << "headers:" << std::endl;
    AWS_MultiDict::iterator i;
    for(i = io.headers.begin(); i != io.headers.end(); ++i)
        ostrm << i->first << ": " << i->second << std::endl;
    return ostrm;
}

//************************************************************************************************
// AWS
//************************************************************************************************

AWS::AWS(const string & kid, const string & sk):
    keyID(kid), secret(sk),
    verbosity(0)
{
}

AWS::~AWS()
{
    // Teardown connections, etc
}


void AWS::ParseBucketsList(list<AWS_S3_Bucket> & buckets, const string & xml)
{
    string::size_type crsr = 0;
    string data;
    string name, date;
    string ownerName, ownerID;
    ExtractXML(ownerID, crsr, "ID", xml);
    ExtractXML(ownerName, crsr, "DisplayName", xml);
    
    while(ExtractXML(data, crsr, "Name", xml))
    {
        name = data;
        if(ExtractXML(data, crsr, "CreationDate", xml))
            date = data;
        else
            date = "";
        buckets.push_back(AWS_S3_Bucket(name, date));
    }
}

void AWS::ParseObjectsList(list<AWS_S3_Object> & objects, const string & xml)
{
    string::size_type crsr = 0;
    string data;
    
    while(ExtractXML(data, crsr, "Key", xml))
    {
        AWS_S3_Object obj;
        obj.key = data;
        
        if(ExtractXML(data, crsr, "LastModified", xml))
            obj.lastModified = data;
        if(ExtractXML(data, crsr, "ETag", xml))
            obj.eTag = data;
        if(ExtractXML(data, crsr, "Size", xml))
            obj.size = data;
        
        if(ExtractXML(data, crsr, "ID", xml))
            obj.ownerID = data;
        if(ExtractXML(data, crsr, "DisplayName", xml))
            obj.ownerDisplayName = data;
        
        if(ExtractXML(data, crsr, "StorageClass", xml))
            obj.storageClass = data;
        
        objects.push_back(obj);
    }
}

std::list<AWS_S3_Bucket> & AWS::GetBuckets(bool getContents, bool refresh,
                                           AWS_Connection ** conn)
{
    if(refresh || buckets.empty())
        RefreshBuckets(getContents, conn);
    return buckets;
}

void AWS::RefreshBuckets(bool getContents, AWS_Connection ** conn)
{
    std::ostringstream bucketList;
    AWS_IO io(NULL, &bucketList);
    ListBuckets(io, conn);
    
    buckets.clear();
    ParseBucketsList(buckets, bucketList.str());
    
    if(getContents) {
        list<AWS_S3_Bucket>::iterator bkt;
        for(bkt = buckets.begin(); bkt != buckets.end(); ++bkt)
            GetBucketContents(*bkt, conn);
    }
}

void AWS::GetBucketContents(AWS_S3_Bucket & bucket, AWS_Connection ** conn)
{
    std::ostringstream objectList;
    AWS_IO io(NULL, &objectList);
    ListBucket(bucket.name, io, conn);
    ParseObjectsList(bucket.objects, objectList.str());
}

string AWS::GenRequestSignature(const AWS_IO & io, const string & uri, const string & mthd)
{
	std::ostringstream sigstrm;
    sigstrm << mthd << "\n";
    sigstrm << io.sendHeaders.GetWithDefault("Content-MD5", "") << "\n";
    sigstrm << io.sendHeaders.GetWithDefault("Content-Type", "") << "\n";
    sigstrm << io.httpDate << "\n";
    
    // http://docs.amazonwebservices.com/AmazonS3/latest/index.html?RESTAccessPolicy.html
    // CanonicalizedAmzHeaders
    // TODO: convert headers into canonicalized form (almost there already):
    // lower-case (TODO)
    // sorted lexicographically (natural result from map?)
    // combine headers of same type
    // unfold long lines
    // no space (check)
    AWS_MultiDict::const_iterator i;
    for(i = io.sendHeaders.begin(); i != io.sendHeaders.end(); ++i) {
        if(i->first.substr(0, 6) == "x-amz-")
            sigstrm << i->first + ":" + i->second << '\n';
    }
    sigstrm << "/" << uri;

    if(verbosity >= 3)
        cout << "#### sigtext:\n" << sigstrm.str() << "\n#### end sigtext" << endl;
    
    return GenerateSignature(secret, sigstrm.str());
}

// read by libcurl...read data to send
struct ReadDataCB {
    AWS_IO & io;
    ReadDataCB(AWS_IO & ioio): io(ioio) {}
    size_t operator()(char * buf, size_t size, size_t nmemb) {return io.Read(buf, size, nmemb);}
};

// write by libcurl...handle data received
struct WriteDataCB {
    AWS_IO & io;
    WriteDataCB(AWS_IO & ioio): io(ioio) {}
    size_t operator()(char * buf, size_t size, size_t nmemb) {return io.Write(buf, size, nmemb);}
};

// Handle header data.
//"The header callback will be called once for each header and
// only complete header lines are passed on to the callback."
struct HeaderCB {
    AWS_IO & io;
    HeaderCB(AWS_IO & ioio): io(ioio) {}
    size_t operator()(char * buf, size_t size, size_t nmemb) {return io.HandleHeader(buf, size, nmemb);}
};

void AWS::Send(const string & url, const string & uri, const string & method,
               AWS_IO & io, AWS_Connection ** reqPtr)
{
    string signature;
    io.httpDate = HTTP_Date();
    signature = GenRequestSignature(io, uri, method);
    
    if(verbosity >= 2)
        io.printProgress = true;
    
    try {
        cURLpp::Easy * req;
        // create new Easy or reset and reuse old one.
        if(reqPtr == NULL)//no handle, locally create and delete Easy
            req = new cURLpp::Easy;
        else {
            if(*reqPtr == NULL) {
                // Create new Easy, save in handle
                req = *reqPtr = new cURLpp::Easy;
            }
            else {
                // reuse old Easy
                req = *reqPtr;
                req->reset();
            }
        }
        
        cURLpp::Easy & request = *req;
        
        std::ostringstream authstrm, datestrm, urlstrm;
        datestrm << "Date: " << io.httpDate;
        authstrm << "Authorization: AWS " << keyID << ":" << signature;
        
        std::list<std::string> headers;
        headers.push_back(datestrm.str());
        headers.push_back(authstrm.str());
        
        AWS_MultiDict::iterator i;
        for(i = io.sendHeaders.begin(); i != io.sendHeaders.end(); ++i) {
            headers.push_back(i->first + ": " + i->second);
            if(verbosity >= 3)
                cout << "special header: " << i->first + ": " + i->second << endl;
        }
        
        request.setOpt(new cURLpp::Options::WriteFunction(cURLpp::Types::WriteFunctionFunctor(WriteDataCB(io))));
        request.setOpt(new cURLpp::Options::HeaderFunction(cURLpp::Types::WriteFunctionFunctor(HeaderCB(io))));
        
        if(method == "GET") {
            request.setOpt(new cURLpp::Options::HttpGet(true));
        }
        else if(method == "PUT") {
            request.setOpt(new cURLpp::Options::Upload(true));
            request.setOpt(new cURLpp::Options::ReadFunction(cURLpp::Types::ReadFunctionFunctor(ReadDataCB(io))));
            request.setOpt(new cURLpp::Options::InfileSize(io.bytesToPut));
        }
        else if(method == "HEAD") {
            request.setOpt(new cURLpp::Options::Header(true));
            request.setOpt(new cURLpp::Options::NoBody(true));
        }
        else {
            request.setOpt(new cURLpp::Options::CustomRequest(method));
        }
        
        request.setOpt(new cURLpp::Options::Url(url));
        request.setOpt(new cURLpp::Options::Verbose(verbosity >= 3));
        request.setOpt(new cURLpp::Options::HttpHeader(headers));
        
        io.WillStart();
        request.perform();
        io.DidFinish();
        
        // If created new Easy for this call, delete it.
        if(reqPtr == NULL)
            delete req;
    }
    catch(cURLpp::RuntimeError & e) {
        io.error = true;
        cerr << "Error: " << e.what() << endl;
    }
    catch(cURLpp::LogicError & e) {
        io.error = true;
        cerr << "Error: " << e.what() << endl;
    }
}


void AWS::PutObject(const string & bkt, const string & key, const string & acl,
                    AWS_IO & io, AWS_Connection ** reqPtr)
{
    std::ostringstream urlstrm;
    urlstrm << "http://" << bkt << ".s3.amazonaws.com/" << key;
    
    if(acl != "") io.sendHeaders.Set("x-amz-acl", acl);
    
    istream & fin = *io.istrm;
    uint8_t md5[EVP_MAX_MD_SIZE];
    size_t mdLen = ComputeMD5(md5, fin);
    io.sendHeaders.Set("Content-MD5", EncodeB64(md5, mdLen));
    
    fin.clear();
    fin.seekg(0, std::ios_base::end);
    ifstream::pos_type endOfFile = fin.tellg();
    fin.seekg(0, std::ios_base::beg);
    ifstream::pos_type startOfFile = fin.tellg();
    
    io.bytesReceived = 0;
    io.bytesToPut = static_cast<size_t>(endOfFile - startOfFile);
    
    Send(urlstrm.str(), bkt + "/" + key, "PUT", io, reqPtr);
}

void AWS::PutObject(const string & bkt, const string & key,
                    const string & acl, const string & path,
                    AWS_IO & io, AWS_Connection ** reqPtr)
{
    ifstream fin(path.c_str(), ios_base::binary | ios_base::in);
    if(!fin) {
        cerr << "Could not read file " << path << endl;
        return;
    }
    io.istrm = &fin;
    PutObject(bkt, key, acl, io, reqPtr);
}

//************************************************************************************************
// Objects
//************************************************************************************************

void AWS::GetObject(const string & bkt, const string & key,
                    AWS_IO & io, AWS_Connection ** reqPtr)
{
    std::ostringstream urlstrm;
    urlstrm << "http://" << bkt << ".s3.amazonaws.com/" << key;
    Send(urlstrm.str(), bkt + "/" + key, "GET", io, reqPtr);
}

void AWS::GetObjectMData(const string & bkt, const string & key,
                         AWS_IO & io, AWS_Connection ** reqPtr)
{
    std::ostringstream urlstrm;
    urlstrm << "http://" << bkt << ".s3.amazonaws.com/" << key;
    Send(urlstrm.str(), bkt + "/" + key, "HEAD", io, reqPtr);
}

void AWS::DeleteObject(const string & bkt, const string & key,
                       AWS_IO & io, AWS_Connection ** reqPtr)
{
    std::ostringstream urlstrm;
    urlstrm << "http://" << bkt << ".s3.amazonaws.com/" << key;
    Send(urlstrm.str(), bkt + "/" + key, "DELETE", io, reqPtr);
}

void AWS::CopyObject(const std::string & srcbkt, const std::string & srckey,
                     const std::string & dstbkt, const std::string & dstkey, bool copyMD,
                     AWS_IO & io, AWS_Connection ** reqPtr)
{
    std::ostringstream urlstrm;
    urlstrm << "http://" << dstbkt << ".s3.amazonaws.com/" << dstkey;
    io.sendHeaders.Set("x-amz-copy-source", string("/") + srcbkt + "/" + srckey);
    io.sendHeaders.Set("x-amz-metadata-directive", copyMD? "COPY" : "REPLACE");
//    io.sendHeaders["x-amz-copy-source-if-match"] =  etag
//    io.sendHeaders["x-amz-copy-source-if-none-match"] =  etag
//    io.sendHeaders["x-amz-copy-source-if-unmodified-since"] =  time_stamp
//    io.sendHeaders["x-amz-copy-source-if-modified-since"] =  time_stamp
    Send(urlstrm.str(), dstbkt + "/" + dstkey, "PUT", io, reqPtr);
}

//************************************************************************************************
// Buckets
//************************************************************************************************

void AWS::ListBuckets(AWS_IO & io, AWS_Connection ** reqPtr)
{
    Send("http://s3.amazonaws.com/", "", "GET", io, reqPtr);
}

void AWS::CreateBucket(const string & bkt, AWS_IO & io, AWS_Connection ** reqPtr)
{
    std::ostringstream urlstrm;
    urlstrm << "http://" << bkt << ".s3.amazonaws.com";
    io.bytesToPut = 0;
    Send(urlstrm.str(), bkt + "/", "PUT", io, reqPtr);
}

void AWS::ListBucket(const string & bkt, AWS_IO & io, AWS_Connection ** reqPtr)
{
    std::ostringstream urlstrm;
    urlstrm << "http://" << bkt << ".s3.amazonaws.com";
    Send(urlstrm.str(), bkt + "/", "GET", io, reqPtr);
}

void AWS::DeleteBucket(const string & bkt, AWS_IO & io, AWS_Connection ** reqPtr)
{
    std::ostringstream urlstrm;
    urlstrm << "http://" << bkt << ".s3.amazonaws.com";
    Send(urlstrm.str(), bkt + "/", "DELETE", io, reqPtr);
}


//************************************************************************************************
// ACLs
//************************************************************************************************

std::string AWS::GetACL(const std::string & bkt, const std::string & key,
                        AWS_IO & io, AWS_Connection ** reqPtr)
{
    std::ostringstream aclResponse;
    io.ostrm = &aclResponse;
    std::ostringstream urlstrm;
//    urlstrm << "http://" << bkt << ".s3.amazonaws.com/";
    urlstrm << "http://" << bkt << ".s3.amazonaws.com/" << key << "?acl";
    Send(urlstrm.str(), bkt + "/" + key + "?acl", "GET", io, reqPtr);
    
    return aclResponse.str();
}

std::string AWS::GetACL(const std::string & bkt, AWS_IO & io, AWS_Connection ** reqPtr)
{
    std::ostringstream aclResponse;
    io.ostrm = &aclResponse;
    std::ostringstream urlstrm;
    urlstrm << "http://" << bkt << ".s3.amazonaws.com/?acl";
    Send(urlstrm.str(), bkt + "/?acl", "GET", io, reqPtr);
    
    return aclResponse.str();
}

// Set full ACL for object in bucket
void AWS::SetACL(const std::string & bkt, const std::string & key, const std::string & acl,
                 AWS_IO & io, AWS_Connection ** reqPtr)
{
    std::istringstream aclStrm(acl);
    io.istrm = &aclStrm;
    std::ostringstream urlstrm;
    urlstrm << "http://" << bkt << ".s3.amazonaws.com/" << key << "?acl";
    io.bytesToPut = acl.length();
    Send(urlstrm.str(), bkt + "/" + key + "?acl", "PUT", io, reqPtr);
}

// Set full ACL for bucket
void AWS::SetACL(const std::string & bkt, const std::string & acl,
                 AWS_IO & io, AWS_Connection ** reqPtr)
{
    std::istringstream aclStrm(acl);
    io.istrm = &aclStrm;
    std::ostringstream urlstrm;
    urlstrm << "http://" << bkt << ".s3.amazonaws.com/?acl";
    io.bytesToPut = acl.length();
    Send(urlstrm.str(), bkt + "/?acl", "PUT", io, reqPtr);
}

// Set canned ACL for object in bucket
void AWS::SetCannedACL(const std::string & bkt, const std::string & key, const std::string & acl,
                 AWS_IO & io, AWS_Connection ** reqPtr)
{
    // TODO: enforce valid acl, one of:
    // "private", "public-read", "public-read-write", "authenticated-read"
    std::ostringstream urlstrm;
    urlstrm << "http://" << bkt << ".s3.amazonaws.com/" << key << "?acl";
    io.sendHeaders.Set("x-amz-acl", acl);
    io.bytesToPut = 0;
    Send(urlstrm.str(), bkt + "/" + key + "?acl", "PUT", io, reqPtr);
}

// Set canned ACL for bucket
void AWS::SetCannedACL(const std::string & bkt, const std::string & acl,
                 AWS_IO & io, AWS_Connection ** reqPtr)
{
    std::ostringstream urlstrm;
    urlstrm << "http://" << bkt << ".s3.amazonaws.com/?acl";
    io.sendHeaders.Set("x-amz-acl", acl);
    io.bytesToPut = 0;
    Send(urlstrm.str(), bkt + "/?acl", "PUT", io, reqPtr);
}
