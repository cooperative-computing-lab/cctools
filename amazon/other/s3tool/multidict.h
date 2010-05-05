//    Copyright (c) 2009, Christopher James Huff
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

#ifndef MULTIDICT_H
#define MULTIDICT_H

#include <iostream>
#include <string>
#include <map>
#include <cstdlib>
#include <exception>
#include <stdexcept>

struct Dictionary_Error: public std::runtime_error {
	Dictionary_Error(const std::string & msg = ""): std::runtime_error(msg) {}
};

// A string-to-string dictionary, with additional methods for conversion to doubles and integers.
class AWS_MultiDict {
    std::multimap<std::string, std::string> entries;
  public:
    AWS_MultiDict() {}
    typedef std::multimap<std::string, std::string>::iterator iterator;
    typedef std::multimap<std::string, std::string>::const_iterator const_iterator;
    
    iterator begin() {return entries.begin();}
    const_iterator begin() const {return entries.begin();}
    
    iterator end() {return entries.end();}
    const_iterator end() const {return entries.end();}
    
    std::pair<iterator, iterator> equal_range(const std::string & key) {
        return entries.equal_range(key);
    }
    std::pair<const_iterator, const_iterator> equal_range(const std::string & key) const {
        return entries.equal_range(key);
    }
    
    void Clear() {entries.clear();}
    
    bool Exists(const std::string & key) const {return (entries.find(key) != entries.end());}
    
    // Get first value for key if one exists. Return true if a value found for key,
    // return false otherwise.
    bool Get(const std::string & key, std::string & value) const {
        const_iterator val = entries.find(key);
        if(val != entries.end()) value = val->second;
        return (val != entries.end());
    }
    bool Get(const std::string & key, double & value) const {
        const_iterator val = entries.find(key);
        if(val != entries.end()) value = strtod(val->second.c_str(), NULL);
        return (val != entries.end());
    }
    bool Get(const std::string & key, int & value) const {
        const_iterator val = entries.find(key);
        if(val != entries.end()) value = strtol(val->second.c_str(), NULL, 0);
        return (val != entries.end());
    }
    bool Get(const std::string & key, long & value) const {
        const_iterator val = entries.find(key);
        if(val != entries.end()) value = strtol(val->second.c_str(), NULL, 0);
        return (val != entries.end());
    }
    bool Get(const std::string & key, size_t & value) const {
        const_iterator val = entries.find(key);
        if(val != entries.end()) value = strtol(val->second.c_str(), NULL, 0);
        return (val != entries.end());
    }
    
    // Get first value for key if one exists. Return value if found for key,
    // return defaultVal otherwise.
    const std::string & GetWithDefault(const std::string & key, const std::string & defaultVal) const {
        const_iterator val = entries.find(key);
        return (val != entries.end())? val->second : defaultVal;
    }
    double GetWithDefault(const std::string & key, double defaultVal) const {
        const_iterator val = entries.find(key);
        return (val != entries.end())? strtod(val->second.c_str(), NULL) : defaultVal;
    }
    int GetWithDefault(const std::string & key, int defaultVal) const {
        const_iterator val = entries.find(key);
        return (val != entries.end())? strtol(val->second.c_str(), NULL, 0) : defaultVal;
    }
    long GetWithDefault(const std::string & key, long defaultVal) const {
        const_iterator val = entries.find(key);
        return (val != entries.end())? strtol(val->second.c_str(), NULL, 0) : defaultVal;
    }
    size_t GetWithDefault(const std::string & key, size_t defaultVal) const {
        const_iterator val = entries.find(key);
        return (val != entries.end())? strtol(val->second.c_str(), NULL, 0) : defaultVal;
    }
    
    // Insert entry into dictionary, regardless of existence of previous entries with key.
    void Insert(const std::string & key, const std::string & value) {
        entries.insert(std::make_pair(key, value));
    }
    
    // Set value for existing key if possible, insert entry into dictionary if no value for key
    // exists.
    void Set(const std::string & key, const std::string & value) {
        iterator val = entries.find(key);
        if(val == entries.end())
            Insert(key, value);
        else
            val->second = value;
    }
};

#endif //MULTIDICT_H
