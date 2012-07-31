//
//  File.h
//  
//
//  Created by Matthew Pruitt on 3/16/12.
//  Copyright 2012 __MyCompanyName__. All rights reserved.
//

#ifndef _File_h
#define _File_h

#include<iostream>
#include<fstream>


class File:public std::ifstream{
public:
    std::string fname;
    explicit File(std::string Filename);
    std::string getFileName();  
    bool isOpen();
    bool hasNext();
};

#endif
