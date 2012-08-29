//
//  Scanner.h
//  
//
//  Created by Matthew Pruitt on 3/16/12.
//  Copyright 2012 __MyCompanyName__. All rights reserved.
//

#ifndef _Scanner_h
#define _Scanner_h
#include <iostream>
#include <fstream>
#include <string>
#include "File.h"


class Scanner{
public:
    Scanner(File* f);
    bool hasNext();
    std::string nextLine();
};

#endif
