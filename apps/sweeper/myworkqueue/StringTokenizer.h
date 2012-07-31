//
//  StringTokenizer.h
//  
//
//  Created by Matthew Pruitt on 3/16/12.
//  Copyright 2012 __MyCompanyName__. All rights reserved.
//

#ifndef _StringTokenizer_h
#define _StringTokenizer_h
#include <stdio.h>
#include <stdlib.h>
#include <string>
#include <vector>
#include "Utils.h"

class StringTokenizer{
public: 
    StringTokenizer (std::string input);
    StringTokenizer (std::string input, std::string delimiter);
    bool hasNext();
    std::string nextToken();
    void GetPairedValue(double* time, double* value, std::string aString, std::string delimiter);
    void SplitString (std::vector<std::string>& vec, std::string aString, std::string delimiter);
    void advanceToken();
};

#endif
