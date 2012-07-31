//
//  File.cpp
//  
//
//  Created by Matthew Pruitt on 3/16/12.
//  Copyright 2012 __MyCompanyName__. All rights reserved.
//

#include <iostream>
#include "File.h"

using namespace std;

File::File (string filename):ifstream(filename.c_str(), ifstream::in){
    fname=filename;
}

string File::getFileName(){
    return fname;
}


bool File::hasNext(){
    return good();
}

bool File::isOpen(){
    return is_open();
}
