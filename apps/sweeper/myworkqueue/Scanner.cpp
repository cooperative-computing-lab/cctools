//
//  Scanner.cpp
//  
//
//  Created by Matthew Pruitt on 3/16/12.
//  Copyright 2012 __MyCompanyName__. All rights reserved.
//

#include <iostream>
#include "Scanner.h"

using namespace std;

File* file;

Scanner::Scanner(File* f){
    file=f;
}

bool Scanner::hasNext(){
    return(file->isOpen() && file->hasNext());
}

string Scanner::nextLine(){
    string line;
    getline (*file,line);
    return line;
}
