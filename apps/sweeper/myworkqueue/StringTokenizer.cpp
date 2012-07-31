//
//  StringTokenizer.cpp
//  
//
//  Created by Matthew Pruitt on 3/16/12.
//  Copyright 2012 __MyCompanyName__. All rights reserved.
//

#include <iostream>
#include <cstring>
#include "StringTokenizer.h"


unsigned int position=0;
std::vector<std::string> holder;

StringTokenizer::StringTokenizer (std::string input){
   holder.clear();
    position=0;
    
    char source[256];
    strcpy(source, input.c_str());
    
    char * pch;
    pch = strtok(source,",");
    while (pch != NULL)
    {
        //Push back pch as a string
        
        holder.push_back(pch);
        //Get the next pch
        pch = strtok (NULL, ",");
    }
}

StringTokenizer::StringTokenizer (std::string input, std::string delimiter){
    holder.clear();
    position=0;
    char * pch;
    char source[256];
    strcpy(source, input.c_str());
    pch = strtok(source,delimiter.c_str());
    while (pch != NULL)
    {
        //Push back pch as a string
        holder.push_back(pch);
        //Get the next pch
        pch = strtok (NULL, delimiter.c_str());
    }
}

bool StringTokenizer::hasNext(){
    if(position>=holder.size()){
        return false;
    }else{
        return true;
    }
}

std::string StringTokenizer::nextToken(){
    position++;
    return holder[position-1];
    
}
void StringTokenizer::advanceToken(){
    position++;
}

void StringTokenizer::GetPairedValue(double* time, double* value, std::string aString, std::string delimiter){
    
   
    
    char source[256];
    strcpy(source, aString.c_str());
    
    char * pch;
    pch = strtok(source,delimiter.c_str());
    *time=atof(pch);
       
    
    pch = strtok (NULL, delimiter.c_str());
    *value=atof(pch);
    
    
    
    

}

void StringTokenizer::SplitString (std::vector<std::string>& vec, std::string aString, std::string delimiter)
{
    
    char * cstr, *p;
    char * cdelim;
    
    //string str ("Please split this phrase into tokens");
    
    std::string str = aString;
    std::string delim=delimiter;
    
    cstr = new char [str.size()+1];
    strcpy (cstr, str.c_str());
    
    cdelim=new char [delim.size()+1];
    strcpy (cdelim, delimiter.c_str());
    
    // cstr now contains a c-string copy of str
    
    p=strtok (cstr,cdelim);
    while (p!=NULL)
    {
        vec.push_back(p);
        p=strtok(NULL,cdelim);
    }
    
    delete[] cstr;
    delete[] cdelim;
    
    
}
