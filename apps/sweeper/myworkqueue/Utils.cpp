//
//  Utils.cpp
//  
//
//  Created by Matthew Pruitt on 2/20/12.
//  Copyright 2012 __MyCompanyName__. All rights reserved.
//

#include <iostream>
#include <cstring>
#include "Utils.h"


bool stringEquals(std::string first, std::string second){
    if(strcmp (first.c_str(),second.c_str()) == 0){
        return true;
    }else{
        return false;
    }
}

bool stringEquals(const char* first, std::string second){
    if(strcmp (first,second.c_str()) == 0){
        return true;
    }else{
        return false;
    }
}

bool stringEquals(std::string first, const char* second){
    if(strcmp (first.c_str(),second) == 0){
        return true;
    }else{
        return false;
    }
}

bool stringEquals(const char* first, const char* second){
    if(strcmp (first,second) == 0){
        return true;
    }else{
        return false;
    }
}


std::vector<std::string> tokenizeString(const std::string& str, const std::string& delimiters){
    std::vector<std::string> tokenVector;
    // Find the first string field in the delimited line.
    std::string::size_type lastPos = str.find_first_not_of(delimiters, 0);
    std::string::size_type pos = str.find_first_of(delimiters, lastPos);
    // Now, parse the rest of the line.
    while (pos != std::string::npos || lastPos != std::string::npos){
        tokenVector.push_back(str.substr(lastPos, pos - lastPos));
        lastPos = str.find_first_not_of(delimiters, pos);
        pos = str.find_first_of(delimiters, lastPos);
    }
    return tokenVector;
}

std::string doubleToStr(double f)
{
    char tmp[255];
    sprintf(tmp, "%f", f);
    std::string result = tmp;
    return result;
}

std::string floatToStr(float f)
{
    char tmp[255];
    sprintf(tmp, "%f", f);
    std::string result = tmp;
    return result;
}


std::string intToStr(int i)
{
    char tmp[255];
    sprintf(tmp, "%d", i);
    std::string result = tmp;
    return result;
}

std::string extractFileExtension(const std::string &filePath){
    int dotIdx = 0;
    for (int i = filePath.length() - 1; i >= 0; i--){
        if (filePath[i] == '.'){
            dotIdx = i;
            break;
        }
    }
    return filePath.substr(dotIdx);
}

std::string baseFileName(std::string fileName){
    int fileLength = fileName.length(), extensionLength = extractFileExtension(fileName).length();
    return fileName.substr(0, fileLength - extensionLength);
}

std::string extractFileName(const std::string &filePath){
    std::vector<std::string> tokens = tokenizeString(filePath, "/");
    return tokens[tokens.size() - 1];
}
void println(std::string text){
    std::cout<<text<<std::endl;
}
void print(std::string text){
    std::cout<<text;
}
void println(int text){
    std::cout<<intToStr(text)<<std::endl;
}
void print(int text){
    std::cout<<intToStr(text);
}

void println(double text){
    std::cout<<text<<std::endl;
}
void print(double text){
    std::cout<<text;
}

void printBlockHeader(std::string text){
    std::cout<<"***************************************************************************"<<std::endl;
    std::cout<<"** "<<text<<std::endl;
    std::cout<<"***************************************************************************"<<std::endl;
}

void printBlockFooter(){
    std::cout<<"***************************************************************************"<<std::endl;
    std::cout<<"***************************************************************************"<<std::endl;
}

void printBlockParameter(std::string name, std::string value){
    std::cout <<"** "<<name<<std::endl<<"** \t"<<value<<std::endl;
}
void printBlockParameter(std::string name, double value){
    std::cout <<"** "<<name<<std::endl<<"** \t"<<value<<std::endl;
}
void printBlockParameter(std::string name, int value){
    std::cout <<"** "<<name<<std::endl<<"** \t"<<value<<std::endl;
}
void printBlockParameter(std::string name, float value){
    std::cout <<"** "<<name<<std::endl<<"** \t"<<value<<std::endl;
}
void printBlockLine(std::string name){
    std::cout << "** "<<name<<std::endl;
}
void printBlockLine(int name){
    std::cout << "** "<<name<<std::endl;
}
void printBlockLine(double name){
    std::cout << "** "<<name<<std::endl;
}
void printBlockLine(float name){
    std::cout << "** "<<name<<std::endl;
}



int dumbRound(double d)
{
    int i = (int) d;
    double floor = (double) i;
    return (d - floor >= 0.5)?i + 1:i;
}

int timeToFrame(double time, int FPS){
    //Time is in minutes, convert to milliseconds
    double ms=time*60*1000;
    
    //Make our lives easier and get ms/frame
    double msPerFrame=1000.0/(double)FPS;
    
    //Get approx frame number
    double frameNumber=ms/msPerFrame;
    
    //Round to frame it belongs to
    return (int)floor(frameNumber);
}

void readGroundTruth(std::vector<double>& storage, std::string file, int FPS){
    
    File* f=new File(file);
    Scanner* s=new Scanner(f);
    
    
    while(s->hasNext()){
        
        std::string temp=s->nextLine();
        if(temp.empty()){
            return;
        }
        StringTokenizer* st=new StringTokenizer(temp, ",");
        double tstamp, value;
       // println("Getting Paired Value");
        st->GetPairedValue(&tstamp, &value, temp, ",");
        
        //println("Converting to Frame number");
        int frameNum=timeToFrame(tstamp, FPS);
        
        //println("Seeing where to put it in the array");
        //print("("); print(frameNum); print(","); print(value); println(")");
        if((int)storage.size()>=frameNum+1){
            //We already have an entry for this frame, average it out
            storage[frameNum]=(storage[frameNum]+value)/2;
        }else{
            storage.push_back(value);
        }
        
         
    }
    
    
}

void echo( bool on )
{
    struct termios settings;
    tcgetattr( STDIN_FILENO, &settings );
    settings.c_lflag = on
    ? (settings.c_lflag |   ECHO )
    : (settings.c_lflag & ~(ECHO));
    tcsetattr( STDIN_FILENO, TCSANOW, &settings );
}

std::string getPassword(){
    std::string pwd;
    std::cout <<"Password: ";
    
    echo( false );
    getline( std::cin, pwd );
    echo( true );
    println("");
    return pwd;
}

std::string exec(char* cmd) {
    FILE* pipe = popen(cmd, "r");
    if (!pipe) return "ERROR";
    char buffer[128];
    std::string result = "";
    while(!feof(pipe)) {
        if(fgets(buffer, 128, pipe) != NULL)
            result += buffer;
    }
    pclose(pipe);
    return result;
}

