//
//  Utils.h
//  
//
//  Created by Matthew Pruitt on 2/20/12.
//  Copyright 2012 __MyCompanyName__. All rights reserved.
//

#ifndef _Utils_h
#define _Utils_h

#include <string>
#include <stdlib.h>
#include <vector>
#include <cmath>
#include <termios.h>
#include <unistd.h>
#include "Scanner.h"
#include "StringTokenizer.h"

/* Macros to get the max/min of 3 values */

#define MAX3(r,g,b) ((r)>(g)?((r)>(b)?(r):(b)):((g)>(b)?(g):(b)))
#define MIN3(r,g,b) ((r)<(g)?((r)<(b)?(r):(b)):((g)<(b)?(g):(b)))
//#define MIN(x,y) (((x)<(y)) ? (x):(y))
//#define MAX(x,y) (((x)>(y)) ? (x):(y))

std::vector<std::string> tokenizeString(const std::string& str, const std::string& delimiters);
std::string doubleToStr(double f);
std::string floatToStr(float f);
std::string intToStr(int i);
std::string extractFileExtension(const std::string &filePath);
std::string baseFileName(std::string fileName);
std::string extractFileName(const std::string &filePath);
void println(std::string text);
void print(std::string text);
void println(int text);
void print(int text);
void println(double text);
void print(double text);
void printBlockFooter();
void printBlockParameter(std::string name, std::string value);
void printBlockParameter(std::string name, double value);
void printBlockParameter(std::string name, int value);
void printBlockParameter(std::string name, float value);
void printBlockLine(std::string name);
void printBlockHeader(std::string text);
void printBlockLine(int name);
void printBlockLine(double name);
void printBlockLine(float name);
int dumbRound(double d);
int timeToFrame(double time, int FPS);
void readGroundTruth(std::vector<double>& storage, std::string file, int FPS);
bool stringEquals(std::string first, std::string second);
bool stringEquals(const char* first, std::string second);
bool stringEquals(std::string first, const char* second);
bool stringEquals(const char* first, const char* second);
void echo( bool on );
std::string getPassword();
std::string exec(char* cmd);

#endif
