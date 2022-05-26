#ifndef SEMAPHORE_H
#define SEMAPHORE_H

int  semaphore_create( int value );
void semaphore_down( int s );
void semaphore_up( int s );

#endif

