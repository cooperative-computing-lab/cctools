#ifndef TEXT_ARRAY_H
#define TEXT_ARRAY_H

/** @file text_array.h
A two dimensional array of strings.
Each cell may contain either a null pointer or a pointer to an ordinary string.
A simple external representation is used to load, store, and subset arrays between processes.
*/

/** Create a new text array.
@param w Width of the array.
@param h Height of the array.
@return A new text array on success, or null on failure.
*/
struct text_array * text_array_create( int w, int h );

/** Delete a text array and all of its contents.
@param t The text array to deleted.
*/
void text_array_delete( struct text_array *t );

/** Get the width of the array.
@param t A text array.
@return The width of the array.
*/
int text_array_width( struct text_array *t );

/** Get the height of the array.
@param t A text array.
@return The height of the array.
*/
int text_array_height( struct text_array *t );

/** Look up one cell in the array.
@param t A text array.
@param x The x position of the cell.
@param y The y position of the cell.
@return The value of the cell, which might be null.
*/
const char * text_array_get( struct text_array *t, int x, int y );

/** Set one cell in the array.
@param t A text array.
@param x The x position of the cell.
@param y The y position of the cell.
@param c A string to place in the cell.  If not null, c will be copied with strdup and placed in the data structure.  Regardless, the current occupant of the cell will be freed.
*/
int text_array_set( struct text_array *t, int x, int y, const char *c );

/** Load an array from a file.
@param t An array created by @ref text_array_create.
@param filename The filename to load from.
*/
int text_array_load( struct text_array *t, const char *filename );

/** Save an array to a file.
@param t An array created by @ref text_array_create.
@param filename The filename to write to.
*/
int text_array_save( struct text_array *t, const char *filename );

/** Save a portion of an array to a file.
@param t An array created by @ref text_array_create.
@param filename The filename to write to.
@param x The starting x position of the range to save.
@param y The starting y position of the range to save.
@param w The width of the range to save.
@param h The height of the range to save.
*/
int text_array_save_range( struct text_array *t, const char *filename, int x, int y, int w, int h );

#endif
