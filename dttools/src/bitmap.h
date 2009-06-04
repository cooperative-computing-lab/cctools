/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2005- The University of Notre Dame
This software is distributed under a BSD-style license.
See the file COPYING for details.
*/

#ifndef BITMAP_H
#define BITMAP_H

struct bitmap * bitmap_create( int w, int h );
void            bitmap_delete( struct bitmap *b );

int   bitmap_get( struct bitmap *b, int x, int y );
void  bitmap_set( struct bitmap *b, int x, int y, int value );
int   bitmap_width( struct bitmap *b );
int   bitmap_height( struct bitmap *b );
void  bitmap_reset( struct bitmap *b, int value );
int  *bitmap_data( struct bitmap *b );

void  bitmap_rotate_clockwise( struct bitmap *s, struct bitmap *t );
void  bitmap_rotate_counterclockwise( struct bitmap *s, struct bitmap *t );

int  bitmap_average( struct bitmap *s );
void bitmap_smooth( struct bitmap *s, struct bitmap *t, int msize );
void bitmap_subset( struct bitmap *s, int x, int y, struct bitmap *t );
void bitmap_convolve( struct bitmap *s, struct bitmap *t, int (*f)( int x ) );
void bitmap_copy( struct bitmap *s, struct bitmap *t );

struct bitmap * bitmap_load_any( const char *path );

struct bitmap * bitmap_load_raw( const char *file );
struct bitmap * bitmap_load_bmp( const char *file );
struct bitmap * bitmap_load_pcx( const char *file );
struct bitmap * bitmap_load_sgi_rgb( const char *file );
struct bitmap * bitmap_load_jpeg( const char *file );

int  bitmap_save_raw( struct bitmap *b, const char *file );
int  bitmap_save_bmp( struct bitmap *b, const char *file );
int  bitmap_save_jpeg( struct bitmap *b, const char *file );

#ifndef MAKE_RGBA
/** Create a 32-bit RGBA value from 8-bit red, green, blue, and alpha values */
#define MAKE_RGBA(r,g,b,a) ( (((int)(a))<<24) | (((int)(r))<<16) | (((int)(g))<<8) | (((int)(b))<<0) )
#endif

#ifndef GET_RED
/** Extract an 8-bit red value from a 32-bit RGBA value. */
#define GET_RED(rgba) (( (rgba)>>16 ) & 0xff )
#endif

#ifndef GET_GREEN
/** Extract an 8-bit green value from a 32-bit RGBA value. */
#define GET_GREEN(rgba) (( (rgba)>>8 ) & 0xff )
#endif

#ifndef GET_BLUE
/** Extract an 8-bit blue value from a 32-bit RGBA value. */
#define GET_BLUE(rgba) (( (rgba)>>0 ) & 0xff )
#endif

#ifndef GET_ALPHA
/** Extract an 8-bit alpha value from a 32-bit RGBA value. */
#define GET_ALPHA(rgba) (( (rgba)>>24 ) & 0xff)
#endif

#endif

