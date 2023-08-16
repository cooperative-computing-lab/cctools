/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2022 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifdef HAS_LIBJPEG
#include "jinclude.h"
#include "jpeglib.h"
#endif /*  */

#include <stdlib.h>
#include <stdio.h>
#include <string.h>

#include "bitmap.h"
#include "macros.h"
struct bitmap {
	int width;
	int height;
	int *data;
};
struct bitmap *bitmap_create(int w, int h)
{
	struct bitmap *m;
	m = malloc(sizeof *m);
	if(!m)
		return 0;
	m->data = malloc(w * h * sizeof(int));
	if(!m->data) {
		free(m);
		return 0;
	}
	m->width = w;
	m->height = h;
	return m;
}

void bitmap_delete(struct bitmap *m)
{
	free(m->data);
	free(m);
} void bitmap_reset(struct bitmap *m, int value)
{
	int i;
	for(i = 0; i < (m->width * m->height); i++) {
		m->data[i] = value;
	}
}

int bitmap_get(struct bitmap *m, int x, int y)
{
	while(x >= m->width)
		x -= m->width;
	while(y >= m->height)
		y -= m->height;
	while(x < 0)
		x += m->width;
	while(y < 0)
		y += m->height;
	return m->data[y * m->width + x];
}

void bitmap_set(struct bitmap *m, int x, int y, int value)
{
	while(x >= m->width)
		x -= m->width;
	while(y >= m->height)
		y -= m->height;
	while(x < 0)
		x += m->width;
	while(y < 0)
		y += m->height;
	m->data[y * m->width + x] = value;
}

int bitmap_width(struct bitmap *m)
{
	return m->width;
}

int bitmap_height(struct bitmap *m)
{
	return m->height;
}

int *bitmap_data(struct bitmap *m)
{
	return m->data;
}

int bitmap_average(struct bitmap *m)
{
	double r = 0, g = 0, b = 0;
	int avg;
	int i;
	int size = m->width * m->height;
	for(i = 0; i < size; i++) {
		int rgb = m->data[i];
		r += GET_RED(rgb);
		g += GET_GREEN(rgb);
		b += GET_BLUE(rgb);
	} r = r / size;
	g = g / size;
	b = b / size;
	avg = MAKE_RGBA((int) r, (int) g, (int) b, 255);
	return avg;
}

void bitmap_rotate_clockwise(struct bitmap *s, struct bitmap *t)
{
	int i, j;
	if(s->width != t->height || s->height != t->width)
		return;
	for(j = 0; j < s->height; j++) {
		for(i = 0; i < s->width; i++) {
			bitmap_set(t, j, i, bitmap_get(s, i, j));
		}
	}
}

void bitmap_rotate_counterclockwise(struct bitmap *s, struct bitmap *t)
{
	int i, j;
	if(s->width != t->height || s->height != t->width)
		return;
	for(j = 0; j < s->height; j++) {
		for(i = 0; i < s->width; i++) {
			bitmap_set(t, s->height - j - 1, s->width - i - 1, bitmap_get(s, i, j));
		}
	}
}

void bitmap_copy(struct bitmap *a, struct bitmap *b)
{
	if(a->width != b->width || a->height != b->height)
		return;
	memcpy(b->data, a->data, a->width * a->height * sizeof(*a->data));
}

void bitmap_smooth(struct bitmap *a, struct bitmap *b, int size)
{
	int i, j, m, n;
	int avg;
	int ncells = (size * 2 + 1) * (size * 2 + 1);
	for(j = 0; j < a->height; j++) {
		for(i = 0; i < a->width; i++) {
			avg = 0;
			for(n = -size; n <= size; n++) {
				for(m = -size; m <= size; m++) {
					avg += bitmap_get(a, i + m, j + n);
				}
			}
			bitmap_set(b, i, j, avg / ncells);
		}
	}
}

void bitmap_convolve(struct bitmap *a, struct bitmap *b, int (*f) (int x))
{
	int L = a->width * a->height;
	int i;
	for(i = 0; i < L; i++) {
		b->data[i] = f(a->data[i]);
	}
}

struct bitmap *bitmap_load_any(const char *path)
{
	char *tail = strrchr(path, '.');
	if(!tail) {
		printf("bitmap: cannot determine type of %s\n", path);
		return 0;
	}
	tail++;
	if(!strcmp(tail, "raw")) {
		return bitmap_load_raw(path);
	} else if(!strcmp(tail, "bmp") || !strcmp(tail, "BMP")) {
		return bitmap_load_bmp(path);
	} else if(!strcmp(tail, "pcx") || !strcmp(tail, "PCX")) {
		return bitmap_load_pcx(path);
	} else if(!strcmp(tail, "rgb") || !strcmp(tail, "RGB")) {
		return bitmap_load_sgi_rgb(path);

#ifdef HAS_LIBJPEG
	} else if(!strcmp(tail, "jpg") || !strcmp(tail, "JPG")) {
		return bitmap_load_jpeg(path);
	} else if(!strcmp(tail, "jpeg") || !strcmp(tail, "JPEG")) {
		return bitmap_load_jpeg(path);

#endif /*  */
	} else {
		printf("bitmap: bitmap type %s not supported.\n", tail);
		return 0;
	}
}

struct bitmap *bitmap_load_raw(const char *path)
{
	FILE *file;
	int width, height;
	struct bitmap *m;
	file = fopen(path, "rb");
	if(!file)
		return 0;
	fread(&width, 1, sizeof(width), file);
	fread(&height, 1, sizeof(height), file);
	m = bitmap_create(width, height);
	if(!m) {
		fclose(file);
		return 0;
	}
	fread(m->data, 1, width * height * sizeof(int), file);
	fclose(file);
	return m;
}

int bitmap_save_raw(struct bitmap *m, const char *path)
{
	FILE *file;
	file = fopen(path, "wb");
	if(!file)
		return 0;
	fwrite(&m->width, 1, sizeof(m->width), file);
	fwrite(&m->height, 1, sizeof(m->height), file);
	fwrite(m->data, 1, m->width * m->height * sizeof(int), file);
	fclose(file);
	return 1;
}

void bitmap_subset(struct bitmap *m, int origx, int origy, struct bitmap *n)
{
	int i, j;
	int x, y;
	while(origx < 0)
		origx = origx + m->width;
	while(origy < 0)
		origy = origy + m->height;
	while(origx >= m->width)
		origx -= m->width;
	while(origy >= m->height)
		origy -= m->height;
	y = origy;
	for(j = 0; j < n->height; j++, y++) {
		if(y >= m->height)
			y = 0;
		x = origx;
		for(i = 0; i < n->width; x++, i++) {
			if(x >= m->width)
				x = 0;
			n->data[j * n->width + i] = m->data[y * m->width + x];
		}
	}
}


#pragma pack(1)
struct bmp_header {
	char magic1;
	char magic2;
	int size;
	int reserved;
	int offset;
	int infosize;
	int width;
	int height;
	short planes;
	short bits;
	int compression;
	int imagesize;
	int xres;
	int yres;
	int ncolors;
	int icolors;
};
int bitmap_save_bmp(struct bitmap *m, const char *path)
{
	FILE *file;
	struct bmp_header header;
	int i, j;
	unsigned char *scanline, *s;
	file = fopen(path, "wb");
	if(!file)
		return 0;
	memset(&header, 0, sizeof(header));
	header.magic1 = 'B';
	header.magic2 = 'M';
	header.size = m->width * m->height * 3;
	header.offset = sizeof(header);
	header.infosize = sizeof(header) - 14;
	header.width = m->width;
	header.height = m->height;
	header.planes = 1;
	header.bits = 24;
	header.compression = 0;
	header.imagesize = m->width * m->height * 3;
	header.xres = 1000;
	header.yres = 1000;
	fwrite(&header, 1, sizeof(header), file);

	/* if the scanline is not a multiple of four, round it up. */
	int padlength = 4 - (m->width * 3) % 4;
	if(padlength == 4)
		padlength = 0;
	scanline = malloc(header.width * 3);
	for(j = 0; j < m->height; j++) {
		s = scanline;
		for(i = 0; i < m->width; i++) {
			int rgba = bitmap_get(m, i, j);
			*s++ = GET_BLUE(rgba);
			*s++ = GET_GREEN(rgba);
			*s++ = GET_RED(rgba);
		} fwrite(scanline, 1, m->width * 3, file);
		fwrite(scanline, 1, padlength, file);
	} free(scanline);
	fclose(file);
	return 1;
}

struct bitmap *bitmap_load_bmp(const char *path)
{
	FILE *file;
	int size;
	struct bitmap *m;
	struct bmp_header header;
	int i;
	file = fopen(path, "rb");
	if(!file)
		return 0;
	fread(&header, 1, sizeof(header), file);
	if(header.magic1 != 'B' || header.magic2 != 'M') {
		printf("bitmap: %s is not a BMP file.\n", path);
		fclose(file);
		return 0;
	}
	if(header.compression != 0 || header.bits != 24) {
		printf("bitmap: sorry, I only support 24-bit uncompressed bitmaps.\n");
		fclose(file);
		return 0;
	}
	m = bitmap_create(header.width, header.height);
	if(!m) {
		fclose(file);
		return 0;
	}
	size = header.width * header.height;
	for(i = 0; i < size; i++) {
		int r, g, b;
		b = fgetc(file);
		g = fgetc(file);
		r = fgetc(file);
		if(b == 0 && g == 0 && r == 0) {
			m->data[i] = 0;
		} else {
			m->data[i] = MAKE_RGBA(r, g, b, 255);
		}
	}
	fclose(file);
	return m;
}

struct pcx_header {
	unsigned char manufacturer;
	unsigned char version;
	unsigned char encoding;
	unsigned char bitsperpixel;
	unsigned short xmin;
	unsigned short ymin;
	unsigned short xmax;
	unsigned short ymax;
	unsigned short xdpi;
	unsigned short ydpi;
	unsigned char palette[48];
	unsigned char reserved;
	unsigned char colorplanes;
	unsigned short bytesperline;
	unsigned short palettetype;
	unsigned char filler[58];
};
static unsigned char pcx_rle_repeat = 0;
static unsigned char pcx_rle_value = 0;
static unsigned char pcx_rle_read(FILE * file)
{
	unsigned char c;
	  retry:if(pcx_rle_repeat > 0) {
		pcx_rle_repeat--;
		return pcx_rle_value;
	}
	c = fgetc(file);
	if(c == 255) {
		return c;
	} else if(c >= 0xc0) {
		pcx_rle_repeat = c & 0x3f;
		pcx_rle_value = fgetc(file);
		goto retry;
	} else {
		return c;
	}
}

struct bitmap *bitmap_load_pcx(const char *path)
{
	FILE *file;
	int size;
	struct bitmap *m;
	struct pcx_header header;
	int width, height;
	int *palette;
	int palettesize;
	int i, j, p;
	pcx_rle_repeat = 0;
	pcx_rle_value = 0;
	file = fopen(path, "rb");
	if(!file)
		return 0;
	fread(&header, 1, sizeof(header), file);
	if(header.manufacturer != 0x0a || header.encoding != 0x01) {
		printf("bitmap: %s is not a PCX file.\n", path);
		fclose(file);
		return 0;
	}
	width = header.xmax - header.xmin + 1;
	height = header.ymax - header.ymin + 1;
	size = width * height;
	m = bitmap_create(width, height);
	if(!m) {
		fclose(file);
		return 0;
	}
	bitmap_reset(m, 0);
	if(header.colorplanes == 1) {

		/* This is a palette based image */
		if(header.bitsperpixel == 4) {
			palette = malloc(sizeof(int) * 16);
			palettesize = 16;
			fseek(file, 16, SEEK_SET);
		} else if(header.bitsperpixel == 8) {
			palette = malloc(sizeof(int) * 256);
			palettesize = 256;
			fseek(file, -768, SEEK_END);
		} else {
			printf("bitmap: %s has %d bits per pixel, I don't support that...\n", path, header.bitsperpixel);
			fclose(file);
			free(m);
			return 0;
		}
		for(i = 0; i < palettesize; i++) {
			int r, g, b;
			b = fgetc(file);
			g = fgetc(file);
			r = fgetc(file);
			palette[i] = MAKE_RGBA(r, g, b, 255);
		} fseek(file, sizeof(struct pcx_header), SEEK_SET);
		i = 0;
		while(i < size) {
			unsigned char c = pcx_rle_read(file);
			m->data[i] = palette[c];
		} free(palette);
	} else {
		for(j = height - 1; j >= 0; j--) {
			for(p = 0; p < 3; p++) {
				for(i = 0; i < width; i++) {
					unsigned char c = pcx_rle_read(file);
					m->data[j * width + i] |= ((int) c) << (8 * p);
				}
			}
		}
	} fclose(file);
	return m;
}


#define SGI_RGB_MAGIC 0x01da
#define SGI_RGB_MAGIC_SWAPPED 0xda01
struct sgi_rgb_header {
	unsigned short magic;
	char compressed;
	char bytes_per_channel;
	unsigned short dimensions;
	unsigned short xsize, ysize, zsize;
	int pixmin, pixmax;
	int dummy;
	char name[80];
	int colorbitmap;
	char pad[404];
};
static void swap_short(void *vdata, int length)
{
	int i;
	char *data = vdata;
	for(i = 0; i < length; i += 2) {
		char t = data[i];
		data[i] = data[i + 1];
		data[i + 1] = t;
	}
}

static void swap_long(void *vdata, int length)
{
	int i;
	char *data = vdata;
	for(i = 0; i < length; i += 4) {
		int a = data[i];
		int b = data[i + 1];
		int c = data[i + 2];
		int d = data[i + 3];
		data[i] = d;
		data[i + 1] = c;
		data[i + 2] = b;
		data[i + 3] = a;
	}
}

struct bitmap *bitmap_load_sgi_rgb(const char *path)
{
	FILE *file = NULL;
	struct bitmap *m = NULL;
	struct sgi_rgb_header header;
	int x, y, z, doswap = 0;
	int *start_table = 0;
	int *length_table = 0;
	int table_length = 0;
	unsigned char *line = NULL;
	file = fopen(path, "rb");
	if(!file)
		return 0;
	fread(&header, 1, sizeof(header), file);
	if(header.magic == SGI_RGB_MAGIC_SWAPPED) {
		doswap = 1;
	}
	if(doswap)
		swap_short(&header, sizeof(header));
	if(header.magic != SGI_RGB_MAGIC) {
		printf("bitmap: %s is not an SGI RGB file.\n", path);
		fclose(file);
		return 0;
	}
	if(header.bytes_per_channel != 1) {
		printf("bitmap: %s: sorry, I can't handle bpc=%d.\n", path, header.bytes_per_channel);
		fclose(file);
		return 0;
	}
	if(header.colorbitmap != 0) {
		printf("bitmap: %s: sorry, I only handle direct color bitmaps\n", path);
		fclose(file);
		return 0;
	}
	if(header.compressed) {
		int result;
		table_length = header.ysize * header.zsize * sizeof(int);
		start_table = malloc(table_length);
		length_table = malloc(table_length);
		if(!start_table || !length_table)
			goto failure;
		fseek(file, 512, SEEK_SET);
		result = fread(start_table, 1, table_length, file);
		if(result != table_length)
			goto failure;
		result = fread(length_table, 1, table_length, file);
		if(result != table_length)
			goto failure;
		if(doswap) {
			swap_long(start_table, table_length);
			swap_long(length_table, table_length);
		}
	}
	m = bitmap_create(header.xsize, header.ysize);
	if(!m) {
		goto failure;
	}
	bitmap_reset(m, 0);
	line = malloc(header.xsize);
	if(!line)
		goto failure;
	for(y = 0; y < header.ysize; y++) {
		for(z = 0; z < header.zsize; z++) {
			x = 0;
			if(header.compressed) {
				int r = 0;
				int rle_offset = start_table[y + header.ysize * z];
				int rle_length = length_table[y + header.ysize * z];
				char *rle_data = malloc(rle_length);
				fseek(file, rle_offset, SEEK_SET);
				fread(rle_data, 1, rle_length, file);
				while(r < rle_length) {
					char count = rle_data[r] & 0x7f;
					char marker = rle_data[r] & 0x80;
					if(!count)
						break;
					r++;
					if(marker) {
						while(count--) {
							line[x++] = rle_data[r++];
						}
					} else {
						while(count--) {
							line[x++] = rle_data[r];
						}
						r++;
					}
				}
				free(rle_data);
			} else {
				fread(line, 1, header.xsize, file);
			}
			for(x = 0; x < header.xsize; x++) {
				m->data[y * header.xsize + x] |= ((int) line[x]) << ((3 - z) * 8);
			}
		}
	}
	free(line);
	fclose(file);
	if(start_table)
		free(start_table);
	if(length_table)
		free(length_table);
	return m;

	failure:
	if(m)
		bitmap_delete(m);
	if(line)
		free(line);
	if(start_table)
		free(start_table);
	if(length_table)
		free(length_table);
	if(file)
		fclose(file);
	return 0;
}


#ifdef HAS_LIBJPEG
struct bitmap *bitmap_load_jpeg(const char *path)
{
	struct jpeg_decompress_struct jinfo;
	struct jpeg_error_mgr jerr;
	FILE *file;
	struct bitmap *m;
	int i, j;
	JSAMPROW scanline;
	file = fopen(path, "rb");
	if(!file)
		return 0;
	jinfo.err = jpeg_std_error(&jerr);
	jpeg_create_decompress(&jinfo);
	jpeg_stdio_src(&jinfo, file);
	jpeg_read_header(&jinfo, TRUE);
	m = bitmap_create(jinfo.image_width, jinfo.image_height);
	if(!m) {
		fclose(file);
		jpeg_destroy_decompress(&jinfo);
		return 0;
	}
	jpeg_start_decompress(&jinfo);
	scanline = malloc(m->width * 3);
	for(i = m->height - 1; i >= 0; i--) {
		jpeg_read_scanlines(&jinfo, &scanline, 1);
		for(j = 0; j < m->width; j++) {
			unsigned char *s = &scanline[j * 3];
			m->data[i * m->width + j] = MAKE_RGBA(s[0], s[1], s[2], 255);
		}
	}
	free(scanline);
	jpeg_finish_decompress(&jinfo);
	jpeg_destroy_decompress(&jinfo);
	fclose(file);
	return m;
}

int bitmap_save_jpeg(struct bitmap *m, const char *path)
{
	struct jpeg_compress_struct jinfo;
	struct jpeg_error_mgr jerr;
	FILE *file;
	int i, j;
	JSAMPROW scanline;
	file = fopen(path, "wb");
	if(!file)
		return 0;
	jinfo.err = jpeg_std_error(&jerr);
	jpeg_create_compress(&jinfo);
	jpeg_stdio_dest(&jinfo, file);
	jinfo.image_width = m->width;
	jinfo.image_height = m->height;
	jinfo.input_components = 3;
	jinfo.in_color_space = JCS_RGB;
	jpeg_set_defaults(&jinfo);
	jpeg_set_quality(&jinfo, 50, 1);
	jpeg_start_compress(&jinfo, TRUE);
	scanline = malloc(m->width * 3);
	for(i = m->height - 1; i >= 0; i--) {
		for(j = 0; j < m->width; j++) {
			unsigned char *s = &scanline[j * 3];
			int pixel = m->data[i * m->width + j];
			s[0] = GET_RED(pixel);
			s[1] = GET_GREEN(pixel);
			s[2] = GET_BLUE(pixel);
		} jpeg_write_scanlines(&jinfo, &scanline, 1);
	} free(scanline);
	jpeg_finish_compress(&jinfo);
	jpeg_destroy_compress(&jinfo);
	fclose(file);
	return 1;
}

#endif /*  */

/* vim: set noexpandtab tabstop=8: */
