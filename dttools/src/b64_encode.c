#include "b64_encode.h"
#include <stdio.h>


const char b64_table[] = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/=";

int b64_encode(const char *input, int len, char *output, int buf_len)
{
	int i, out_len;

	out_len = (len / 3) * 4;
	if(len % 3)
		out_len += 4;
	if(buf_len < out_len)
		return -1;

	for(i = 0; i < len / 3; i++) {
		output[4 * i] = b64_table[(input[3 * i] >> 2) & 0x3F];
		output[4 * i + 1] = b64_table[((input[3 * i] << 4) & 0x30) + ((input[3 * i + 1] >> 4) & 0x0F)];
		output[4 * i + 2] = b64_table[((input[3 * i + 1] << 2) & 0x3C) + ((input[3 * i + 2] >> 6) & 0x03)];
		output[4 * i + 3] = b64_table[input[3 * i + 2] & 0x3F];
	}

	switch (len % 3) {
	case 2:
		output[4 * i] = b64_table[(input[3 * i] >> 2) & 0x3F];
		output[4 * i + 1] = b64_table[((input[3 * i] << 4) & 0x30) + ((input[3 * i + 1] >> 4) & 0x0F)];
		output[4 * i + 2] = b64_table[(input[3 * i + 1] << 2) & 0x3C];
		output[4 * i + 3] = '=';
		break;
	case 1:
		output[4 * i] = b64_table[(input[3 * i] >> 2) & 0x3F];
		output[4 * i + 1] = b64_table[(input[3 * i] << 4) & 0x30];
		output[4 * i + 2] = '=';
		output[4 * i + 3] = '=';
		break;
	default:
		break;
	}

	return 0;
}
