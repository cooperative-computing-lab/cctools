#ifndef URL_DECODE_H
#define URL_ENCODE_H

/** @file url_encode.h provides routines for encoding strings according to RFC-2396.
This is typically used to constructing strings that don't have spaces or other
special characters, and can be safely used as file names, URLs, or other identifiers
where special characters are not allowed.
*/

/** Encodes a plain ASCII string into the percent-hex form of RFC 2396.
For example, the string <tt>Let's go</tt> becomes <tt>Let%27s%20go.</tt>
Typically used to encode URLs and Chirp file names.
@param source The plain ASCII input string.
@param target The location of the encoded output string.
@param length The size in bytes of the output string space.
@see url_decode
 */

void url_encode( const char *source, char *target, int length );

/** Decodes an RFC 2396 string into plain ASCII.
For example, the string <tt>Let%27s%20go</tt> becomes <tt>Let's go</tt>.
Typically used to decode URLs and Chirp file names.
@param source The plain ASCII input string.
@param target The location of the encoded output string.
@param length The size in bytes of the output string space.
@see url_encode
 */

void url_decode( const char *source, char *target, int length );

#endif
