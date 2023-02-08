/*
Copyright (C) 2022 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef LINUX_VERSION_H
#define LINUX_VERSION_H

#ifdef __cplusplus
extern "C" {
#endif

extern int linux_major;
extern int linux_minor;
extern int linux_micro;

#define linux_available(major,minor,micro) (linux_major > major || (linux_major == major && (linux_minor > minor || (linux_minor == minor && (linux_micro >= micro)))))

#ifdef __cplusplus
}
#endif

#endif /* LINUX_VERSION_H */
