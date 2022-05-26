#include "catch.h"
#include "debug.h"

#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

#include <assert.h>
#include <errno.h>
#include <inttypes.h>
#include <limits.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>

typedef uint32_t Elf32_Addr;
typedef uint16_t Elf32_Half;
typedef uint32_t Elf32_Off;
typedef int32_t  Elf32_Sword;
typedef uint32_t Elf32_Word;

typedef uint64_t Elf64_Addr;
typedef uint16_t Elf64_Half;
typedef uint64_t Elf64_Off;
typedef uint32_t Elf64_Word;
typedef int32_t  Elf64_Sword;

typedef uint64_t Elf32_Xword;
typedef int64_t  Elf32_Sxword;
typedef uint64_t Elf64_Xword;
typedef int64_t  Elf64_Sxword;

#define EI_NIDENT 16
#define ELFMAG "\x7f" "ELF"

/* indexes */
#define EI_MAG0 0
#define EI_MAG1 1
#define EI_MAG2 2
#define EI_MAG3 3
#define EI_CLASS 4
#define EI_DATA 5
#define EI_VERSION 6
#define EI_PAD 7

#define ELFCLASS32 1
#define ELFCLASS64 2

typedef struct {
	unsigned char e_ident[EI_NIDENT];
	Elf32_Half e_type;
	Elf32_Half e_machine;
	Elf32_Word e_version;
	Elf32_Addr e_entry;
	Elf32_Off e_phoff;
	Elf32_Off e_shoff;
	Elf32_Word e_flags;
	Elf32_Half e_ehsize;
	Elf32_Half e_phentsize;
	Elf32_Half e_phnum;
	Elf32_Half e_shentsize;
	Elf32_Half e_shnum;
	Elf32_Half e_shstrndx;
} Elf32_Ehdr;

typedef struct {
	unsigned char e_ident[EI_NIDENT];
	Elf64_Half e_type;
	Elf64_Half e_machine;
	Elf64_Word e_version;
	Elf64_Addr e_entry;
	Elf64_Off e_phoff;
	Elf64_Off e_shoff;
	Elf64_Word e_flags;
	Elf64_Half e_ehsize;
	Elf64_Half e_phentsize;
	Elf64_Half e_phnum;
	Elf64_Half e_shentsize;
	Elf64_Half e_shnum;
	Elf64_Half e_shstrndx;
} Elf64_Ehdr;

typedef struct {
	Elf32_Word p_type;
	Elf32_Off p_offset;
	Elf32_Addr p_vaddr;
	Elf32_Addr p_paddr;
	Elf32_Word p_filesz;
	Elf32_Word p_memsz;
	Elf32_Word p_flags;
	Elf32_Word p_align;
} Elf32_Phdr;

typedef struct {
	Elf64_Word p_type;
	Elf64_Word p_flags;
	Elf64_Off p_offset;
	Elf64_Addr p_vaddr;
	Elf64_Addr p_paddr;
	Elf64_Xword p_filesz;
	Elf64_Xword p_memsz;
	Elf64_Xword p_align;
} Elf64_Phdr;

#define PT_INTERP 3

#define p16(e) debug(D_DEBUG, "%s = %" PRIu16, #e, e)
#define p32(e) debug(D_DEBUG, "%s = %" PRIu32, #e, e)
#define p64(e) debug(D_DEBUG, "%s = %" PRIu64, #e, e)

/* XXX We don't handle endianness of the object file */
static int elf_interp(int fd, int set, char *interp)
{
	int rc;
	void *addr = NULL;
	size_t addr_len;
	unsigned char *ident;
	struct stat info;

	CATCHUNIX(fstat(fd, &info));

	if (info.st_size < (off_t)sizeof(Elf32_Ehdr))
		CATCH(ENOEXEC);

	addr_len = info.st_size;
	addr = mmap(NULL, addr_len, PROT_READ|(set ? PROT_WRITE : 0), MAP_SHARED, fd, 0);
	CATCHUNIX(addr == NULL ? -1 : 0);

	if (strncmp((const char *)addr, ELFMAG, sizeof(ELFMAG)-1) != 0)
		CATCH(ENOEXEC);

	ident = (unsigned char *)addr;
	switch (ident[EI_CLASS]) {
		case ELFCLASS32: {
			int i;
			Elf32_Phdr *phdr;
			Elf32_Ehdr *hdr = addr;
			p16(hdr->e_type);
			p32(hdr->e_phoff);
			p16(hdr->e_phentsize);
			p16(hdr->e_phnum);

			phdr = addr+sizeof(*hdr);
			for (i = 0; i < hdr->e_phnum; i++) {
				if ((uintptr_t)&phdr[i+1] >= ((uintptr_t)addr+addr_len))
					CATCH(ENOEXEC);
				p32(phdr[i].p_type);
				p32(phdr[i].p_offset);
				p32(phdr[i].p_filesz);
				if (phdr[i].p_type == PT_INTERP) {
					const char *old = (const char *)addr+phdr[i].p_offset;
					if (set) {
						/* So the basic idea here is that we just add 4096
						 * bytes (PATH_MAX) to the end of the file and point to
						 * that. It's not that inefficient and we can skip
						 * fixing file offsets in all the ELF headers.
						 */
						debug(D_DEBUG, "old interp: '%s'", old);
						phdr[i].p_offset = addr_len;
						phdr[i].p_filesz = PATH_MAX;
						CATCHUNIX(pwrite(fd, interp, PATH_MAX, phdr[i].p_offset));
					} else {
						strcpy(interp, old);
					}
					rc = 0;
					goto out;
				}
			}
			CATCH(EINVAL);
			break;
		}
		case ELFCLASS64: {
			int i;
			Elf64_Phdr *phdr;
			Elf64_Ehdr *hdr = addr;
			p16(hdr->e_type);
			p64(hdr->e_phoff);
			p16(hdr->e_phentsize);
			p16(hdr->e_phnum);

			phdr = addr+sizeof(*hdr);
			for (i = 0; i < hdr->e_phnum; i++) {
				if ((uintptr_t)&phdr[i+1] >= ((uintptr_t)addr+addr_len))
					CATCH(ENOEXEC);
				p32(phdr[i].p_type);
				p64(phdr[i].p_offset);
				p64(phdr[i].p_filesz);
				if (phdr[i].p_type == PT_INTERP) {
					const char *old = (const char *)addr+phdr[i].p_offset;
					if (set) {
						/* So the basic idea here is that we just add 4096
						 * bytes (PATH_MAX) to the end of the file and point to
						 * that. It's not that inefficient and we can skip
						 * fixing file offsets in all the ELF headers.
						 */
						debug(D_DEBUG, "old interp: '%s'", old);
						phdr[i].p_offset = addr_len;
						phdr[i].p_filesz = PATH_MAX;
						CATCHUNIX(pwrite(fd, interp, PATH_MAX, phdr[i].p_offset));
					} else {
						strcpy(interp, old);
					}
					rc = 0;
					goto out;
				}
			}
			CATCH(EINVAL);
			break;
		}
		default:
			CATCH(ENOEXEC);
			break;
	}

	rc = 0;
	goto out;
out:
	if (addr) {
		munmap(addr, addr_len);
	}
	return RCUNIX(rc);
}

int elf_get_interp(int fd, char *interp)
{
	return elf_interp(fd, 0, interp);
}


int elf_set_interp(int fd, const char *interp)
{
	char path[PATH_MAX] = "";

	if (strlen(interp) >= PATH_MAX)
		return errno = ENAMETOOLONG, -1;
	strcpy(path, interp);

	return elf_interp(fd, 1, path);
}

/* vim: set noexpandtab tabstop=4: */
