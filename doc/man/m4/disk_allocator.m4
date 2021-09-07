include(manual.h)dnl
HEADER(disk_allocator)

SECTION(NAME)
BOLD(disk_allocator) - tool for creating and deleting loop device allocations of a given size.

SECTION(SYNOPSIS)
CODE(disk_allocator [options] PARAM(create|delete) PARAM(target directory) PARAM(size) PARAM(filesystem))

SECTION(DESCRIPTION)

BOLD(disk_allcator) is a tool for creating and deleting loop device allocations
of a given size in order to sandbox an application. For creating an allocation,
it accepts a desired location for the device, the size for the sandbox, and the
filesystem to mount. For deleting an allocation, BOLD(disk_allocator) needs only
the directory of the mounted loop device which is to be removed.

PARA

You will need superuser priveleges to run BOLD(disk_allocator) on your local machine.

PARA

SECTION(OPTIONS)
OPTIONS_BEGIN
OPTION_FLAG(h,help)Show this help screen.
OPTION_FLAG(v,version)Show version string.
OPTIONS_END

SECTION(EXIT STATUS)
On success, returns zero.  On failure, returns non-zero.

SECTION(EXAMPLES)

Create a disk allocation:
LONGCODE_BEGIN
disk_allocator create /tmp/test 100MB ext2
LONGCODE_END

Delete a disk allocation:
LONGCODE_BEGIN
disk_allocator delete /tmp/test
LONGCODE_END

SECTION(COPYRIGHT)

COPYRIGHT_BOILERPLATE

FOOTER
