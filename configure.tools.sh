save_arguments()
{
cat >configure.rerun <<EOF
#!/bin/sh
$@
EOF
chmod 755 configure.rerun
}

# Some shells (like /bin/sh on osx) do not support echo -n,
# so we use this function as an alias instead.

isodate() {
	date +'%Y-%m-%d %H:%M:%S %z'
}
NOW=$(isodate)

echon()
{
	printf "%s" "$@"
}

# substr <s> <pos> <len>
substr() {
	local str="$1"
	local i="$2"
	local j="$3"

	if [ -z "$i" ]; then
		i=0
	fi
	if [ -z "$j" ]; then
		j=-0
	fi
	printf "%s" "$str" | tail -c "+$i" | head -c "$j"
}

abspath() {
	case "$1" in
		/*)
			echo "$1"
			;;
		*)
			echo "$(pwd)/$1"
			;;
	esac
}

check_file ()
{
	echon "checking for $1..."
	if [ -f $1 ]
	then
		echo "yes"
		return 0
	else
		echo "no"
		return 1
	fi
}

check_path ()
{
	echon "checking for executable $1..."

	local check_path_var=$1
	if [ "${check_path_var}" != "${check_path_var#/}" ]
	then
		if [ -f "${check_path_var}" -a -x "${check_path_var}" ]
		then
			echo "yes"
			return 0
		else
			echo "no"
			return 1
		fi
	fi

	IFS=":"
	for dir in $PATH
	do
		if [ -f $dir/$1 -a -x $dir/$1 ]
		then
			echo $dir/$1
			IFS=" "
			return 0
		fi
	done
	echo "no"
	IFS=" "
	return 1
}

search_file_executable() {
	executable_search_result=""
	for candidate in "$@"
	do
		if check_path ${candidate}
		then
			executable_search_result=$(which ${candidate})
			return 0
		fi
	done

	return 1
}



check_library_static() {
	local libdir
	local library

	library="lib$1.a"
	libdir="$2"

	if check_file "$libdir/$library"
	then
		return 0
	else
		return 1
	fi
}

check_library_dynamic() {
	local library
	local libdir

	library="$1"
	libdir="$2"

	echon "checking for lib$1 in $2..."

	if echo 'int main;' | ${CC:-gcc} -o /dev/null -x c - "-L${libdir}" "-l${library}" >/dev/null 2>/dev/null; then
		echo "yes"
		return 0
	else
		echo "no"
		return 1
	fi
}

check_function()
{
	local func
	local header

	name="$1"
	shift

	header="$1"
	shift

	compiler_options="$@"

	echon "checking for $name in $header..."

cat > .configure.tmp.c << EOF
#define _GNU_SOURCE
#include <stdlib.h>
#include <${header}>
int main(int argc, char **argv) {
	${name};
	return 0;
}
EOF
	if ${CC:-gcc} ${CFLAGS} .configure.tmp.c -c -o .configure.tmp.o ${compiler_options} > .configure.tmp.out 2>&1; then
		echo yes
		rm -f .configure.tmp.c .configure.tmp.out
		return 0
	else
		echo no
		rm -f .configure.tmp.c .configure.tmp.out
		return 1
	fi
}

check_compiler_flag()
{
	echon "checking if ${ccompiler} supports $1..."
cat > .configure.tmp.c << EOF
#include <stdlib.h>
EOF
	if ${ccompiler} $1 .configure.tmp.c -c -o .configure.tmp.o > .configure.tmp.out 2>&1
	then
		echo "yes"
		return 0
	else
		echo "no"
		return 1
	fi
}

require_file ()
{
	if check_file $1
	then
		return 0
	else
		echo "Sorry, I can't proceed without file $1";
		exit 1
	fi
}

optional_path ()
{
	if check_path $1
	then
		return 0
	else
		return 1
	fi
}

require_path ()
{
	if optional_path $1
	then
		return 0
	else
		echo "Sorry, I can't proceed without program $1";
		exit 1
	fi
}

require_function ()
{
	if check_function "$1" "$2"
	then
		return 0
	else
		echo "Sorry, I can't proceed without function $1";
		exit 1
	fi
}

library_search_mode=prefer_dynamic

#
# library search looks for libraries according to a variety of rules,
# then returns the result in the global variable library_search_result
#

library_search()
{
	if library_search_normal $@;
	then
		return 0
	fi
	if [ "X${HOST_MULTIARCH}" != X ] && library_search_multiarch $@;
	then
		return 0
	fi

	return 1
}

library_search_multiarch()
{
	if [ X$3 = X ];
	then
		library_search_normal $1 $2 $HOST_MULTIARCH
	else
		library_search_normal $1 $2 $HOST_MULTIARCH/$3
	fi
}

library_search_normal()
{
	local lib
	local basedir
	local libdir

	lib="$1"
	basedir="$2"
	subdir="$3"

	# Must clear out any previous values from the global
	library_search_result=""

	# If the second argument is root, and we are not careful,
	# we will end up with a path that has two slashes, which
	# means something unintended in Windows

	if [ "$basedir" = / ]
	then
		basedir=
	fi

	# If a third argument is given, it is interpreted as a subdirectory where
	# the libraries might be. Useful when several architectures live in the
	# same basedir.
	if [ -n "$subdir" ]
	then
		subdir="/$subdir"
	else
		subdir=""
	fi

	arch_dirs="${basedir}/lib ${basedir}"

	# If we are running on a 64-bit platform, then give preference to 
	# libraries in /lib64.

	if [ $BUILD_CPU = X86_64 -a -d $2/lib64 ]
	then
		arch_dirs="${basedir}/lib64 ${arch_dirs}"
	fi

	# Now check for the library file in all of the known places,
	# and add it to the link line as appropriate for the type and platform.

	for arch_dir in $arch_dirs
	do
		# check if the subdirectory exists
		libdir="$arch_dir$subdir"
		if [ ! -d "$libdir" ]
		then
			libdir="$arch_dir"
		fi

		if [ $library_search_mode = prefer_static -o $library_search_mode = require_static ]
		then
			if check_library_static "$lib" "$libdir"
			then
				library_search_result="$libdir/lib${lib}.a"
				return 0
			fi
		fi

		if [ $library_search_mode != require_static ]
		then
			if check_library_dynamic "$lib" "$libdir"
			then
				if [ "$libdir" != /lib -a "$libdir" != /lib64 -a "$libdir" != /usr/lib -a "$libdir" != /usr/lib64 ]
				then
					library_search_result="-L${libdir} -l${lib}"
				else
					library_search_result="-l${lib}"
				fi

				if [ $BUILD_SYS = DARWIN ]
				then
					library_search_result="${library_search_result} -rpath $libdir"
				fi

				return 0
			fi
		fi

		if [ $library_search_mode = prefer_dynamic ]
		then
			if check_library_static "$lib" "$libdir"
			then
				library_search_result="$libdir/lib${lib}.a"
				return 0
			fi
		fi
	done

	return 1
}

ccflags_append()
{
	for arg; do
		ccflags="${ccflags} ${arg}"
	done
}

ccflags_append_define()
{
	for arg; do
		ccflags="${ccflags} -D${arg}"
	done
}

optional_function()
{
	name="$1"
	header="$2"
	shift
	shift

	multiarch_include=$(echo "${header}" | sed "s|include/|include/$HOST_MULTIARCH/|")

	if check_function "$name" "$header"; then
		ccflags_append_define "$@"
		return 0
	elif [ -n "$HOST_MULTIARCH" ] && check_function "$name" "$multiarch_include"; then
		ccflags_append_define "$@"
		return 0
	fi

	return 1
}

optional_file()
{
	file="$1"
	shift
	if check_file "$file"; then
		ccflags_append_define "$@"
		return 0
	else
		return 1
	fi
}

optional_include()
{
	header="$1"
	shift
	
	echon "checking for header ${header}..."

cat > .configure.tmp.c << EOF
#include <stdlib.h>
#include <${header}>
EOF
	if ${CC:-gcc} ${CFLAGS} .configure.tmp.c -c -o .configure.tmp.o > .configure.tmp.out 2>&1; then
		echo yes
		rm -f .configure.tmp.c .configure.tmp.out
		ccflags_append_define "$@"
		return 0
	else
		echo no
		rm -f .configure.tmp.c .configure.tmp.out
		return 1
	fi
}

optional_library()
{
	library="$1"
	shift

	echon "checking for library ${library}..."

	if echo 'int main;' | ${CC:-gcc} -o /dev/null -x c - "-l${library}" >/dev/null 2>/dev/null; then
		echo yes
		ccflags_append_define "$@"
		return 0
	else
		echo no
		return 1
	fi
}

optional_library_function()
{
	h="$1"
	f="$2"
	l="$3"
	shift
	shift
	shift

	echon "checking for library function ${f}..."

	${CC:-gcc} -o /dev/null -x c - "-l${l}" >/dev/null 2>/dev/null <<EOF
#include <stdlib.h>
#include <${h}>

int main (int argc, char *argv[])
{
	(void)${f};
	return 0;
}
EOF
	if [ $? -eq 0 ]; then
		echo yes
		ccflags_append_define "$@"
		return 0
	else
		echo no
		return 1
	fi
}

check_gnu_make()
{
	if check_path $1
	then
		echon "checking if $1 is GNU make..."
		kind=`$1 -v 2>&1| head -1 | awk '{print $1}'`
		if [ X$kind = XGNU ]
		then
			echo "yes"
			MAKE=$1
			export MAKE
			return 0
		else
			echo "no"
		fi
	fi

	return 1
}

require_gnu_make()
{
	echo "checking for GNU make in several places..."
	if check_gnu_make make
	then
		return 0
	else
		if check_gnu_make gmake
		then
			return 0
		else
			if check_gnu_make gnumake
			then
				return 0
			fi
		fi
	fi

	echo "Sorry, you must have GNU make/gmake/gnumake in your path."
	exit 1
}

check_perl_version()
{
	if check_path "perl"
	then
		echon "checking perl version..."
		cat >configure.perl-test <<EOF
print "\$]\n";

\$v = \$ARGV[0]+\$ARGV[1]/1000+\$ARGV[2]/1000000;

if(\$v>=\$]) {
		exit 1;
} else  {
		exit 0;
}
EOF
		if perl configure.perl-test $1 $2 $3
		then
			return 0
		else
			return 1
		fi
	fi

	return 1
}

fix_globus_install()
{
	if check_file "${1}/include/${2}/globus_config.h"
	then
		echo "globus source install has been configured properly"
	else
		echo "*** globus source install has not been configured properly"
		echo "*** try running \"gpt-build -nosrc $2\" to set it up properly."
		exit 1
	fi
}

check_for_globus_flavors()
{
	echo "examining the globus installation in $1..."

	if check_perl_version 5 5 0
	then
		echo "perl version is ok"
	else
		echo "*** sorry, the globus build tools require"
		echo "*** perl >= v5.5.0 in order to even examine the installation."
		exit 1
	fi

	#
	# globus-makefile-header seems to move around.
	# Use -f rather than -x, because the AFS acl may
	# not match the UNIX perms.
	#

	GMH=$1/sbin/globus-makefile-header
	echo "checking for $GMH..."
	if [ ! -f "${GMH}" ]
	then
		GMH=$1/bin/globus-makefile-header
		echo "checking for $GMH..."
		if [ ! -f "${GMH}" ]
		then
			echo "not found"
			return 1
		fi
	fi

	#
	# Some installations have a trailing " " afer the pound-bang
	# line, so we'll try to perl it directly if it looks like
	# a perl script.  However, we don't want to universally assume
	# that it is perl without some evidence.
	#

	if head -1 $GMH | grep perl > /dev/null 2>&1
	then
		GMH="perl ${GMH}"
	fi

	#
	# Now search for any flavor we can think of...
	#

	for compiler in gcc64 gcc32 vendorcc32
	do
		for debug in "" dbg
		do
			if check_for_globus "$1" "$2" "${compiler}${debug}" "${GMH}"
			then
				return 0
			fi
		done
	done

	return 1
}

check_for_globus()
{
	echon "checking for globus package $2 flavor $3 in $1..."

	GLOBUS_LOCATION=$1
	export GLOBUS_LOCATION

	#
	# Run globus-makefile-header and dump the output into
	# a Makefile.  Note that the single-dash option form
	# stops working with version > 2.0
	#

	$4 --static --flavor=$3 $2 2> config.mk.globus.errors > config.mk.globus

	#
	# This script seems to always exit with status zero,
	# even if it detects an error.
	#

	if [ $? -eq 0 ]
	then
		#
		# Some busted versions put error messages
		# in the output Makefile itself.
		#

		if grep ERROR config.mk.globus > /dev/null
		then
			#
			# On the other hand, some working versions have
			# the string ERROR in a package name!
			#

			if grep ERROR_VERSION config.mk.globus > /dev/null
			then
				echo "tricky, but yes!"
				return 0
			else
				rm config.mk.globus
				echo "broken"
				return 1
			fi
		else
			echo "yes"
			return 0
		fi
	else
		#
		# Never seen this happen, but perhaps the script
		# will indicate the package is not present
		#

		echo "no"
		return 1
	fi
}

check_multiarch()
{
	if [ -r /etc/debian_version ]; then
		HOST_MULTIARCH="$(uname -m)-linux-gnu"
	else
		HOST_MULTIARCH=
	fi

	# echo to capture result
	echo "$HOST_MULTIARCH"
}

format_version()
{
	echo "$@" | awk -F. '{printf("%d%03d%03d%03d", $1, $2, $3, $4); }'
}

config_X_path()
{
	if [ "$1" = no ]
	then
		echo no
	else
		echo yes
	fi
}

report_detection()
{
	package=$1
	available=$2
	required=$3
	path_tried=$4

	if [ "${available}" = yes ]
	then
		echo "compiling with ${package} support"
	elif [ "${required}" = no ]
	then
		echo "${package} support explicitly disabled"
	elif [ "${required}" = yes ]
	then
		echo "*** Sorry, I couldn't find ${package} in ${path_tried}"
		echo "*** Check --with-${package}-path and try again"
		exit 1
	else
		echo "skipping ${package} support as it could not be automatically detected"
	fi
}

# vim: set noexpandtab tabstop=4:
