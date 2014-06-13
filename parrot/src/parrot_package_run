#!/bin/sh
#This script can be used to create the mountlist file for executing one pacakge under Parrot.
package_path=""
mountlist=""
show_help()
{
	echo "Options:"
	echo "-m, --mountfile   Set the path of the mountlist file."
	echo "-p, --path                   The path of the package."
	echo "-h, --help                    Show this help message."

	exit 1
}

while [ $# -gt 0 ]
do
	case $1 in
		-p | --path)
			shift
			package_path=$1
			;;
		-m | --mountfile)
			shift
			mountlist=$1
			;;
		-h | --help)
			show_help
			;;
		*)
			break
			;;
	esac
	shift
done

#construct mountlist
if [ -e $mountlist ]; then
rm $mountlist
fi

package_path=`readlink -f $package_path`
echo "/ $package_path" >> $mountlist
echo "$package_path $package_path" >> $mountlist
cat $package_path/common-mountlist >> $mountlist
