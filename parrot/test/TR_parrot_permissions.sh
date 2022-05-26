#!/bin/sh

. ../../dttools/test/test_runner_common.sh
. ./parrot-test.sh

PARROT_MOUNTFILE=$PWD/parrot_mount.$PPID
PARROT_TMPDIR=$PWD/parrot_tmp.$PPID

set -x

prepare()
{
  mkdir -p "$PARROT_TMPDIR/subdir"
  mkdir -p "$PARROT_TMPDIR/rw"
  touch "$PARROT_TMPDIR/file1"
  touch "$PARROT_TMPDIR/rw/file2"
  touch "$PARROT_TMPDIR/subdir/file3"

  cat > "$PARROT_MOUNTFILE" <<EOF
/                  rx
/dev/null          rwx
/proc/1            DENY
$PARROT_TMPDIR/rw  rwx
EOF
}

run()
{
  parrot -m "$PARROT_MOUNTFILE" -- sh -c "ls /proc/1/fd" >/dev/null 2>&1
  if [ $? -eq 0 ]; then echo 'ignored DENY'; return 1; fi

  parrot -m "$PARROT_MOUNTFILE" -- sh -c "rm -f $PARROT_TMPDIR/file1" >/dev/null 2>&1
  if [ $? -eq 0 ]; then echo 'ignored read only path'; return 1; fi

  parrot -m "$PARROT_MOUNTFILE" -- sh -c "mv $PARROT_TMPDIR/rw/file2 $PARROT_TMPDIR" >/dev/null 2>&1
  if [ $? -eq 0 ]; then echo 'ignored read only target dir'; return 1; fi

  parrot -m "$PARROT_MOUNTFILE" -- sh -c "echo test >> $PARROT_TMPDIR/file1" >/dev/null 2>&1
  if [ $? -eq 0 ]; then echo 'ignored read only file'; return 1; fi

  parrot -m "$PARROT_MOUNTFILE" -- sh -c "touch $PARROT_TMPDIR/subdir/file3" >/dev/null 2>&1
  if [ $? -eq 0 ]; then echo 'ignored read only subdirectory'; return 1; fi

  parrot -m "$PARROT_MOUNTFILE" -- sh -c "ln /etc/passwd $PARROT_TMPDIR/rw/document" >/dev/null 2>&1
  if [ $? -eq 0 ]; then echo 'allowed potentially dangerous hard link'; return 1; fi

  parrot -m "$PARROT_MOUNTFILE" -- sh -c "echo test >> $PARROT_TMPDIR/rw/file4" >/dev/null 2>&1
  if [ $? -ne 0 ]; then echo 'blocked write'; return 1; fi

  parrot -m "$PARROT_MOUNTFILE" -- sh -c "mkdir $PARROT_TMPDIR/rw/subdir2" >/dev/null 2>&1
  if [ $? -ne 0 ]; then echo 'blocked mkdir'; return 1; fi

  parrot -m "$PARROT_MOUNTFILE" -- sh -c "mv $PARROT_TMPDIR/rw/file4 $PARROT_TMPDIR/rw/subdir2/file4" >/dev/null 2>&1
  if [ $? -ne 0 ]; then echo 'blocked mv'; return 1; fi

  parrot -m "$PARROT_MOUNTFILE" -- sh -c "cp -v $PARROT_TMPDIR/subdir/file3 $PARROT_TMPDIR/rw" >/dev/null 2>&1
  if [ $? -ne 0 ]; then echo 'blocked cp'; return 1; fi


  return 0
}

clean()
{
  rm -rf "$PARROT_TMPDIR"
  rm -f "$PARROT_MOUNTFILE"
  return 0
}

dispatch "$@"

# vim: set noexpandtab tabstop=4:
