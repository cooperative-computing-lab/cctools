#!/bin/bash
set -u

# Runs the regression tests that specifically exercise packages under
# valgrind, one package at a time, using the same
# check_needed/prepare/run/clean protocol as run_all_tests.sh (see
# dttools/test/test_runner_common.sh) but WITHOUT running each package's
# full functional test suite -- most of those tests have nothing to do with
# valgrind and running all of them for every valgrind-covered package is too
# slow for CI (makeflow's suite alone routinely takes several minutes; four
# packages' full suites run serially blew well past a 20-minute budget in
# testing). This script isolates just the valgrind coverage so the job stays
# fast and its failures are unambiguously about memory-safety, not about
# unrelated functional regressions.
#
# Only packages with a dedicated TR_*_valgrind.sh test are listed here --
# not every package has one. See the taskvine tech-debt audit's "valgrind"
# item for how this list was derived; add an entry when a package grows a
# valgrind test of its own.

CCTOOLS_SRC="$(cd "$(dirname "${BASH_SOURCE[0]}")"/../.. && pwd)"
cd "${CCTOOLS_SRC}"

if ! command -v valgrind > /dev/null 2>&1; then
	echo "valgrind is not installed -- see packaging/valgrind/run-valgrind-tests.sh for what this job checks."
	exit 1
fi

# resource_monitor's binary is expected on PATH by some tests, mirroring
# run_all_tests.sh.
export PATH="${CCTOOLS_SRC}/resource_monitor/src:${PATH}"

PACKAGES="taskvine makeflow work_queue resource_monitor"
declare -A VALGRIND_SCRIPT=(
	[taskvine]="TR_vine_valgrind.sh"
	[makeflow]="TR_makeflow_valgrind.sh"
	[work_queue]="TR_work_queue_valgrind.sh"
	[resource_monitor]="TR_rmonitor_valgrind.sh"
)

overall=0

for package in ${PACKAGES}; do
	script="${VALGRIND_SCRIPT[$package]}"
	test_dir="${CCTOOLS_SRC}/${package}/test"

	if [ ! -x "${test_dir}/${script}" ]; then
		echo "=== ${package}: ${script} not found or not executable -- skipping ==="
		continue
	fi

	echo "=== ${package}: ${script} ==="
	(
		cd "${test_dir}" || exit 1

		./"${script}" check_needed
		needed=$?
		if [ "${needed}" -ne 0 ]; then
			echo "--- ${package}/${script}: skipped (check_needed) ---"
			exit 0
		fi

		./"${script}" prepare
		prepare_status=$?
		if [ "${prepare_status}" -ne 0 ]; then
			echo "--- ${package}/${script}: prepare failed ---"
			exit 1
		fi

		./"${script}" run
		run_status=$?

		./"${script}" clean

		if [ "${run_status}" -ne 0 ]; then
			echo "--- ${package}/${script}: FAILED (valgrind found errors, see above) ---"
			exit 1
		fi

		echo "--- ${package}/${script}: passed ---"
		exit 0
	)
	status=$?
	if [ "${status}" -ne 0 ]; then
		overall=1
	fi
done

if [ "${overall}" -ne 0 ]; then
	echo "=== one or more valgrind regression tests failed ==="
else
	echo "=== all valgrind regression tests passed ==="
fi

exit "${overall}"

# vim: set noexpandtab tabstop=4:
