#! /bin/bash
set -e

# sort and comm must agree on collation, or comm spuriously reports
# "not in sorted order" on perfectly sorted input (locale-dependent
# collation can disagree with itself across the two calls) and aborts the
# script via set -e before it can report its outcome.
export LC_ALL=C

# Find cctools src directory
CCTOOLS_SRC="$(cd "$(dirname "${BASH_SOURCE[0]}")"/../.. && pwd)"
cd "${CCTOOLS_SRC}"

# Static analysis via clang's static analyzer (scan-build); see the
# taskvine tech-debt audit's "static analysis" item. Covers every C package
# under CCTOOLS_PACKAGES instead of taskvine only. doc and poncho are
# excluded: they aren't C.
C_PACKAGES="dttools batch_job taskvine grow makeflow work_queue ftp_lite resource_monitor chirp deltadb"

# --- locate scan-build and the ccc-analyzer/c++-analyzer wrapper scripts it needs ---

SCAN_BUILD=""
if command -v scan-build > /dev/null 2>&1
then
	SCAN_BUILD="scan-build"
else
	# Debian/Ubuntu package versioned scan-build as scan-build-<N> with no
	# unversioned symlink guaranteed; search newest-first down to the
	# oldest LLVM release still plausibly present on a runner.
	for version in $(seq 30 -1 15)
	do
		if command -v "scan-build-${version}" > /dev/null 2>&1
		then
			SCAN_BUILD="scan-build-${version}"
			break
		fi
	done
fi

if [ -z "${SCAN_BUILD}" ]
then
	echo "scan-build (clang static analyzer) is not installed -- see packaging/lint/scan-build.sh for what this job checks."
	exit 1
fi

CCC_ANALYZER=""
CXX_ANALYZER=""
for dir in "${CONDA_PREFIX:-}/libexec" /usr/lib/llvm-*/libexec /usr/share/clang/scan-build*/libexec /usr/libexec
do
	if [ -x "${dir}/ccc-analyzer" ] && [ -x "${dir}/c++-analyzer" ]
	then
		CCC_ANALYZER="${dir}/ccc-analyzer"
		CXX_ANALYZER="${dir}/c++-analyzer"
		break
	fi
done

if [ -z "${CCC_ANALYZER}" ]
then
	echo "found ${SCAN_BUILD} but couldn't locate its ccc-analyzer/c++-analyzer wrapper scripts (looked under /usr/lib/llvm-*/libexec, /usr/share/clang/scan-build*/libexec, /usr/libexec) -- see packaging/lint/scan-build.sh."
	exit 1
fi

echo "=== using ${SCAN_BUILD}, analyzers at $(dirname "${CCC_ANALYZER}") ==="

# --- match the analyzer's "real" compiler to the one this tree is configured with ---
#
# ccc-analyzer/c++-analyzer run the static analyzer AND do the actual
# compile, but if CCC_CC/CCC_CXX aren't set they default to plain gcc/g++
# off PATH rather than whatever this tree was configured with (see
# CCTOOLS_CC/CCTOOLS_CXX in config.mk). In an environment where those
# differ -- e.g. a conda dev shell, where PATH's gcc is the distro's
# system compiler/glibc but ./configure picked conda's -- that mismatch
# can trip real compiler warnings-as-errors under --strict that don't
# occur with the compiler this tree actually builds with, and don't occur
# in CI either (a plain Ubuntu runner only ever has the one gcc). Pulling
# the real compiler out of config.mk keeps the analyzer's compile step
# consistent with the rest of the build.
CONFIG_MK="${CCTOOLS_SRC}/config.mk"
if [ -f "${CONFIG_MK}" ]
then
	CONFIGURED_CC="$(sed -n 's/^CCTOOLS_CC=.*;//p' "${CONFIG_MK}")"
	CONFIGURED_CXX="$(sed -n 's/^CCTOOLS_CXX=.*;//p' "${CONFIG_MK}")"
	if [ -n "${CONFIGURED_CC}" ] && [ -n "${CONFIGURED_CXX}" ]
	then
		export CCC_CC="${CONFIGURED_CC}"
		export CCC_CXX="${CONFIGURED_CXX}"
		echo "=== analyzer's real compiler set to configured CCTOOLS_CC/CCTOOLS_CXX: ${CCC_CC} / ${CCC_CXX} ==="
	fi
fi

# --- run the analysis, twice ---
#
# cctools' build uses its own CCTOOLS_CC/CCTOOLS_CXX make variables rather
# than the standard CC/CXX that scan-build auto-intercepts, so the analyzer
# wrappers are injected directly via those variables instead (see rules.mk).
# A clean build is required first each time: scan-build only sees compiles
# that actually happen, so any already-built .o files from a previous step
# would silently go unanalyzed.
#
# The clang static analyzer's path exploration is not fully deterministic
# run-to-run for the *same* unchanged source (observed directly while
# building this job: ~2% of findings shifted location or appeared/
# disappeared between two consecutive clean runs of identical code). To
# keep the gate from failing CI on that inherent tool noise, a finding only
# counts as "new" if it shows up in BOTH of two independent runs; anything
# that only appears once is treated as analyzer flakiness, not a real new
# issue. This roughly doubles the job's runtime (a few minutes each way)
# in exchange for not being a flaky gate.

run_scan_build()
{
	out_dir="$1"
	make clean > /dev/null
	# shellcheck disable=SC2086
	"${SCAN_BUILD}" -o "${out_dir}" --keep-empty make -j"$(nproc)" \
		CCTOOLS_CC="@echo COMPILE \$@;${CCC_ANALYZER}" \
		CCTOOLS_CXX="@echo COMPILE \$@;${CXX_ANALYZER}" \
		${C_PACKAGES}
}

OUT_DIR_1="$(mktemp -d)"
OUT_DIR_2="$(mktemp -d)"
FOUND_1="$(mktemp)"
FOUND_2="$(mktemp)"
CONFIRMED="$(mktemp)"
cleanup()
{
	rm -rf "${OUT_DIR_1}" "${OUT_DIR_2}" "${FOUND_1}" "${FOUND_2}" "${CONFIRMED}"
}
trap cleanup EXIT

echo "=== pass 1/2 ==="
run_scan_build "${OUT_DIR_1}"
python3 "${CCTOOLS_SRC}/packaging/lint/scan-build-parse.py" "${OUT_DIR_1}" | sort -u > "${FOUND_1}"
echo "=== pass 1/2: $(wc -l < "${FOUND_1}") finding(s) ==="

echo "=== pass 2/2 ==="
run_scan_build "${OUT_DIR_2}"
python3 "${CCTOOLS_SRC}/packaging/lint/scan-build-parse.py" "${OUT_DIR_2}" | sort -u > "${FOUND_2}"
echo "=== pass 2/2: $(wc -l < "${FOUND_2}") finding(s) ==="

comm -12 "${FOUND_1}" "${FOUND_2}" > "${CONFIRMED}"
echo "=== $(wc -l < "${CONFIRMED}") finding(s) confirmed by both passes ==="

# --- gate on findings that land on a line this change touches ---
#
# There is no baseline/suppression file: a confirmed finding on a line the
# change added or modified fails the build outright, including a repeat
# false positive -- a reviewer evaluates those as they come up rather than
# them being pre-suppressed. What keeps this from also failing on the
# repo's large pre-existing backlog of findings is scope, not a baseline:
# only findings on a touched line are ever compared at all, so a
# pre-existing finding elsewhere in a file this change happens to touch is
# never even looked at, no matter where it sits.
#
# SCAN_BUILD_DIFF_BASE (set by CI to the PR's base commit or the push's
# previous commit) is what provides that scope. Without it -- a local run,
# or a context with no meaningful prior commit to diff against (e.g. a
# release build with no PR) -- there is nothing to scope to, so findings
# are reported for visibility only and the run does not fail the build.
if [ -n "${SCAN_BUILD_DIFF_BASE:-}" ] && [ "${SCAN_BUILD_DIFF_BASE}" != "0000000000000000000000000000000000000000" ] && git cat-file -e "${SCAN_BUILD_DIFF_BASE}^{commit}" 2> /dev/null
then
	MERGE_BASE="$(git merge-base "${SCAN_BUILD_DIFF_BASE}" HEAD)"
	echo "=== diff mode: scoping gate to changes since ${MERGE_BASE} ==="
	TOUCHED_LINES="$(mktemp)"
	CONFIRMED_SCOPED="$(mktemp)"
	trap 'rm -f "${TOUCHED_LINES}" "${CONFIRMED_SCOPED}"; cleanup' EXIT
	# shellcheck disable=SC2086
	git diff --unified=0 "${MERGE_BASE}" HEAD -- ${C_PACKAGES} \
		| python3 "${CCTOOLS_SRC}/packaging/lint/diff-touched-lines.py" \
		| sort -u > "${TOUCHED_LINES}"
	echo "=== $(wc -l < "${TOUCHED_LINES}") line(s) touched by this change ==="
	# CONFIRMED entries are checker:file:line; TOUCHED_LINES entries are
	# file:line. Paths are assumed colon-free.
	awk -F: 'NR==FNR{touched[$0]=1; next} { if (($2 ":" $3) in touched) print }' "${TOUCHED_LINES}" "${CONFIRMED}" > "${CONFIRMED_SCOPED}"

	if [ -s "${CONFIRMED_SCOPED}" ]
	then
		echo "=== scan-build findings on lines this change touches ==="
		cat "${CONFIRMED_SCOPED}"
		echo
		echo "Fix these, or if one is a false positive, leave it for a reviewer to evaluate on the PR."
		exit 1
	fi

	echo "=== no scan-build findings on lines this change touches ==="
else
	echo "=== SCAN_BUILD_DIFF_BASE not set (or not resolvable): nothing to scope to, not gating ==="
	if [ -s "${CONFIRMED}" ]
	then
		echo "=== scan-build findings (repo-wide, informational only) ==="
		cat "${CONFIRMED}"
	else
		echo "=== no scan-build findings ==="
	fi
fi

# vim: set noexpandtab tabstop=4:
