#! /bin/bash
set -e

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
for candidate in scan-build scan-build-19 scan-build-18 scan-build-17 scan-build-16 scan-build-15
do
	if command -v "${candidate}" > /dev/null 2>&1
	then
		SCAN_BUILD="${candidate}"
		break
	fi
done

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
BASELINE="$(mktemp)"
cleanup()
{
	rm -rf "${OUT_DIR_1}" "${OUT_DIR_2}" "${FOUND_1}" "${FOUND_2}" "${CONFIRMED}" "${BASELINE}"
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

# --- compare the findings confirmed by both passes against the baseline ---

comm -12 "${FOUND_1}" "${FOUND_2}" > "${CONFIRMED}"
echo "=== $(wc -l < "${CONFIRMED}") finding(s) confirmed by both passes ==="

SUPPRESSIONS="${CCTOOLS_SRC}/packaging/lint/scan-build-suppressions.txt"
grep -v '^#' "${SUPPRESSIONS}" | grep -v '^[[:space:]]*$' | sort -u > "${BASELINE}"

NEW_FINDINGS="$(comm -23 "${CONFIRMED}" "${BASELINE}")"

if [ -n "${NEW_FINDINGS}" ]
then
	echo "=== new scan-build findings not in packaging/lint/scan-build-suppressions.txt ==="
	echo "${NEW_FINDINGS}"
	echo
	echo "If each of these is a genuine pre-existing issue you're deliberately not fixing right now,"
	echo "add it (as checker:file:line) to packaging/lint/scan-build-suppressions.txt. Otherwise, fix it."
	exit 1
fi

echo "=== no new scan-build findings ==="

# vim: set noexpandtab tabstop=4:
