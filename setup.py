import os
import re
import shutil
import stat
import subprocess
from pathlib import Path

from setuptools import setup
from setuptools.command.build_py import build_py as _build_py
from setuptools.dist import Distribution

ROOT = Path(__file__).resolve().parent

CONFIGURE_ARGS = [
    # No --strict here (it adds -Werror): cibuildwheel's macOS builds target
    # an older MACOSX_DEPLOYMENT_TARGET than the runner's SDK, which turns
    # availability warnings (e.g. utimensat needing 10.13+) into hard errors.
    # --strict is already enforced by this project's own native/conda CI.
    "--without-system-makeflow",
    "--without-system-ftp-lite",
    "--without-system-grow",
    "--without-system-parrot",
    "--without-system-poncho",
    "--without-system-doc",
    "--with-readline-path", "no",
    "--with-fuse-path", "no",
    "--with-perl-path", "no",
]

MAKE_PACKAGES = ["dttools", "batch_job", "chirp", "work_queue", "taskvine", "resource_monitor"]

# (path relative to repo root after `make`, name exposed as console_script / bundled binary)
BINARIES = [
    ("dttools/src/catalog_update", "catalog_update"),
    ("dttools/src/watchdog", "watchdog"),
    ("dttools/src/disk_allocator", "disk_allocator"),
    ("dttools/src/jx2json", "jx2json"),
    ("dttools/src/jx2env", "jx2env"),
    ("dttools/src/jx_repl", "jx_repl"),
    ("dttools/src/env_replace", "env_replace"),
    ("dttools/src/catalog_query", "catalog_query"),
    ("chirp/src/chirp", "chirp"),
    ("chirp/src/chirp_get", "chirp_get"),
    ("chirp/src/chirp_put", "chirp_put"),
    ("chirp/src/chirp_server", "chirp_server"),
    ("chirp/src/chirp_status", "chirp_status"),
    ("chirp/src/chirp_benchmark", "chirp_benchmark"),
    ("chirp/src/chirp_stream_files", "chirp_stream_files"),
    ("chirp/src/chirp_distribute", "chirp_distribute"),
    ("work_queue/src/work_queue_worker", "work_queue_worker"),
    ("work_queue/src/work_queue_status", "work_queue_status"),
    ("work_queue/src/work_queue_example", "work_queue_example"),
    ("taskvine/src/worker/vine_worker", "vine_worker"),
    ("taskvine/src/tools/vine_status", "vine_status"),
    ("taskvine/src/tools/vine_benchmark", "vine_benchmark"),
    ("batch_job/src/work_queue_factory", "work_queue_factory"),
    ("batch_job/src/work_queue_pool", "work_queue_pool"),
    ("batch_job/src/vine_factory", "vine_factory"),
    ("resource_monitor/src/resource_monitor", "resource_monitor"),
    ("resource_monitor/src/piggybacker", "piggybacker"),
    ("resource_monitor/src/rmonitor_poll_example", "rmonitor_poll_example"),
    ("resource_monitor/src/rmonitor_snapshot", "rmonitor_snapshot"),
]

# Non-compiled helper scripts installed alongside the binaries above.
SCRIPTS = [
    ("dttools/src/cctools_gpu_autodetect", "cctools_gpu_autodetect"),
    ("chirp/src/chirp_audit_cluster", "chirp_audit_cluster"),
    ("chirp/src/chirp_server_hdfs", "chirp_server_hdfs"),
    ("work_queue/src/work_queue_submit_common", "work_queue_submit_common"),
    ("work_queue/src/condor_submit_workers", "condor_submit_workers"),
    ("work_queue/src/uge_submit_workers", "uge_submit_workers"),
    ("work_queue/src/torque_submit_workers", "torque_submit_workers"),
    ("work_queue/src/pbs_submit_workers", "pbs_submit_workers"),
    ("work_queue/src/slurm_submit_workers", "slurm_submit_workers"),
    ("work_queue/src/work_queue_graph_log", "work_queue_graph_log"),
    ("work_queue/src/work_queue_graph_workers", "work_queue_graph_workers"),
    ("taskvine/src/tools/vine_plot_performance", "vine_plot_performance"),
    ("taskvine/src/tools/vine_plot_taskgraph", "vine_plot_taskgraph"),
    ("taskvine/src/tools/vine_plot_workers", "vine_plot_workers"),
    ("taskvine/src/tools/vine_plot_txn_log", "vine_plot_txn_log"),
    ("taskvine/src/tools/vine_submit_workers", "vine_submit_workers"),
    ("taskvine/src/tools/vine_plot_compose", "vine_plot_compose"),
    ("taskvine/src/tools/vine_plot_run", "vine_plot_run"),
    ("batch_job/src/condor_chaos_monkey", "condor_chaos_monkey"),
]

# LD_PRELOAD-style helper libraries some of the tools above load from their own
# directory at runtime (not exposed as console_scripts).
RUNTIME_LIBRARIES = [
    ("dttools/src/libforce_halt_enospc.so", "libforce_halt_enospc.so"),
    ("resource_monitor/src/librmonitor_helper.so", "librmonitor_helper.so"),
    ("resource_monitor/src/librminimonitor_helper.so", "librminimonitor_helper.so"),
]

# Directories whose *contents* get merged into the wheel's top-level `ndcctools/` package.
PYTHON_TREES = [
    "taskvine/src/bindings/python3/ndcctools",
    "work_queue/src/bindings/python3/ndcctools",
    "chirp/src/bindings/python3/ndcctools",
    "resource_monitor/src/bindings/python3/ndcctools",
]

SHIM_HEADER = '''"""Generated at build time by setup.py -- thin wrappers that exec the bundled cctools binaries."""
import os
import sys
from importlib import resources


def _exec(name):
    with resources.as_file(resources.files("ndcctools") / "_bin" / name) as path:
        if not path.exists():
            sys.exit(
                f"{name!r} is not available in this ndcctools build "
                "(upstream cctools does not build it for this OS/architecture)."
            )
        os.execv(str(path), [str(path)] + sys.argv[1:])

'''

SHIM_FUNC = """
def {name}():
    _exec({name!r})
"""


def read_version():
    text = (ROOT / "configure").read_text()
    major = re.search(r"^MAJOR=(\d+)", text, re.M).group(1)
    minor = re.search(r"^MINOR=(\d+)", text, re.M).group(1)
    micro = re.search(r"^MICRO=(\d+)", text, re.M).group(1)
    return f"{major}.{minor}.{micro}"


def _make_executable(path):
    mode = path.stat().st_mode
    path.chmod(mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)


class build_py(_build_py):
    def run(self):
        self._configure_and_make()
        dest = Path(self.build_lib) / "ndcctools"
        self._stage_python(dest)
        self._stage_bin(dest / "_bin")
        self._write_shims(dest)

    def _configure_and_make(self):
        env = os.environ.copy()
        # cibuildwheel sets ARCHFLAGS (e.g. "-arch x86_64") to cross-build a
        # non-native macOS wheel on the runner's host arch; it's only consumed
        # by Python's own distutils/setuptools build_ext, which our raw
        # `configure && make` bypasses entirely. Without this, `configure`
        # (which does seed its ccflags/ldflags from $CFLAGS/$LDFLAGS) compiles
        # everything for the host's native arch, while the SWIG extension
        # modules -- built using this venv's python3-config flags -- target
        # the cross-build arch, and linking the two together fails.
        archflags = env.get("ARCHFLAGS")
        if archflags:
            env["CFLAGS"] = f"{env.get('CFLAGS', '')} {archflags}".strip()
            env["LDFLAGS"] = f"{env.get('LDFLAGS', '')} {archflags}".strip()
        # cibuildwheel reuses this same checkout across every (Python version,
        # arch) combo it builds on a macOS runner. If the arch changed since
        # our last build here, stale objects from the previous arch (which
        # Make has no way to know are for the wrong arch) linger and get
        # linked alongside freshly-compiled ones -- so force a clean rebuild
        # whenever ARCHFLAGS differs from what we last built with.
        arch_marker = ROOT / ".wheel_build_archflags"
        last_archflags = arch_marker.read_text() if arch_marker.exists() else None
        if last_archflags != (archflags or ""):
            if (ROOT / "config.mk").exists():
                subprocess.check_call(["make", "clean"], cwd=ROOT, env=env)
            subprocess.check_call(["./configure", *CONFIGURE_ARGS], cwd=ROOT, env=env)
            arch_marker.write_text(archflags or "")
        njobs = str(os.cpu_count() or 1)
        subprocess.check_call(["make", "-j", njobs, *MAKE_PACKAGES], cwd=ROOT, env=env)

    def _stage_python(self, dest):
        dest.mkdir(parents=True, exist_ok=True)
        for tree in PYTHON_TREES:
            src = ROOT / tree
            for item in src.iterdir():
                target = dest / item.name
                if item.is_dir():
                    shutil.copytree(item, target, dirs_exist_ok=True)
                else:
                    shutil.copy2(item, target)

    def _stage_bin(self, bindir):
        bindir.mkdir(parents=True, exist_ok=True)
        for rel, name in [*BINARIES, *SCRIPTS, *RUNTIME_LIBRARIES]:
            src = ROOT / rel
            if not src.exists():
                # e.g. resource_monitor's binaries are only built by upstream's
                # Makefile on native Linux/x86_64 (see CCTOOLS_LINUX_NATIVE_X86_64
                # in resource_monitor/src/Makefile) -- absent elsewhere by design.
                print(f"ndcctools: skipping {rel!r} (not built for this platform)")
                continue
            target = bindir / name
            shutil.copy2(src, target)
            _make_executable(target)

    def _write_shims(self, dest):
        names = [name for _, name in [*BINARIES, *SCRIPTS]]
        body = SHIM_HEADER + "".join(SHIM_FUNC.format(name=n) for n in names)
        (dest / "_bin.py").write_text(body)


class BinaryDistribution(Distribution):
    # No real Extension objects (the native build runs out-of-band in build_py),
    # but the wheel bundles compiled binaries/.so files and must be tagged as a
    # platform wheel (e.g. manylinux_x86_64), not a pure-Python one.
    def has_ext_modules(self):
        return True


setup(
    version=read_version(),
    cmdclass={"build_py": build_py},
    distclass=BinaryDistribution,
    packages=["ndcctools"],
    # Only used by setuptools' metadata bookkeeping (egg_info/sdist); build_py.run()
    # is fully overridden above and does its own staging from the real source tree.
    package_dir={"ndcctools": "taskvine/src/bindings/python3/ndcctools"},
)
