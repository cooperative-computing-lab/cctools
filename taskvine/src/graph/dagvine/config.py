# config.py
# Copyright (C) 2025 The University of Notre Dame
# This software is distributed under the GNU General Public License.
# See the file COPYING for details.

from dataclasses import dataclass, field


@dataclass(frozen=True)
class DefaultTuneParams:
    worker_source_max_transfers: int = 100
    max_retrievals: int = -1
    prefer_dispatch: int = 1
    transient_error_interval: int = 1
    attempt_schedule_depth: int = 1000


@dataclass(frozen=True)
class DefaultPaths:
    shared_file_system_dir: str = "/project01/ndcms/jzhou24/shared_file_system"
    staging_dir: str = "/project01/ndcms/jzhou24/staging"


@dataclass(frozen=True)
class DefaultPolicies:
    replica_placement_policy: str = "random"
    priority_mode: str = "largest-input-first"
    scheduling_mode: str = "files"
    prune_depth: int = 1


@dataclass(frozen=True)
class DefaultOutfileType:
    outfile_type: dict = field(default_factory=lambda: {
        "temp": 1.0,
        "shared-file-system": 0.0,
    })
