"""Unified entrypoint for Cloud Run deployments.

Depending on the ``RUN_MODE`` environment variable, this module either
starts the HTTP server (default) or executes the batch synchronization.

This allows the same container image to power both Cloud Run Services
and Cloud Run Jobs without needing different Dockerfiles.
"""

from __future__ import annotations

import logging
import os
import sys
from typing import Callable


SERVER_MODE = "server"
JOB_MODE = "job"
BATCH_MODE = "batch"


def _get_mode() -> str:
    return os.getenv("RUN_MODE", SERVER_MODE).strip().lower()


def _run_server() -> int:
    from runn_sync import main as server_main

    logging.info("RUN_MODE=%s → starting HTTP server", SERVER_MODE)
    server_main()
    return 0


def _run_batch() -> int:
    from main import main as batch_main

    logging.info("RUN_MODE=%s → running one-off sync", BATCH_MODE)
    return batch_main()


def main() -> int:
    mode = _get_mode()
    runner_map: dict[str, Callable[[], int]] = {
        SERVER_MODE: _run_server,
        JOB_MODE: _run_batch,
        BATCH_MODE: _run_batch,
    }

    try:
        runner = runner_map[mode]
    except KeyError:
        logging.error(
            "Unknown RUN_MODE '%s'. Expected one of: %s",
            mode,
            ", ".join(sorted(runner_map)),
        )
        return 1

    return runner()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    sys.exit(main())
