"""Test helper: run an app under uvicorn, exit when a control file appears.

This sandbox forbids signalling child processes (kill -> EPERM), so tests
stop servers by touching CONTROL_FILE instead of sending SIGTERM. The
deliverable apps themselves stay free of any test hooks.

Usage: python3 tests/run_server.py <appdir> <port> <control_file>
"""

import os
import sys
import threading
import time

import uvicorn


def main() -> None:
    appdir, port, control = sys.argv[1], int(sys.argv[2]), sys.argv[3]
    sys.path.insert(0, appdir)

    def watchdog() -> None:
        while not os.path.exists(control):
            time.sleep(0.1)
        os._exit(0)

    threading.Thread(target=watchdog, daemon=True).start()
    uvicorn.run("app:app", host="127.0.0.1", port=port, workers=1, log_level="info")


if __name__ == "__main__":
    main()
