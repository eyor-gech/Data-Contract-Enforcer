from __future__ import annotations

import os
import sys

# Entry-point wrapper to keep `python contracts/attributor.py ...` stable.
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from attributor import main  # noqa: E402


if __name__ == "__main__":
    raise SystemExit(main())

