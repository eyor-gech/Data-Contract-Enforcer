from __future__ import annotations

import importlib.util
import os
import sys

# Entry-point wrapper: allows `python contracts/report_generator.py` while reusing existing implementation.
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
SCRIPT = os.path.join(ROOT, "scripts", "report_generator.py")

spec = importlib.util.spec_from_file_location("week7_report_generator", SCRIPT)
if spec is None or spec.loader is None:
    raise SystemExit(2)
mod = importlib.util.module_from_spec(spec)
sys.modules["week7_report_generator"] = mod
spec.loader.exec_module(mod)

main = getattr(mod, "main")


if __name__ == "__main__":
    raise SystemExit(main())
