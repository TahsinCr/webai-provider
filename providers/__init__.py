import importlib.util
import sys
from pathlib import Path

_parent = Path(__file__).resolve().parent
_root = _parent.parent

if str(_root) not in __path__:
    __path__.append(str(_root))

_root_init = _root / "__init__.py"
if _root_init.exists():
    _spec = importlib.util.spec_from_file_location("providers", _root_init)
    if _spec and _spec.loader:
        _mod = sys.modules.get("providers")
        if _mod:
            _spec.loader.exec_module(_mod)
