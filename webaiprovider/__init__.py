from __future__ import annotations

import webai_provider as _pkg

__all__ = _pkg.__all__
__version__ = _pkg.__version__

for _name in _pkg.__all__:
    globals()[_name] = getattr(_pkg, _name)
