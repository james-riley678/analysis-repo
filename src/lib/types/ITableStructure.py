# MyPy for Static Typing
from typing import List, Set, Dict, Tuple, Optional, Any
from mypy_extensions import TypedDict
  
class ITableStructure(TypedDict):
    variable1: int

    variable2: float

    variable3: int

    outputvariable: float