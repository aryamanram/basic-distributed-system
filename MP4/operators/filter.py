"""
Filter Operator
"""
from .base import BaseOperator
from typing import List, Tuple

class FilterOperator(BaseOperator):
    """
    Filter operator filters tuples based on a condition.
    """
    
    def __init__(self, args: str = ""):
        super().__init__(args)
        self.pattern = args
    
    def process(self, key: str, value: str) -> List[Tuple[str, str]]:
        """
        Filter tuples based on pattern match.
        """
        if self.pattern in value:
            return [(key, value)]
        else:
            return []