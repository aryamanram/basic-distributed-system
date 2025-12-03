"""
Aggregate Operator
"""
from .base import BaseOperator
from typing import List, Tuple, Dict

class AggregateOperator(BaseOperator):
    """
    Aggregate operator maintains running statistics.
    """
    
    def __init__(self, args: str = ""):
        super().__init__(args)
        self.state: Dict[str, int] = {}
    
    def process(self, key: str, value: str) -> List[Tuple[str, str]]:
        """
        Aggregate by key (count by default).
        """
        if key not in self.state:
            self.state[key] = 0
        
        self.state[key] += 1
        
        # Return updated count
        return [(key, str(self.state[key]))]