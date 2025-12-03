"""
Transform Operator
"""
from .base import BaseOperator
from typing import List, Tuple

class TransformOperator(BaseOperator):
    """
    Transform operator applies a function to each tuple.
    """
    
    def __init__(self, args: str = ""):
        super().__init__(args)
    
    def process(self, key: str, value: str) -> List[Tuple[str, str]]:
        """
        Transform the value (identity by default).
        Override this method for custom transformations.
        """
        return [(key, value)]