"""
Base Operator class
"""
from abc import ABC, abstractmethod
from typing import List, Tuple

class BaseOperator(ABC):
    """
    Base class for all operators.
    """
    
    def __init__(self, args: str = ""):
        self.args = args
    
    @abstractmethod
    def process(self, key: str, value: str) -> List[Tuple[str, str]]:
        """
        Process a tuple and return zero or more output tuples.
        
        Returns:
            List of (key, value) tuples
        """
        pass