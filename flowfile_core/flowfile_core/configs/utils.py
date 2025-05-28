from dataclasses import dataclass


@dataclass
class MutableBool:
    value: bool

    def __bool__(self) -> bool:
        """Allow direct boolean evaluation"""
        return self.value

    def __eq__(self, other) -> bool:
        """Allow equality comparison with booleans"""
        if isinstance(other, bool):
            return self.value == other
        elif isinstance(other, MutableBool):
            return self.value == other.value
        return NotImplemented
