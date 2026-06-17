from __future__ import annotations

from dataclasses import dataclass


@dataclass
class MutableBool:
    value: bool

    def __init__(self, value: bool):
        self.value = bool(value)

    def __bool__(self) -> bool:
        """Allow direct boolean evaluation"""
        return self.value

    def __and__(self, other: bool | MutableBool) -> MutableBool:
        """Combine with other bools"""

        if isinstance(other, bool):
            return MutableBool(self.value and other)
        elif isinstance(other, MutableBool):
            return MutableBool(self.value and other.value)
        return NotImplemented

    def __rand__(self, other: bool) -> MutableBool:
        """Support bool & MutableBool"""
        if isinstance(other, bool):
            return MutableBool(other and self.value)
        return NotImplemented

    def __or__(self, other: bool | MutableBool) -> MutableBool:
        """Bitwise OR with other bools"""
        if isinstance(other, bool):
            return MutableBool(self.value or other)
        elif isinstance(other, MutableBool):
            return MutableBool(self.value or other.value)
        return NotImplemented

    def __ror__(self, other: bool) -> MutableBool:
        """Support bool | MutableBool"""
        if isinstance(other, bool):
            return MutableBool(other or self.value)
        return NotImplemented

    def __eq__(self, other: bool | MutableBool) -> bool:
        """Allow equality comparison with booleans"""
        if isinstance(other, bool):
            return self.value == other
        elif isinstance(other, MutableBool):
            return self.value == other.value
        return NotImplemented

    def __add__(self, other):
        return int(self.value) + other

    def __radd__(self, other):
        return other + int(self.value)

    def __sub__(self, other):
        return int(self.value) - other

    def __rsub__(self, other):
        return other - int(self.value)

    def __mul__(self, other):
        return int(self.value) * other

    def __rmul__(self, other):
        return other * int(self.value)

    def __int__(self) -> int:
        return int(self.value)

    def __float__(self) -> float:
        return float(self.value)

    def set(self, value):
        """Set the value of the MutableBool"""
        self.value = bool(value)
        return self
