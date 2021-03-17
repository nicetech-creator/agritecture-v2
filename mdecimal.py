from decimal import Decimal


class MDecimal(Decimal):

    @classmethod
    def from_float(cls, f):
        return cls(str(f))

    def __mul__(self, other):
        if isinstance(other, float):
            other = MDecimal.from_float(other)
        return MDecimal(Decimal.__mul__(self, other))

    __rmul__ = __mul__

    def __neg__(self):
        return MDecimal(Decimal.__neg__(self))

    def __pos__(self):
        return MDecimal(Decimal.__pos__(self))

    def __abs__(self):
        return MDecimal(Decimal.__abs__(self))

    def __add__(self, other):
        if isinstance(other, float):
            other = MDecimal.from_float(other)
        return MDecimal(Decimal.__add__(self, other))

    def __radd__(self, other):
        if isinstance(other, float):
            other = MDecimal.from_float(other)
        return MDecimal(Decimal.__radd__(self, other))

    def __sub__(self, other):
        if isinstance(other, float):
            other = MDecimal.from_float(other)
        return MDecimal(Decimal.__sub__(self, other))

    def __rsub__(self, other):
        if isinstance(other, float):
            other = MDecimal.from_float(other)
        return MDecimal(Decimal.__rsub__(self, other))

    def __truediv__(self, other):
        if isinstance(other, float):
            other = MDecimal.from_float(other)
        return MDecimal(Decimal.__truediv__(self, other))

    def __rtruediv__(self, other):
        if isinstance(other, float):
            other = MDecimal.from_float(other)
        return MDecimal(Decimal.__rtruediv__(self, other))

    def __divmod__(self, other):
        if isinstance(other, float):
            other = MDecimal.from_float(other)
        return MDecimal(Decimal.__divmod__(self, other))

    def __rdivmod__(self, other):
        if isinstance(other, float):
            other = MDecimal.from_float(other)
        return MDecimal(Decimal.__rdivmod__(self, other))

    def __mod__(self, other):
        if isinstance(other, float):
            other = MDecimal.from_float(other)
        return MDecimal(Decimal.__mod__(self, other))

    def __rmod__(self, other):
        if isinstance(other, float):
            other = MDecimal.from_float(other)
        return MDecimal(Decimal.__rmod__(self, other))

    def remainder_near(self, other):
        if isinstance(other, float):
            other = MDecimal.from_float(other)
        return MDecimal(Decimal.remainder_near(self, other))

    def __floordiv__(self, other):
        if isinstance(other, float):
            other = MDecimal.from_float(other)
        return MDecimal(Decimal.__floordiv__(self, other))

    def __rfloordiv__(self, other):
        if isinstance(other, float):
            other = MDecimal.from_float(other)
        return MDecimal(Decimal.__rfloordiv__(self, other))

    @property
    def imag(self):
        return MDecimal(0)

    def __round__(self, n):
        return MDecimal(Decimal.__round__(self, n))

    def __floor__(self):
        return MDecimal(Decimal.__floor__(self))

    def __ceil__(self):
        return MDecimal(Decimal.__ceil__(self))

    def __pow__(self, other):
        if isinstance(other, float):
            other = MDecimal.from_float(other)
        return MDecimal(Decimal.__pow__(self, other))

    def __rpow__(self, other):
        if isinstance(other, float):
            other = MDecimal.from_float(other)
        return MDecimal(Decimal.__rpow__(self, other))

    def normalize(self):
        return MDecimal(Decimal.normalize(self))

    def quantize(self, exp, rounding=None):
        return MDecimal(Decimal.quantize(self, exp, rounding=rounding))

    def to_integral_exact(self, rounding=None):
        return MDecimal(Decimal.to_integral_exact(self, rounding=rounding))

    def to_integral_value(self, rounding=None):
        return MDecimal(Decimal.to_integral_value(self, rounding=rounding))

    def sqrt(self):
        return MDecimal(Decimal.sqrt(self))

    def max(self, other):
        if isinstance(other, float):
            other = MDecimal.from_float(other)
        return MDecimal(Decimal.max(self, other))

    def min(self, other):
        if isinstance(other, float):
            other = MDecimal.from_float(other)
        return MDecimal(Decimal.min(self, other))

    def exp(self):
        return MDecimal(Decimal.exp(self))

    def ln(self):
        return MDecimal(Decimal.ln(self))

    def log10(self):
        return MDecimal(Decimal.log10(self))

    def logb(self):
        return MDecimal(Decimal.logb(self))

    def logical_and(self, other):
        return MDecimal(Decimal.logical_and(self, other))

    def logical_invert(self):
        return MDecimal(Decimal.logical_invert(self))

    def logical_or(self, other):
        return MDecimal(Decimal.logical_or(self, other))

    def logical_xor(self, other):
        return MDecimal(Decimal.logical_xor(self, other))

    def max_mag(self, other):
        if isinstance(other, float):
            other = MDecimal.from_float(other)
        return MDecimal(Decimal.max_mag(self, other))

    def min_mag(self, other):
        if isinstance(other, float):
            other = MDecimal.from_float(other)
        return MDecimal(Decimal.min_mag(self, other))

    def next_minus(self):
        return MDecimal(Decimal.next_minus(self))

    def next_plus(self):
        return MDecimal(Decimal.next_plus(self))

    def next_toward(self, other):
        if isinstance(other, float):
            other = MDecimal.from_float(other)
        return MDecimal(Decimal.next_toward(self, other))

    def radix(self):
        return MDecimal(Decimal.radix(self))

    def rotate(self, other):
        if isinstance(other, float):
            other = MDecimal.from_float(other)
        return MDecimal(Decimal.rotate(self, other))

    def scaleb(self, other):
        if isinstance(other, float):
            other = MDecimal.from_float(other)
        return MDecimal(Decimal.scaleb(self, other))

    def __repr__(self):
        return str(round(self, 8).normalize())
