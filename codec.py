"""Binary codec for the Kafka wire protocol.

Every request body and response body is a sequence of big-endian primitives,
length-prefixed strings, byte blobs and (inside record batches) zigzag
varints.  The old code re-implemented a handful of ``rd_str`` / ``rd_i32``
closures at the top of *every* parser and hand-rolled ``struct.pack`` for
*every* builder.

:class:`BinaryReader` and :class:`BinaryWriter` centralise that once.  A reader
walks a buffer with an internal cursor; a writer accumulates one with a fluent,
chainable API.  Kafka's string/bytes null conventions live here, so no parser
has to remember that ``-1`` means NULL.
"""

from __future__ import annotations

import struct

# Kafka's nullable-length sentinel.
_NULL = -1


class BinaryReader:
    """Sequential reader over a byte buffer with an advancing cursor."""

    __slots__ = ("_data", "pos")

    def __init__(self, data: bytes, offset: int = 0) -> None:
        self._data = data
        self.pos = offset

    def _take(self, fmt: str, size: int) -> int:
        value = struct.unpack_from(fmt, self._data, self.pos)[0]
        self.pos += size
        return value

    def i8(self) -> int:
        return self._take(">b", 1)

    def i16(self) -> int:
        return self._take(">h", 2)

    def i32(self) -> int:
        return self._take(">i", 4)

    def i64(self) -> int:
        return self._take(">q", 8)

    def u32(self) -> int:
        return self._take(">I", 4)

    def raw(self, n: int) -> bytes:
        chunk = self._data[self.pos : self.pos + n]
        self.pos += n
        return chunk

    def string(self) -> str | None:
        """STRING / NULLABLE_STRING: int16 length prefix, ``-1`` == NULL."""
        n = self.i16()
        if n < 0:
            return None
        return self.raw(n).decode("utf-8")

    def blob(self) -> bytes:
        """BYTES / NULLABLE_BYTES: int32 length prefix, negative == empty."""
        n = self.i32()
        if n < 0:
            return b""
        return self.raw(n)

    def varint(self) -> int:
        """One zigzag-encoded signed varint (as used inside record batches)."""
        raw = 0
        shift = 0
        while True:
            byte = self._data[self.pos]
            self.pos += 1
            raw |= (byte & 0x7F) << shift
            if not (byte & 0x80):  # high bit clear → final byte
                break
            shift += 7
        # zigzag decode: n>=0 → 2n, n<0 → -2n-1
        return (raw >> 1) ^ -(raw & 1)

    def remaining(self) -> int:
        return len(self._data) - self.pos


class BinaryWriter:
    """Accumulates a big-endian buffer with a fluent, chainable API."""

    __slots__ = ("_buf",)

    def __init__(self) -> None:
        self._buf = bytearray()

    def i8(self, v: int) -> "BinaryWriter":
        self._buf += struct.pack(">b", v)
        return self

    def i16(self, v: int) -> "BinaryWriter":
        self._buf += struct.pack(">h", v)
        return self

    def i32(self, v: int) -> "BinaryWriter":
        self._buf += struct.pack(">i", v)
        return self

    def i64(self, v: int) -> "BinaryWriter":
        self._buf += struct.pack(">q", v)
        return self

    def boolean(self, v: bool) -> "BinaryWriter":
        self._buf += struct.pack(">?", v)
        return self

    def raw(self, data: bytes) -> "BinaryWriter":
        self._buf += data
        return self

    def string(self, s: str) -> "BinaryWriter":
        """STRING: int16 length prefix + UTF-8 bytes."""
        encoded = s.encode("utf-8")
        return self.i16(len(encoded)).raw(encoded)

    def null_string(self) -> "BinaryWriter":
        """NULLABLE_STRING carrying NULL (length ``-1``)."""
        return self.i16(_NULL)

    def blob(self, data: bytes) -> "BinaryWriter":
        """BYTES: int32 length prefix + raw bytes."""
        return self.i32(len(data)).raw(data)

    def null_blob(self) -> "BinaryWriter":
        """NULLABLE_BYTES carrying NULL (length ``-1``)."""
        return self.i32(_NULL)

    def getvalue(self) -> bytes:
        return bytes(self._buf)


def frame(payload: bytes) -> bytes:
    """Wrap *payload* in the 4-byte big-endian length prefix used on the wire."""
    return struct.pack(">I", len(payload)) + payload
