"""Deterministic identifier and randomness helpers.

All randomness is derived from SHA-256 digests of the config namespace plus
entity coordinates, so output is a pure function of configuration and seed.
"""

import hashlib
import random


def digest(*parts: object) -> str:
    hasher = hashlib.sha256()
    for part in parts:
        hasher.update(str(part).encode())
        hasher.update(b"\x00")
    return hasher.hexdigest()


def hex_id(namespace: str, kind: str, *indices: object, length: int) -> str:
    """Derive a nonzero hex ID of `length` characters for an entity."""
    value = digest(namespace, kind, *indices)[:length]
    if int(value, 16) == 0:
        return "1" + value[1:]
    return value


def rng(namespace: str, *parts: object) -> random.Random:
    return random.Random(int(digest(namespace, "rng", *parts)[:16], 16))
