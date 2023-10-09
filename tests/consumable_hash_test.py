from hashlib import sha256

from hamt_sharding.consumable_hash import wrap_hash

def hash_fn(b: bytes) -> bytes:
    return sha256(b).digest()

def test_consumable_hash():
    val = 'some value'.encode()

    hash = wrap_hash(hash_fn)
    assert hash(val).take(0) == 0

    hash = wrap_hash(hash_fn)
    assert hash(val).take(10) == 110

    hash = wrap_hash(hash_fn)
    h = hash(val)
    for _ in range(100):
        result = h.take(10)
        assert result < 1024
        assert result > 0

    hash = wrap_hash(hash_fn)
    h = hash(val)
    h.take(10 * 100)
    h.untake(10 * 100)

    values = []
    hash = wrap_hash(hash_fn)
    h = hash(val)
    for _ in range(100):
        values.append(h.take(10))
    h.untake(10 * 100)
    for _ in range(100):
        result = h.take(10)
        assert result == values.pop(0)
