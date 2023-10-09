from hashlib import sha256

from hamt_sharding.hamt_sharding import create_HAMT

def hash_fn(value: bytes):
    return sha256(value).digest()

def test_hamt():
    bucket = create_HAMT(hash_fn)
    assert bucket.get('unknown') is None
    
    bucket = create_HAMT(hash_fn)
    bucket['key'] = 'value'
    assert bucket.get('key') == 'value'

    bucket = create_HAMT(hash_fn)
    bucket['key'] = 'value'
    bucket['key'] = 'other value'
    assert bucket.get('key') == 'other value'

    bucket = create_HAMT(hash_fn)
    del bucket['doesnt exist']

    bucket = create_HAMT(hash_fn)
    bucket['key'] = 'value'
    del bucket['key']
    assert bucket.get('key') is None

    bucket = create_HAMT(hash_fn)
    assert bucket.leaf_count() == 0
    for i in range(400):
        bucket[str(i)] = str(i)
    assert bucket.leaf_count() == 400

    bucket = create_HAMT(hash_fn)
    for i in range(400):
        bucket[str(i)] = str(i)
    assert bucket.children_count() == 256

    bucket = create_HAMT(hash_fn)
    assert bucket.only_child() is None

    bucket = create_HAMT(hash_fn)
    for i in range(400):
        bucket[str(i)] = str(i)
    count = sum(1 for _ in bucket.each_leaf_series())
    assert count == 400

    def small_hash_fn(b) -> bytes:
        return hash_fn(b)[:2]

    bucket = create_HAMT(small_hash_fn)
    for i in range(400):
        bucket[str(i)] = str(i)
    assert bucket['100'] == '100'
