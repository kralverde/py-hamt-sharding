from hashlib import sha256

from hamt_sharding.buckets import HAMTBucket

def hash_fn(value: bytes):
    return sha256(value).digest()

def test_hamt():
    bucket = HAMTBucket[str, str].create_hamt(hash_fn)
    assert bucket.get('unknown') is None
    
    bucket = HAMTBucket[str, str].create_hamt(hash_fn)
    bucket['key'] = 'value'
    assert bucket.get('key') == 'value'

    bucket = HAMTBucket[str, str].create_hamt(hash_fn)
    bucket['key'] = 'value'
    bucket['key'] = 'other value'
    assert bucket.get('key') == 'other value'

    bucket = HAMTBucket[str, str].create_hamt(hash_fn)
    del bucket['doesnt exist']

    bucket = HAMTBucket[str, str].create_hamt(hash_fn)
    bucket['key'] = 'value'
    del bucket['key']
    assert bucket.get('key') is None

    bucket = HAMTBucket[str, str].create_hamt(hash_fn)
    assert bucket.leaf_count() == 0
    for i in range(400):
        bucket[str(i)] = str(i)
    assert bucket.leaf_count() == 400

    bucket = HAMTBucket[str, str].create_hamt(hash_fn)
    for i in range(400):
        bucket[str(i)] = str(i)
    assert bucket.children_count() == 256

    bucket = HAMTBucket[str, str].create_hamt(hash_fn)
    assert bucket.only_child() is None

    bucket = HAMTBucket[str, str].create_hamt(hash_fn)
    for i in range(400):
        bucket[str(i)] = str(i)
    count = sum(1 for _ in bucket.each_leaf_series())
    assert count == 400

    def small_hash_fn(b) -> bytes:
        return hash_fn(b)[:2]

    bucket = HAMTBucket[str, str].create_hamt(small_hash_fn)
    for i in range(400):
        bucket[str(i)] = str(i)
    assert bucket['100'] == '100'

    bucket = HAMTBucket[bytes, int].create_hamt(small_hash_fn)
    bucket[b'test'] = 100
    assert bucket[b'test'] == 100
    assert list(bucket.keys()) == [b'test']
    assert list(bucket.values()) == [100]
    