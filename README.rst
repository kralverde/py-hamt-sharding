This code has been transpiled from `js-hamt-sharding <https://github.com/ipfs/js-hamt-sharding/commit/a993a1e5a3dc234b118a4a32974478d3ba632af9>`_ and has minimal changes. 

Usage
-----

HAMTBucket implements `MutableMapping`.

We suggest you import the HAMTBucket as follows:

>>> from hamt_sharding import HAMTBucket

Setting and getting
^^^^^^^^^^^^^^^^^^^

>>> from hashlib import sha256
>>> def hash_fn(value: bytes):
...     return sha256(value).digest()
...
>>> bucket = HAMTBucket.create_hamt(hash_fn)
>>> bucket['key'] = 'value'
>>> bucket['key']
'value'

