from typing import Optional

from .consumable_hash import HashFunction, wrap_hash
from .buckets import Bucket

def create_HAMT(hash_function: HashFunction, bits: int = 8):
    return Bucket(bits, wrap_hash(hash_function), None)
