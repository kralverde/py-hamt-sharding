from typing import TypeVar, Generic, Optional, Union, Generator, MutableMapping, Iterator

import attr

from bitmap_sparse_array import SparseArray

from .consumable_hash import InfiniteHash, InfiniteWrapper, HashFunction, wrap_hash

K = TypeVar('K', str, bytes)
V = TypeVar('V')

@attr.define(slots=True)
class HAMTBucketChild(Generic[K, V]):
    key: K
    value: V
    infinite_hash: InfiniteHash

@attr.define(slots=True)
class HAMTBucketPosition(Generic[K, V]):
    bucket: 'HAMTBucket[K, V]'
    pos: int
    infinite_hash: InfiniteHash
    existing_child: Optional[HAMTBucketChild[K, V]]

class HAMTBucket(MutableMapping[K, V]):
    def __init__(self, bits: int, infinite_wrapper: InfiniteWrapper, parent: Optional['HAMTBucket[K, V]'] = None, pos_at_parent: int = 0):
        self._bits = bits
        self._infinite_wrapper = infinite_wrapper
        self._pop_count = 0
        self._parent: Optional['HAMTBucket[K, V]'] = parent
        self._pos_at_parent = pos_at_parent
        self._children: SparseArray[Union[HAMTBucket[K, V], HAMTBucketChild[K, V]]] = SparseArray()
        self.key: Optional[K] = None

    @classmethod
    def create_hamt(cls, hash_function: HashFunction, bits: int = 8) -> 'HAMTBucket[K, V]':
        return cls(bits, wrap_hash(hash_function))

    def __setitem__(self, key: K, item: V) -> None:
        if not isinstance(key, (str, bytes)):
            raise TypeError(f'Bucket keys must be strings or bytes, not {type(key)}')
        
        place = self._find_new_bucket_and_pos(key)
        place.bucket._put_at(place, key, item)
        
    def __getitem__(self, key: K) -> V:
        if not isinstance(key, (str, bytes)):
            raise TypeError(f'Bucket keys must be strings or bytes, not {type(key)}')
        
        child = self._find_child(key)
        if child is not None:
            return child.value
        raise IndexError()
    
    def __len__(self) -> int:
        return self.leaf_count()
    
    def __iter__(self) -> Iterator[K]:
        for _, child in self._children.items():
            if isinstance(child, HAMTBucket):
                yield from child
            else:
                yield child.key
        
    def get(self, key: K, default: Optional[V] = None) -> Optional[V]:  # type: ignore[override]
        try:
            return self[key]
        except IndexError:
            return default

    def __delitem__(self, key: K) -> None:
        if not isinstance(key, (str, bytes)):
            raise TypeError(f'Bucket keys must be strings or bytes, not {type(key)}')
        
        place = self._find_place(key)
        child = place.bucket._at(place.pos)
        if child is not None and child.key == key:
            place.bucket._del_at(place.pos)

    def leaf_count(self) -> int:
        result = 0
        for _, child in self._children.items():
            if isinstance(child, HAMTBucket):
                result += child.leaf_count()
            else:
                result += 1
        return result

    def children_count(self) -> int:
        return len(self._children)

    def only_child(self) -> Optional[HAMTBucketChild[K, V]]:
        result = self._children.get(0)
        if result is not None:
            assert isinstance(result, HAMTBucketChild)
        return result

    def each_leaf_series(self) -> Generator[HAMTBucketChild[K, V], None, None]:
        for _, child in self._children.items():
            if isinstance(child, HAMTBucket):
                yield from child.each_leaf_series()
            else:
                yield child

    @property
    def table_size(self) -> int:
        result = pow(2, self._bits)
        assert isinstance(result, int)
        return result

    def _find_child(self, key: K) -> Optional[HAMTBucketChild[K, V]]:
        result = self._find_place(key)
        child = result.bucket._at(result.pos)
        assert not isinstance(child, HAMTBucket)
        if child is not None and child.key == key:
            return child
        return None

    def _find_place(self, key: Union[K, InfiniteHash]) -> HAMTBucketPosition[K, V]:
        hash_value = self._infinite_wrapper(key.encode() if isinstance(key, str) else key)
        index = hash_value.take(self._bits)

        child = self._children.get(index)

        if isinstance(child, HAMTBucket):
            return child._find_place(hash_value)
        
        return HAMTBucketPosition(self, index, hash_value, child)
    
    def _find_new_bucket_and_pos(self, key: Union[K, InfiniteHash]) -> HAMTBucketPosition[K, V]:
        place = self._find_place(key)
        if place.existing_child is not None and place.existing_child.key != key:
            bucket = HAMTBucket(self._bits, self._infinite_wrapper, place.bucket, place.pos)
            place.bucket._put_object_at(place.pos, bucket)

            new_place = bucket._find_place(place.existing_child.infinite_hash)
            new_place.bucket._put_at(new_place, place.existing_child.key, place.existing_child.value)

            return bucket._find_new_bucket_and_pos(place.infinite_hash)
        return place

    def _put_at(self, place: HAMTBucketPosition[K, V], key: K, value: V) -> None:
        self._put_object_at(place.pos, HAMTBucketChild(key, value, place.infinite_hash))

    def _put_object_at(self, pos: int, obj: Union['HAMTBucket[K, V]', HAMTBucketChild[K, V]]) -> None:
        if self._children.get(pos) is None:
            self._pop_count += 1
        self._children[pos] = obj

    def _del_at(self, pos: int) -> None:
        assert pos >= 0
        if self._children.get(pos) is not None:
            self._pop_count -= 1
        del self._children[pos]
        self._level()

    def _at(self, pos: int) -> Optional[Union[HAMTBucketChild[K, V], 'HAMTBucket[K, V]']]:
        return self._children.get(pos)

    def _level(self) -> None:
        if self._parent is not None and self._pop_count <= 1:
            if self._pop_count == 1:
                only_child = next(self._children.values())
                if only_child is not None and not isinstance(only_child, HAMTBucket):
                    only_child.infinite_hash.untake(self._bits)
                    place = HAMTBucketPosition(self._parent, 
                                           self._pos_at_parent,
                                           only_child.infinite_hash,
                                           None)
                    self._parent._put_at(place, only_child.key, only_child.value)
            else:
                self._parent._del_at(self._pos_at_parent)
