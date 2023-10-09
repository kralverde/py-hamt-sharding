import attr
from typing import TypeVar, Generic, Optional, Union, Generator

from sparse_array import SparseArray
from .consumable_hash import InfiniteHash, InfiniteWrapper

V = TypeVar('V')
T = TypeVar('T')

@attr.define(slots=True)
class BucketChild(Generic[V]):
    key: str
    value: V
    hash: InfiniteHash

@attr.define(slots=True)
class BucketPosition(Generic[T]):
    bucket: 'Bucket[T]'
    pos: int
    hash: InfiniteHash
    existing_child: Optional[BucketChild[T]]

class Bucket(Generic[T]):
    def __init__(self, bits: int, hash: InfiniteWrapper, parent: Optional['Bucket[T]'], pos_at_parent: int = 0):
        self._bits = bits
        self._hash = hash
        self._pop_count = 0
        self._parent = parent
        self._pos_at_parent = pos_at_parent
        self._children: SparseArray[Union[Bucket[T], BucketChild[T]]] = SparseArray()
        self.key: Optional[str] = None

    def __setitem__(self, key: str, item: T):
        assert isinstance(key, str) or isinstance(key, bytes)
        place = self._find_new_bucket_and_pos(key)
        place.bucket._put_at(place, key, item)

    def __getitem__(self, key: str):
        assert isinstance(key, str) or isinstance(key, bytes)
        child = self._find_child(key)
        if child is not None:
            return child.value
        raise IndexError()

    def get(self, key: str, default: Optional[T] = None):
        try:
            return self[key]
        except IndexError:
            return default

    def __delitem__(self, key: str):
        place = self._find_place(key)
        child = place.bucket._at(place.pos)
        if child is not None and child.key == key:
            place.bucket._del_at(place.pos)

    def leaf_count(self) -> int:
        result = 0
        for _, child in self._children.items():
            if isinstance(child, Bucket):
                result += child.leaf_count()
            else:
                result += 1
        return result

    def children_count(self):
        return len(self._children)

    def only_child(self) -> Optional[BucketChild[T]]:
        return self._children.get(0)

    def each_leaf_series(self) -> Generator[BucketChild[T], None, None]:
        for _, child in self._children.items():
            if isinstance(child, Bucket):
                yield from child.each_leaf_series()
            else:
                yield child

    @property
    def table_size(self):
        return pow(2, self._bits)

    def _find_child(self, key: str):
        result = self._find_place(key)
        child = result.bucket._at(result.pos)
        assert not isinstance(child, Bucket)
        if child is not None and child.key == key:
            return child
        return None

    def _find_place(self, key: Union[str, InfiniteHash]) -> BucketPosition[T]:
        hash_value = self._hash(key.encode()) if isinstance(key, str) else key
        index = hash_value.take(self._bits)

        child = self._children.get(index)

        if isinstance(child, Bucket):
            return child._find_place(hash_value)
        
        return BucketPosition(self, index, hash_value, child)
    
    def _find_new_bucket_and_pos(self, key: Union[str, InfiniteHash]) -> BucketPosition[T]:
        place = self._find_place(key)
        if place.existing_child is not None and place.existing_child.key != key:
            bucket = Bucket(self._bits, self._hash, place.bucket, place.pos)
            place.bucket._put_object_at(place.pos, bucket)

            new_place = bucket._find_place(place.existing_child.hash)
            new_place.bucket._put_at(new_place, place.existing_child.key, place.existing_child.value)

            return bucket._find_new_bucket_and_pos(place.hash)
        return place

    def _put_at(self, place: BucketPosition[T], key: str, value: T):
        self._put_object_at(place.pos, BucketChild(key, value, place.hash))

    def _put_object_at(self, pos: int, object: Union['Bucket[T]', BucketChild[T]]):
        if self._children.get(pos) is None:
            self._pop_count += 1
        self._children[pos] = object

    def _del_at(self, pos: int):
        assert pos >= 0
        if self._children.get(pos) is not None:
            self._pop_count -= 1
        del self._children[pos]
        self._level()

    def _at(self, pos: int):
        return self._children.get(pos)

    def _level(self):
        if self._parent is not None and self._pop_count <= 1:
            if self._pop_count == 1:
                only_child = next(self._children.values())
                if only_child is not None and not isinstance(only_child, Bucket):
                    only_child.hash.untake(self._bits)
                    place = BucketPosition(self._parent, 
                                           self._pos_at_parent,
                                           only_child.hash,
                                           None)
                    self._parent._put_at(place, only_child.key, only_child.value)
            else:
                self._parent._del_at(self._pos_at_parent)
