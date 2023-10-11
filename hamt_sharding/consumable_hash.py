from typing import Callable, Union, List

from .consumable_buffer import ConsumableBuffer

HashFunction = Callable[[bytes], bytes]
InfiniteWrapper = Callable[[Union['InfiniteHash', bytes]], 'InfiniteHash']

def wrap_hash(hash_function: HashFunction) -> InfiniteWrapper:
    def hashing(value: Union[bytes, InfiniteHash]) -> InfiniteHash:
        if isinstance(value, InfiniteHash):
            return value
        return InfiniteHash(value, hash_function)
    return hashing

class InfiniteHash:
    def __init__(self, value: bytes, hash_function: HashFunction):
        assert isinstance(value, bytes)
        self._value = value
        self._hash_function = hash_function
        self._depth = -1
        self._available_bits = 0
        self._current_buffer_index = 0
        self._buffers: List[ConsumableBuffer] = []

    def take(self, bits: int) -> int:
        while self._available_bits < bits:
            self._produce_more_bits()
        result = 0
        while bits > 0:
            buffer_hash = self._buffers[self._current_buffer_index]
            avaliable = min(buffer_hash.available_bits(), bits)
            took = buffer_hash.take(avaliable)
            result = (result << avaliable) + took
            bits -= avaliable
            self._available_bits -= avaliable

            if buffer_hash.available_bits() == 0:
                self._current_buffer_index += 1
        return result
    
    def untake(self, bits: int) -> None:
        while bits > 0:
            buffer_hash = self._buffers[self._current_buffer_index]
            avaliable_for_untake = min(buffer_hash.total_bits() - buffer_hash.available_bits(), bits)
            buffer_hash.untake(avaliable_for_untake)
            bits -= avaliable_for_untake
            self._available_bits += avaliable_for_untake

            if self._current_buffer_index > 0 and buffer_hash.total_bits() == buffer_hash.available_bits():
                self._depth -= 1
                self._current_buffer_index -= 1

    def _produce_more_bits(self) -> None:
        self._depth += 1

        # JS Uint8Array.from([]) modulos with max value; implement that here
        value = self._value + bytes([self._depth % 0x100]) if self._depth > 0 else self._value
        hash_value = self._hash_function(value)
        buffer = ConsumableBuffer(hash_value)
        self._buffers.append(buffer)
        self._available_bits += buffer.available_bits()
