START_MASKS = [
    0b11111111,
    0b11111110,
    0b11111100,
    0b11111000,
    0b11110000,
    0b11100000,
    0b11000000,
    0b10000000
]

STOP_MASKS = [
    0b00000001,
    0b00000011,
    0b00000111,
    0b00001111,
    0b00011111,
    0b00111111,
    0b01111111,
    0b11111111
]

def mask_for(start: int, length: int) -> int:
    return START_MASKS[start] & STOP_MASKS[min(length + start - 1, 7)]

def byte_bits_to_int(value: int, start: int, length: int) -> int:
    mask = mask_for(start, length)
    return (value & mask) >> start

class ConsumableBuffer:
    def __init__(self, value: bytes):
        self._value = value
        self._current_byte_pos = len(value) - 1
        self._current_bit_pos = 7

    def available_bits(self) -> int:
        return self._current_bit_pos + 1 + self._current_byte_pos * 8
    
    def total_bits(self) -> int:
        return len(self._value) * 8
    
    def _have_bits(self) -> bool:
        return self._current_byte_pos >= 0
    
    def take(self, bits: int) -> int:
        result = 0
        while bits > 0 and self._have_bits():
            bite = self._value[self._current_byte_pos]
            avaliable_bits = self._current_bit_pos + 1
            taking = min(avaliable_bits, bits)
            value = byte_bits_to_int(bite, avaliable_bits - taking, taking)
            result = (result << taking) + value

            bits -= taking
            self._current_bit_pos -= taking

            if self._current_bit_pos < 0:
                self._current_bit_pos = 7
                self._current_byte_pos -= 1
        return result
    
    def untake(self, bits: int) -> None:
        self._current_bit_pos += bits
        while self._current_bit_pos > 7:
            self._current_bit_pos -= 8
            self._current_byte_pos += 1
