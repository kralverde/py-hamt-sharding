from hamt_sharding.consumable_buffer import ConsumableBuffer

def test_consumable():
    empty_buf = ConsumableBuffer(b'')
    assert empty_buf.take(0) == 0
    assert empty_buf.take(100) == 0
    assert empty_buf.take(1000) == 0

    buf = ConsumableBuffer(b'\0')
    assert buf.take(0) == 0
    assert buf.take(100) == 0
    assert buf.take(1000) == 0

    buf = ConsumableBuffer(bytes([0b11111111]))
    assert buf.take(0) == 0
    for _ in range(8):
        assert buf.take(1) == 1
    assert buf.take(1) == 0
    assert buf.take(100) == 0
    assert buf.take(1000) == 0

    buf = ConsumableBuffer(bytes([0xff, 0xff, 0xff]))
    assert buf.take(0) == 0
    for _ in range(24):
        assert buf.take(1) == 1
    assert buf.take(1) == 0

    buf = ConsumableBuffer(bytes([0xff, 0xff, 0xff]))
    for _ in range(12):
        assert buf.take(2) == 3
    assert buf.take(1) == 0

    buf = ConsumableBuffer(bytes([0xff, 0xff, 0xff]))
    assert buf.take(24) == 0b111111111111111111111111
    assert buf.take(1) == 0

    buf.untake(2)
    assert buf.take(2) == 3
    assert buf.take(1) == 0
