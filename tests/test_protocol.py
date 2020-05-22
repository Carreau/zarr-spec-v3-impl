import pytest
from zarr3 import MemoryStoreV3, ZarrProtocolV3


async def test_scenario():

    store = MemoryStoreV3()

    await store.set("data/a", bytes(1))

    with pytest.raises(ValueError):
        await store.get("arbitrary")
    with pytest.raises(ValueError):
        await store.get("data")
    with pytest.raises(ValueError):
        await store.get("meta")  # test commit

    assert await store.get("data/a") == bytes(1)

    await store.set("meta/this/is/nested", bytes(1))
    await store.set("meta/this/is/a/group", bytes(1))
    await store.set("meta/this/also/a/group", bytes(1))
    await store.set("meta/thisisweird/also/a/group", bytes(1))

    assert len(await store.list()) == 5

    assert await store.list_dir("meta/this") == ["meta/this", "meta/thisisweird"]


async def test_2():
    protocol = ZarrProtocolV3()
    store = protocol._store

    await protocol.create_group('g1')
    assert isinstance(await store.get('meta/g1.group'), bytes)
