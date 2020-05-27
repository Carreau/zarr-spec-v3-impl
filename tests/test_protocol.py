import pytest
from zarr3 import MemoryStoreV3, ZarrProtocolV3


async def test_scenario():

    store = MemoryStoreV3()

    store.set("data/a", bytes(1))

    with pytest.raises(ValueError):
        store.get("arbitrary")
    with pytest.raises(ValueError):
        store.get("data")
    with pytest.raises(ValueError):
        store.get("meta")  # test commit

    assert store.get("data/a") == bytes(1)
    assert await store.async_get("data/a") == bytes(1)

    await store.async_set("meta/this/is/nested", bytes(1))
    await store.async_set("meta/this/is/a/group", bytes(1))
    await store.async_set("meta/this/also/a/group", bytes(1))
    await store.async_set("meta/thisisweird/also/a/group", bytes(1))

    assert len(store.list()) == 5

    assert store.list_dir("meta/this") == ["meta/this", "meta/thisisweird"]
    assert await store.async_list_dir("meta/this") == ["meta/this", "meta/thisisweird"]


async def test_2():
    protocol = ZarrProtocolV3()
    store = protocol._store

    await protocol.async_create_group('g1')
    assert isinstance(store.get('meta/g1.group'), bytes)
