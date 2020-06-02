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


def test_misc():
    from zarr3 import V2from3Adapter, MemoryStoreV3
    from zarr.storage import init_group

    store = V2from3Adapter(MemoryStoreV3())
    
    init_group(store)
    
    assert store._v3store._backend ==  {'meta/root.group': b'{\n    "zarr_format": 2\n}'}
    assert store['.zgroup'] ==  b'{\n    "zarr_format": 2\n}'
