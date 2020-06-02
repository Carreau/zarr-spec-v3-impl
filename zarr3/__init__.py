"""
Zarr spec v3 draft implementation
"""

__version__ = "0.0.1"

from string import ascii_letters, digits
from .utils import AutoSync

import json


class MemoryStoreV3(AutoSync):
    def __init__(self):
        self._backend = dict()

    def _valid_path(self, key: str) -> bool:
        """
        A key us any string containing only character in the range a-z, A-Z, 0-9, or in the set /.-_
        """
        if not key.isascii():
            return False
        if set(key) - set(ascii_letters + digits + "/.-_"):
            return False

        if (
            not key.startswith("data/")
            and (not key.startswith("meta/"))
            and (not key == "zarr.json")
        ):
            raise ValueError(f"keys starts with unexpected value: `{key}`")
        # todo likely more logics to add there.
        return True

    async def async_get(self, key: str):
        assert self._valid_path(key)
        result = self._backend[key]
        assert isinstance(result, bytes)
        return result

    async def async_set(self, key: str, value: bytes):
        if not isinstance(value, bytes):
            raise TypeError(f"expected, bytes, or bytesarray, got {type(value)}")
        assert self._valid_path(key)
        self._backend[key] = value

    async def async_delete(self, key):
        del self._backend[key]

    async def async_list(self):
        return list(self._backend.keys())

    async def async_list_prefix(self, prefix):
        return [k for k in await self.async_list() if k.startswith(prefix)]

    async def async_list_dir(self, prefix):
        """
        Note: carefully test this with trailing/leading slashes
        """

        all_keys = await self.async_list_prefix(prefix)
        print('store:', self._backend)
        print('all keys', all_keys)
        len_prefix = len(prefix)
        trail = {k[len_prefix:].split("/", maxsplit=1)[0] for k in all_keys}
        return [prefix + k for k in trail]


class ZarrProtocolV3(AutoSync):

    def __init__(self, store=MemoryStoreV3):
        self._store = store()
        self.init_hierarchy()

    def init_hierarchy(self):
        basic_info = {
            "zarr_format": "https://purl.org/zarr/spec/protocol/core/3.0",
            "metadata_encoding": "application/json",
            "extensions": []
        }
        try:
            self._store.get('zarr.json')
        except KeyError:
            self._store.set('zarr.json', json.dumps('basic_info').encode())

    def _g_meta_key(self, key):
        return 'meta/'+key+'.group'

    def _a_meta_key(self, key):
        return 'meta/'+key+'.array'

    async def async_create_group(self, group_path: str):
        """
        create a goup at `group_path`, 
        we need to make sure none of the subpath of group_path are arrays. 

        say  path is g1/g2/g3, we want to check

        /meta/g1.array
        /meta/g1/g2.array

        we could also assume that protocol implementation never do that.
        """
        DEFAULT_GROUP = """{
                "extensions": [],
        "attributes": {
            "spam": "ham",
            "eggs": 42,
        }  }
        """
        await self._store.async_set(self._g_meta_key(group_path), DEFAULT_GROUP.encode())

    def _create_array_metadata(self, shape=(10, ), dtype='<f64', chunk_shape=(1,)):
        metadata = {
            "shape": shape,
            "data_type": dtype,
            "chunk_grid": {"type": "regular",
                           "chunk_shape":  chunk_shape
                           },
            "chunk_memory_layout": "C",
            "compressor": {
                "codec": "https://none",
                "configuration": {},
                "fill_value": "NaN",
            },
            "extensions": [],
            "attributes": {}
        }

    async def create_array(self, array_path: str):
        """
        create a goup at `array_path`, 
        we need to make sure none of the subpath of array_path are arrays. 

        say  path is g1/g2/d3, we want to check

        /meta/g1.array
        /meta/g1/g2.array

        we could also assume that protocol implementation never do that.
        """
        DEFAULT_ARRAY = """{
                "extensions": [],
        "attributes": {
            "spam": "ham",
            "eggs": 42,
        }  }
        """
        await self._store.set(self._g_meta_key(group_path), DEFAULT_GROUP.encode())


from collections.abc import MutableMapping

class StoreComparer(MutableMapping):
    """
    Compare two store implementations, and make sure to do the same operation on both stores. 

    The operation from the first store are always considered as reference and
    the will make sure the second store will return the same value, or raise
    the same exception where relevant.

    This should have minimal impact on API, but can as some generators are reified and sorted to make sure they are identical.
    """


    def __init__(self, reference, tested):
        self.reference = reference
        self.tested = tested

    def __getitem__(self, key):
        try :
            k1 = self.reference[key]
        except Exception as e1:
            try:
                k2 = self.tested[key]
                assert False, "should raise"
            except Exception as e2:
                assert isinstance(e2, type(e1))
            raise
        k2 = self.tested[key]
        assert k2 == k1
        return k1

    def __setitem__(self, key, value):
        # todo : not quite happy about casting here, maybe we shoudl stay strict ? 
        from numcodecs.compat import ensure_bytes
        values = ensure_bytes(value)
        try:
            self.reference[key] = value
        except Exception as e:
            try: 
                self.tested[key] = value
            except Exception as e2:
                assert isinstance(e, type(e2))
        try:
            self.tested[key] = value
        except Exception as e:
            assert False, f"should not raise, got {e}"

    def keys(self):
        try :
            k1 = list(sorted(self.reference.keys()))
        except Exception as e1:
            try:
                k2 = self.tested.keys()
                assert False, "should raise"
            except Exception as e2:
                assert isinstance(e2, type(e1))
            raise
        k2 = sorted(self.tested.keys())
        assert k2 == k1, f"got {k2}, expecting {k1}"
        return k1

    def __delitem__(self, key):
        try :
            del self.reference[key]
        except Exception as e1:
            try:
                del self.tested[key]
                assert False, "should raise"
            except Exception as e2:
                assert isinstance(e2, type(e1))
            raise
        del self.tested[key]

    def __iter__(self):
        return iter(self.keys())

    def __len__(self):
        return len(self.keys())


class V2from3Adapter(MutableMapping):
    """
    class to wrap a 3 store and return a V2 interface
    """

    def __init__(self, v3store):
        """

        Wrapper arround a v3store to give a v2 compatible interface. 

        Still have some rough edges, and try to do the sensible things for
        most case mostly key converstions so far.

        Note that the V3 spec is still in flux, so this is simply a prototype
        to see the pain points in using the spec v3 and must not be used for
        production.


        This will try to do the followign conversions: 
         - name of given keys `.zgroup` -> `.group` for example. 
         - path of storage (prefix with root/ meta// when relevant and vice versa.)
         - try to ensure the stored objects are bytes before reachign the underlying store. 
         - try to adapt v2/v2 nested/flatt structures

        """
        self._v3store = v3store

    def __getitem__(self, key):
        """
        In v2  both metadata and data are mixed so we'll need to convert things that ends with .z to the metadata path.
        """
        assert isinstance(key, str), f"expecting string got {key!r}"
        res = self._v3store.get(self._convert_2_to_3_keys(key))
        assert isinstance(res, bytes)
        return res

    def __setitem__(self, item, value):
        """
        In v2  both metadata and data are mixed so we'll need to convert things that ends with .z to the metadata path.
        """
        # TODO convert to bytes if needed
        from numcodecs.compat import ensure_bytes
        parts = item.split('/')
        self._v3store.set(self._convert_2_to_3_keys(item), ensure_bytes(value))

    def __contains__(self, key):
        return self._convert_2_to_3_keys(key) in self._v3store.list()

    def _convert_3_to_2_keys(self, v3key: str) -> str:
        """
        todo handle special .attribute which is merged with .zarray
        """
        if v3key == 'meta/root.group':
            return '.zgroup'
        if v3key == 'meta/root.array':
            return '.zarray'
        suffix = v3key[10:]
        if suffix.endswith('.array'):
            return suffix[:-6] + '.zarray'
        if suffix.endswith('.group'):
            return suffix[:-6] + '.zgroup'
        return suffix

    def _convert_2_to_3_keys(self, v2key: str) -> str:
        """
        todo handle special .attribute which is merged with .zarray
        """
        # head of the hierachy is different:
        if v2key == '.zgroup':
            return 'meta/root.group'
        if v2key == '.zarray':
            return 'meta/root.array'
        assert not v2key.startswith(
            '/'), f"expect keys to not start with slash but does {v2key!r}"
        if v2key.endswith('.zarray'):
            return 'meta/root/'+v2key[:-7]+'.array'
        if v2key.endswith('.zgroup'):
            return 'meta/root/'+v2key[:-7]+'.group'
        return 'data/root/'+v2key

    def __len__(self):
        return len(self._v3store.list())

    def clear(self):
        keys = self._v3store.list()
        for k in keys:
            self._v3store.delete(k)

    def __delitem__(self, key):
        item3 = self._convert_2_to_3_keys(key)

        items = self._v3store.list_prefix(item3)
        if not items:
            raise KeyError(
                f"{key} not found in store (converted key to {item3}")
        for _item in self._v3store.list_prefix(item3):
            self._v3store.delete(_item)

    def keys(self):
        return [self._convert_3_to_2_keys(k) for k in self._v3store.list()]

    def listdir(self, path=''):
        v3path = self._convert_2_to_3_keys(path)
        if not v3path.endswith('/'):
            v3path = v3path + '/'
        ps = [p for p in
              self._v3store.list_dir(v3path)
              ]
        tov2 = [
            self._convert_3_to_2_keys(p) for p in ps]

        return [p.split('/')[-1] for p in tov2]

    def __iter__(self):
        return iter(self.keys())

    #def values(self):
    #    for k in self.keys():
    #        yield self[k]

    #def items(self):
    #    for k in self.keys():
    #        yield k, self[k]
