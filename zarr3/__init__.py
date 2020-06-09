"""
Zarr spec v3 draft implementation
"""

__version__ = "0.0.1"

from string import ascii_letters, digits
from .utils import AutoSync

import json

class BaseV3Store(AutoSync):
    """
    Base utility class to create a v3-complient store with extra checks and utilities. 

    It provides a number of default method implementation adding extra checks in order to ensure the correctness fo the implmentation.
    """

    @staticmethod
    def _valid_path(key: str) -> bool:
        """
        Verify that a key is confirm to the specification. 

        A key us any string containing only character in the range a-z, A-Z,
        0-9, or in the set /.-_ it will return True if that's the case, false
        otherwise.

        In addition, in spec v3, keys can only start with the prefix meta/,
        data/ or be exactly zarr.json. This should not be exposed to the
        user, and is a store implmentation detail, so thie method will raise
        a ValueError in that case.
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
        """
        default implementation of async_get/get that validate the key, and
        check that the return value by bytes. rely on `async def _get(key)`
        to be implmented.

        Will ensure that the following are correct:
            - return group metadata objects are json and contain a signel `attributes` keys.
        """
        assert self._valid_path(key)
        result = await self._get(key)
        assert isinstance(result, bytes), f"Expected bytes, got {result}"
        if key.endswith('/.group'):
            v= json.loads(result.decode())
            assert set(v.keys()) ==  {'attributes'}, f"got unexpected keys {v.keys()}"
        return result

    async def async_set(self, key: str, value: bytes):
        """
        default implementation of async_set/set that validate the key, and
        check that the return value by bytes. rely on `async def _set(key, value)`
        to be implmented.

        Will ensure that the following are correct:
            - set group metadata objects are json and contain a signel `attributes` keys.
        """
        if key.endswith('/.group'):
            v= json.loads(value.decode())
            assert set(v.keys()) ==  {'attributes'}, f"got unexpected keys {v.keys()}"
        if not isinstance(value, bytes):
            raise TypeError(f"expected, bytes, or bytesarray, got {type(value)}")
        assert self._valid_path(key)
        await self._set(key, value)

    async def async_initialize(self):
        """
        Default implementation to initilize async store. 

        Async store may need to do asyncronous initialisation, but this is not possible in `__init__` which is sync.

        """
        pass

    async def async_list_prefix(self, prefix):
        return [k for k in await self.async_list() if k.startswith(prefix)]

    async def async_delete(self, key):
        deln = await self._backend().delete(key)
        if deln == 0:
            raise KeyError(key)




class RedisStore(BaseV3Store):
    def __init__(self):
        """initialisation is in _async initialize
        for early failure.
        """
        pass

    def __getstate__(self):
        return {}

    def __setstate__(self, state):
        self.__init__()
        from redio import Redis
        self._backend = Redis("redis://localhost/")



    async def async_initialize(self):
        from redio import Redis
        self._backend = Redis("redis://localhost/")
        b = self._backend()
        for k in await self._backend().keys():
            b.delete(k)
        await b


    async def _get(self, key):
        res = await self._backend().get(key)
        if res is None:
            raise KeyError
        return res

    async def _set(self, key, value):
        return await self._backend().set(key, value)

    async def async_list(self):
        return await self._backend().keys()

class MemoryStoreV3(BaseV3Store):


    def __init__(self):
        self._backend = dict()

    async def _get(self, key):
        return self._backend[key]

    async def _set(self, key, value):
        self._backend[key]  = value

    async def async_delete(self, key):
        del self._backend[key]

    async def async_list(self):
        return list(self._backend.keys())

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
                assert False, f"should raise, got {k2} for {key}"
            except Exception as e2:
                raise
                if not isinstance(e2, type(e1)):
                    raise AssertionError("Expecting {type(e1)} got {type(e2)}") from e2
            raise
        k2 = self.tested[key]
        if key.endswith('.zgroup'):
            assert json.loads(k1.decode()) == json.loads(k2.decode())
        else:
            assert k2 == k1 , f"{k1} != {k2}"
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
            raise
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
         - try to adapt v2/v2 nested/flat structures

        THere will ikley need to be _some_

        """
        self._v3store = v3store

    def __getitem__(self, key):
        """
        In v2  both metadata and data are mixed so we'll need to convert things that ends with .z to the metadata path.
        """
        assert isinstance(key, str), f"expecting string got {key!r}"
        v3key = self._convert_2_to_3_keys(key)
        res = self._v3store.get(v3key)
        if v3key == 'meta/root.group':
            data = json.loads(res.decode())
            data['zarr_format'] = 2
            res = json.dumps(data, indent=4).encode()
        elif v3key.endswith('/.group'):
            data = json.loads(res.decode())
            data['zarr_format'] = 2
            if not data['attributes']:
                del data['attributes']
            res = json.dumps(data).encode()
        assert isinstance(res, bytes)
        return res

    def __setitem__(self, key, value):
        """
        In v2  both metadata and data are mixed so we'll need to convert things that ends with .z to the metadata path.
        """
        # TODO convert to bytes if needed
        from numcodecs.compat import ensure_bytes
        parts = key.split('/')
        v3key = self._convert_2_to_3_keys(key)
        if v3key == 'meta/root.group':
            data = json.loads(value.decode())
            data['zarr_format'] = "https://purl.org/zarr/spec/protocol/core/3.0"
            data = json.dumps(data, indent=4).encode()
        elif v3key.endswith('/.group'):
            data = json.loads(value.decode())
            del data['zarr_format']
            if 'attributes' not in data:
                data['attributes'] = {}
            data = json.dumps(data).encode()
        else:
            data = value
        self._v3store.set(v3key, ensure_bytes(data))

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
