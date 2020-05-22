"""
Zarr spec v3 draft implementation
"""

__version__ = "0.0.1"

from string import ascii_letters, digits

import trio


class MemoryStoreV3:
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

    async def get(self, key: str):
        assert self._valid_path(key)
        result = self._backend[key]
        assert isinstance(result, bytes)
        return result

    async def set(self, key: str, value: bytes):
        assert isinstance(value, bytes)
        assert self._valid_path(key)
        self._backend[key] = value

    def delete(self, key):
        del self._backend[key]

    def list(self):
        return list(self._backend.keys())

    def list_prefix(self, prefix):
        return [k for k in self.list() if k.startswith(prefix)]

    def list_dir(self, prefix):
        """
        Note: carefully test this with trailing/leading slashes
        """

        all_keys = self.list_prefix(prefix)
        len_prefix = len(prefix)
        trail = {k[len_prefix:].split("/", maxsplit=1)[0] for k in all_keys}
        return [prefix + k for k in trail]


class ZarrProtocolV3:

    def __init__(self, store=MemoryStoreV3):
        self._store = store()

    def _g_meta_key(self, key):
        return 'meta/'+key+'.group'

    async def create_group(self, group_path: str):
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
        await self._store.set(self._g_meta_key(group_path), DEFAULT_GROUP.encode())

    async def create_array(self, group_path: str):
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
        await self._store.set(self._g_meta_key(group_path), DEFAULT_GROUP.encode())
