from pympler.asizeof import asizeof
from .errors import ItemNotFoundError
from .item import CacheItem


class Engine:
    mibibytes = 1024*1024

    def __init__(self, total_memory=None, offload_size=None, dump_path=None):
        """
            Initialize CacheEngine

            Keyword arguments:
            total_memory -- Memory the engine is allowed to use until it has to
            offload
            offload_size -- How much memory the engine should dump to drive
            when it is offloading
            dump_path -- Path to dump cache items to during offload

        """
        self.data = dict()

        self.offload_size = offload_size if offload_size else 20
        self.is_offloading = False

        self.total_memory = total_memory if total_memory else 100
        self.offload_size = offload_size if offload_size else 20

        self.dump_path = dump_path if dump_path else '/tmp/xcacher/'

        self.total_memory = self.total_memory * self.mibibytes
        self.offload_size = self.offload_size * self.mibibytes

    def __str__(self):
        return (f"Allocated memory: {self.total_memory} bytes\n"
                f"Allocated offload size: {self.offload_size} bytes\n"
                f"Cached items: {len(self.data)}\n"
                f"Used memory: {asizeof(self)} bytes\n"
                f"Persistence path: {self.dump_path}")

    def store(self, key, data):
        """Store an item in the cache

        Keyword arguments:
        key -- the key with which the data will be identified
        data -- the data object to be cached
        """
        self.data[key] = CacheItem(data, key, path=self.persistence_path)

        # Check if the new item breaks the specified memory threshold
        # If it does, begin the offloading process
        curr_size = asizeof(self)
        if(curr_size >= self.total_memory):
            self.offload()

    def get(self, key):
        """Retrieve an item from the cache

        Keyword arguments:
        key -- the key with which the item will be identified

        Raises ItemNotFoundError if the key could not be found
        """
        try:
            item = self.data[key]
            return item
        except KeyError:
            raise ItemNotFoundError(f'{key} could not be found')

    def size(self, key=None):
        """Return the size of an item from the cache or size of all items in
        the cache

        Keyword arguments:
        key -- the key with which the item will be identified
        """
        if not key:
            return asizeof(self.data)

        try:
            return asizeof(self.data[key])
        except KeyError:
            raise ItemNotFoundError(f'{key} could not be found')

    def offload(self):
        """
        Offloads the cached data to disk persistence sorted by last accessed
        timestamp until offload_size is freed from total_memory.
        Item identifiers are still kept in the cache engine to later
        retrieve and load them back into memory when necessary.

        The cache engine automatically checks if the allowed memory
        threshold has been crossed and calls offload during store.
        """
        self.is_offloading = True

        # Create key value pair with timestamp being key
        # and CacheItem being value
        data = {}
        for i in self.data.keys():
            item = self.data[i]
            if not item.is_offloaded:
                data[item.last_accessed] = item.assigned_key

        # Sort by timestamp
        keys = list(data.keys())
        keys.sort()
        free = 0.0

        for key in keys:
            if free >= self.offload_size:
                break
            ac_key = data[key]
            item = self.data[ac_key]
            size = asizeof(item)
            item.offload()
            free = free + size

        self.is_offloading = False
