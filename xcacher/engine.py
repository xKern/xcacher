from pympler.asizeof import asizeof
from .errors import ItemNotFoundError
from .item import CacheItem


class Engine:
    defaults = {
        # How much to offload (mib)
        "offload_size": 20,
        # Total persistence in memory (mib)
        "total_memory": 100,
        "persistence_path": "/tmp"
    }

    def __init__(self, **kwargs):
        self.total_memory = kwargs.get('total_memory',
                                       self.defaults['total_memory'])
        self.persistence_path = kwargs.get('persistence_path',
                                           self.defaults['persistence_path'])

        self.offload_size = kwargs.get('offload_size',
                                       self.defaults['offload_size'])
        self.data = {}

        self.total_memory = self.total_memory*1024*1024
        self.offload_size = self.offload_size*1024*1024
        self.is_offloading = False

    def __str__(self):
        return (f"Total allowed memory: {self.total_memory/1024/1024} MiB"
                f"\nTotal items: {len(self.data)}\n"
                f"Offload size: {self.offload_size/1024/1024} MiB\n"
                f"Current memory usage: "
                f"{asizeof(self.data)/1024/1024} MiB\n"
                f"Currently offloading: {self.is_offloading}")

    def store(self, key, data):
        # Check if the new data coming in would  break the memory threshold
        # If it does, then offload
        self.data[key] = CacheItem(data, key, path=self.persistence_path)
        curr_size = asizeof(self)
        if(curr_size >= self.total_memory):
            self.offload()

    def get(self, key):
        try:
            item = self.data[key]
            return item
        except KeyError:
            raise ItemNotFoundError(f'{key} could not be found')

    def size(self, key=None):
        if not key:
            return asizeof(self.data)

        try:
            return asizeof(self.data[key])
        except KeyError:
            raise ItemNotFoundError(f'{key} could not be found')

    def offload(self):
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
