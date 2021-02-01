import time
import pickle
import hashlib
import os


class CacheItem:
    def __init__(self, data, key, **kwargs):
        self.path = kwargs.get('path', '')
        self.__data = data
        self.assigned_key = key
        self.added = time.time()
        self.last_accessed = time.time()
        self.is_offloaded = False

        # Generate an identifier for self
        self.id_ = hashlib.sha1(
            f'{self.assigned_key}_{time.time()}'.encode()
        ).hexdigest()

    @property
    def data(self):
        if not self.__data:
            self.reload()
            self.delete_from_disk()
            # TODO: Make delete operation async

        self.last_accessed = time.time()
        return self.__data

    def delete_from_disk(self):
        os.remove(f"{self.path}/{self.id_}")

    def offload(self):
        with open(f'{self.path}/{self.id_}', 'wb') as f:
            try:
                pickle.dump(self, f)
            except Exception as e:
                print(str(e))
                self.is_offloaded = False
                return
        # Assuming everything went well
        self.__data = None
        self.is_offloaded = True

    def reload(self):
        with open(f'{self.path}/{self.id_}', 'rb') as f:
            x = pickle.load(f)
            self.__dict__ = x.__dict__
            self.is_offloaded = False

    def __getstate__(self):
        return self.__dict__

    def __setstate__(self, data):
        self.__dict__ = data
