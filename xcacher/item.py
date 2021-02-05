import time
import pickle
import hashlib
import os


class CacheItem:
    """
    Initializes a CacheItem

    Keywords:
    key -- the key that the data is asociated with
    data -- the actual data object to be cached
    path -- path where the cache item should serialize itself on the disk
    """
    def __init__(self, key, data, path):
        self.__data = data
        self.assigned_key = key
        self.added = time.time()
        self.last_accessed = time.time()
        self.is_offloaded = False

        # Generate an identifier for self
        self.id_ = hashlib.sha1(
            f'{self.assigned_key}_{time.time()}'.encode()
        ).hexdigest()

        self.path = f"{path}/{self.id_}"

    @property
    def data(self):
        """ A getter for the item's data property

        This is done so that we have control over reloading the cached data
        back to memory from disk if it has been offloaded and so that each item
        can be sorted later according to their last accessed timestamp.
        """
        if not self.__data:
            self.reload()
            self.delete_from_disk()
            # TODO: Make delete operation async

        self.last_accessed = time.time()
        return self.__data

    def delete_from_disk(self):
        """ Deletes item's persistence file from disk """
        os.remove(self.path)

    def offload(self):
        """ Offloads item data onto disk """
        with open(self.path, 'wb') as f:
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
        """ Reloads item's data back into memory from disk """
        with open(self.path, 'rb') as f:
            x = pickle.load(f)
            self.__dict__ = x.__dict__
            self.is_offloaded = False

    def __getstate__(self):
        return self.__dict__

    def __setstate__(self, data):
        self.__dict__ = data
