from ralf import Record

class Disk:

    def __init__(self):
        self.storage = dict()
    
    def fetch(self, key) -> 'tuple[Record, int]':
        if key in self.storage:
            return self.storage[key]
        return None, None

    def save(self, key, value):
        self.storage[key] = value
