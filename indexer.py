import hashlib
import pandas as pd

class Indexer:
    def __init__(self):
        self.index = pd.DataFrame(columns=['hash_id', 'data'])
        self.current_id = 1
    
    def generate_hash_id(self, data):
        return hashlib.sha256(data.encode()).hexdigest()

    def add_entry(self, data):
        hash_id = self.generate_hash_id(data)
        if not self.search_by_hash_id(hash_id):
            self.index = self.index.append({'hash_id': hash_id, 'data': data}, ignore_index=True)
            self.current_id += 1
        else:
            pass 
    
    def search_by_hash_id(self, hash_id):
        if not self.index[self.index['hash_id'] == hash_id].empty:
            return True
        else: 
            return False
    
    def return_link_list(self):
        return self.index

