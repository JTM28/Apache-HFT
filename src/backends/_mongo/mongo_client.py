import re
import ast

from pymongo import MongoClient
from pymongo import errors


class MongoConfig(object):

    def __init__(self):
        with open('C:\\Users\jtmar\OneDrive\Desktop\Apache-HFT\.gitignore') as file:
            for line in file:
                if len(line) > 0:
                    self.line = line
                    if re.search(r"^{'MONGO_URI", str(line)):
                        config = ast.literal_eval(line)
                        self.MONGO_URI = config['MONGO_URI']


class DBClient(MongoConfig):

    def __init__(self, threads=100, **kwargs):
        super().__init__()

        self.client = MongoClient(self.MONGO_URI)
        print(self.client)

    def get_dbs(self):
        print(self.client)




