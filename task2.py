from DbConnector import DbConnector
from tabulate import tabulate
from pprint import pprint 
import os

class Task2():
    def __init__(self):
        self.connection = DbConnector()
        self.client = self.connection.client
        self.db = self.connection.db
    
    def fetch_documents(self, collection_name):
        collection = self.db[collection_name]
        documents = collection.find({})
        for doc in documents: 
            pprint(doc)

def main():
    program = None

    # init program
    try:
        program = Task2()

        program.fetch_documents("users")


    except Exception as e:
        print("ERROR: Failed to use database:", e)
    finally:
        if program:
            program.connection.close_connection()

if __name__ == '__main__':
    main()