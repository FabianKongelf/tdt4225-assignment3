from DbConnector import DbConnector
from tabulate import tabulate
from pprint import pprint 
import os


def main():
    # init program
    try:
        connection = DbConnector()
        client = connection.client
        db = connection.db

        print("TEST TEST TEST \n" )

        result = db["activity"].aggregate([{
            "$group": {
                "_id": "$transportation_mode",
                "tot": { "$sum": 1 }
            }
        }])
        for res in result:
            pprint(res["tot_tp"])
        
    except Exception as e:
        print("ERROR: Failed to use database:", e)
    
    finally:
        connection.close_connection()

if __name__ == '__main__':
    main()