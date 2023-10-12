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
            "$match": {
                "transportation_mode": "taxi"
            },
            "$group": {
                "_id": "null",
                "user": {
                    "$distinct": "$user"
                }
            }
        }])
        for res in result:
            pprint(res["tot_tp"])


        connection.close_connection()
    except Exception as e:
        print("ERROR: Failed to use database:", e)

if __name__ == '__main__':
    main()