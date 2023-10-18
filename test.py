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

        result = db["activity"].aggregate([
        { # all activites with a transportation mode not none
            "$match": {
                "transportation_mode": {
                    "$ne": "none"
                }
            }
        }, { # group on user
            "$group": {
                "_id": {"user": "$user", "mode": "$transportation_mode"},
                "count": { "$sum": 1 }
            }
        }, {
            "$sort": { "count": -1 }
        }, {
            "$group": {
                "_id": "$_id.user",
                "most_used_mode": {"$first": "$_id.mode"},
                # "count": {"$first": "$count"}
            }
        }, {
            "$sort": {
                "_id": 1
            }
        }])
        
        print("User | Mode")
        print(tabulate(result))
        
    except Exception as e:
        print("ERROR: Failed to use database:", e)
    
    finally:
        connection.close_connection()

if __name__ == '__main__':
    main()