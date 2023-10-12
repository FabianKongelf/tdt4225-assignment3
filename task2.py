from DbConnector import DbConnector
from tabulate import tabulate
from pprint import pprint 
import os


def main():
    # init program
    try:
        # program = Task2()

        connection = DbConnector()
        client = connection.client
        db = connection.db

        # ----------------------------------------
        # Task 1
        # ----------------------------------------

        print("Task 1: how many users, activities and trackpoint in db. \n" )

        result = db["users"].count_documents({})
        print("Total amount of users: ", result)

        result = db["activity"].count_documents({})
        print("Total amount of activites: ", result)

        result = db["activity"].aggregate([{
            "$group": {
                "_id": "null", 
                "tot_tp": {
                    "$sum": {
                        "$size": "$trackpoints"
                    }
                }
            }
        }])
        print("Total amount of trackpoints: ", end="")
        for res in result:
            pprint(res["tot_tp"])

        print("\n-----------------------------------------------\n")


        connection.close_connection()
    except Exception as e:
        print("ERROR: Failed to use database:", e)

if __name__ == '__main__':
    main()