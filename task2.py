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

        print("Task 1: How many users, activities and trackpoints are there in the dataset \n" )

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


        # ----------------------------------------
        # Task 2
        # ----------------------------------------

        print("Task 2: Find avg amount of activites per user.\n")

        ant_activites = db["activity"].count_documents({})
        ant_users = db["users"].count_documents({})

        print("Avg activites per user: ", round(ant_activites / ant_users, 1))

        print("\n-----------------------------------------------\n")


        # ----------------------------------------
        # Task 3
        # ----------------------------------------

        print("Task 3: Find the top 20 users with the highest number of activities \n")

        pipeline = [
            { "$group": {
                "_id": "$user",
                "count": { "$sum": 1 }
                }
            },
            { "$sort": { "count": -1 } },
        { "$limit": 20 }
        ]

        result = db.activity.aggregate(pipeline)

        for document in result:
            print(document)

        print("\n-----------------------------------------------\n")


        # ----------------------------------------
        # Task 4
        # ----------------------------------------

        # print("Task 4: Find all users who have taken a taxi \n")

        # Insert code here

        # print("\n-----------------------------------------------\n")


        # ----------------------------------------
        # Task 5
        # ----------------------------------------

        print("Task 5: Find all types of transportation modes and count how many activities that are tagged with these transportation mode labels. Do not count the rows where the mode is null \n")

        result = db["activity"].aggregate([{
                "$match": {
                    "transportation_mode": {
                        "$ne": "none"
                    }
                }
            }, {
                "$group": {
                    "_id": "$transportation_mode",
                    "tot": { "$sum": 1 }
                }
            }, {
                "$sort": {
                    "tot": -1
                }
            }])
        for res in result:
            pprint(res)


        print("\n-----------------------------------------------\n")

        

        # ----------------------------------------
        # Task 6
        # ----------------------------------------

        # print("Task 6: a) Find the year with the most activities \n")

        pipeline = [
            {"$project": {"year": {"$year": {"$dateFromString": {"dateString": "$start_date","format": "%Y/%m/%d %H:%M:%S"}}}}},
            {"$group": {"_id": "$year", "count": { "$sum": 1 }}},
            {"$sort": {"count": -1}},
            {"$limit": 1}
        ]

        result = list(db.activity.aggregate(pipeline))

        for document in result:
            print(document)

        # print("\n\nTask 6: b) Find the year with the most recorded hours \n")

        # Insert code here

        # print("\n-----------------------------------------------\n")


        # ----------------------------------------
        # Task 7
        # ----------------------------------------

        # print("Task 7: Find the total distance (in km) walked in 2008, by user with id=112 ")
        # Insert code here

        # print("\n-----------------------------------------------\n")


        # ----------------------------------------
        # Task 8
        # ----------------------------------------

        # print("Task 8: Find the top 20 users who have gained the most altitude meters ")
        # Insert code here

        # print("\n-----------------------------------------------\n")


        # ----------------------------------------
        # Task 9
        # ----------------------------------------

        # print("Task 9: Find all users who have invalid activities, and the number of invalid activities per user ")
        # Insert code here

        # print("\n-----------------------------------------------\n")


        # ----------------------------------------
        # Task 10
        # ----------------------------------------

        # print("Task 10: Find the users who have tracked an activity in the Forbidden City of Beijing ")
        # Insert code here

        # print("\n-----------------------------------------------\n")


        # ----------------------------------------
        # Task 11
        # ----------------------------------------

        # print("Task 11: Find all users who have registered transportation_mode and their most used transportation_mode ")
        # Insert code here

        # print("\n-----------------------------------------------\n")

        connection.close_connection()
    except Exception as e:
        print("ERROR: Failed to use database:", e)

if __name__ == '__main__':
    main()

# https://gitlab.stud.idi.ntnu.no/tdt4225lukrik/assignment3/-/tree/d37d03db4b643f018d234e0b1ad4210350b3e995
