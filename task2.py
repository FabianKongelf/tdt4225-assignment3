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

        print("Task 1: How many users, activities and trackpoint in db. \n" )

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

        result = db["activity"].aggregate([{ 
                "$group": { 
                    "_id": "$user", 
                    "tot": { "$sum": 1 } 
                } 
            }, { 
                "$sort": { "tot": -1 } 
            }, { 
                "$limit": 20 
            }])
        for res in result:
            pprint(res)

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

        # Insert code here

        # print("Task 6: b) Is this also the year with most recorded hours? \n")

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

        print("Task 8: Find the top 20 users who have gained the most altitude meters (This query takes a long time)")

        # identify all users
        users = db["users"].find()
        result = []

        # loop thourgh every user
        for user in users:
            activites = db["activity"].find({ "user": user["_id"] })
            
            altitude = 0
            for activity in activites:
                for i in range(1, len(activity["trackpoints"])):
                    gain = (float(activity["trackpoints"][(i-1)]["altitude"]) - float(activity["trackpoints"][i]["altitude"]))
                    if gain > 0:
                        altitude += gain
            result.append({"id": user["_id"], "altitude": altitude})
            # print({"id": user["_id"], "altitude": altitude})
        
        sorted_result = sorted(result, key=lambda x: x["altitude"], reverse=True)
        for i in range(0, 20):
            print(round(sorted_result[i], 0))

                

        print("\n-----------------------------------------------\n")


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

        # print("Task 10: Find all users who have registered transportation_mode and their most used transportation_mode ")
        # Insert code here

        # print("\n-----------------------------------------------\n")

        connection.close_connection()
    except Exception as e:
        print("ERROR: Failed to use database:", e)

if __name__ == '__main__':
    main()

# https://gitlab.stud.idi.ntnu.no/tdt4225lukrik/assignment3/-/tree/d37d03db4b643f018d234e0b1ad4210350b3e995
