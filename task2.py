from DbConnector import DbConnector
from tabulate import tabulate
from pprint import pprint 
from datetime import datetime
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

        # print("Task 1: How many users, activities and trackpoints are there in the dataset \n" )

        # result = db["users"].count_documents({})
        # print("Total amount of users: ", result)

        # result = db["activity"].count_documents({})
        # print("Total amount of activites: ", result)

        # result = db["activity"].aggregate([{
        #     "$group": {
        #         "_id": "null", 
        #         "tot_tp": {
        #             "$sum": {
        #                 "$size": "$trackpoints"
        #             }
        #         }
        #     }
        # }])
        # print("Total amount of trackpoints: ", end="")
        # for res in result:
        #     pprint(res["tot_tp"])

        # print("\n-----------------------------------------------\n")


        # ----------------------------------------
        # Task 2
        # ----------------------------------------

        # print("Task 2: Find avg amount of activites per user.\n")

        # ant_activites = db["activity"].count_documents({})
        # ant_users = db["users"].count_documents({})

        # print("Avg activites per user: ", round(ant_activites / ant_users, 1))

        # print("\n-----------------------------------------------\n")


        # ----------------------------------------
        # Task 3
        # ----------------------------------------

        # print("Task 3: Find the top 20 users with the highest number of activities \n")

        # pipeline = [
        #     { "$group": {
        #         "_id": "$user",
        #         "count": { "$sum": 1 }
        #         }
        #     },
        #     { "$sort": { "count": -1 } },
        # { "$limit": 20 }
        # ]

        # result = db.activity.aggregate(pipeline)

        # for document in result:
        #     print(document)

        # print("\n-----------------------------------------------\n")


        # ----------------------------------------
        # Task 4
        # ----------------------------------------

        # print("Task 4: Find all users who have taken a taxi \n")

        # Insert code here

        # print("\n-----------------------------------------------\n")


        # ----------------------------------------
        # Task 5
        # ----------------------------------------

        # print("Task 5: Find all types of transportation modes and count how many activities that are tagged with these transportation mode labels. Do not count the rows where the mode is null \n")

        # result = db["activity"].aggregate([{
        #         "$match": {
        #             "transportation_mode": {
        #                 "$ne": "none"
        #             }
        #         }
        #     }, {
        #         "$group": {
        #             "_id": "$transportation_mode",
        #             "tot": { "$sum": 1 }
        #         }
        #     }, {
        #         "$sort": {
        #             "tot": -1
        #         }
        #     }])
        # for res in result:
        #     pprint(res)

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

        print("Mode | activities")
        print(tabulate(result))

        print("\n-----------------------------------------------\n")

        

        # ----------------------------------------
        # Task 6
        # ----------------------------------------

        print("Task 6: a) Find the year with the most activities \n")

        # years of the dataset
        years = ["2007", "2008", "2009", "2010", "2011"]

        # list for storing activity counts of each year (index 0 = 2007, index 1 = 2008, ...)
        activity_counts = []

        # NB: I count an activity as belonging to a certain year if it has either startdate or enddate in that year
        # That means that if an activity starts in 2007 and ends in 2008, it will count as belonging to both 2007 and 2008
        for year in years:
            cursor = db["activity"].aggregate([
                {"$match": {
                    "$or": [
                        {"start_date": {"$regex": year}},
                        {"end_date": {"$regex": year}}
                    ]
                }},
                {"$count": f"activities_in_{year}"}
            ])
            
            # the aggregate function returns a cursor containing dictionaries
            # these dictionaries are parsed and the number of activities per year are added to the activity_counts list
            for doc in cursor:
                key = f"activities_in_{year}"
                activity_count = doc[key]
                activity_counts.append(activity_count) 

        most_activities = max(activity_counts)
        most_active_year = activity_counts.index(most_activities) + 2007

        print(f'{most_active_year} was the year with most activities, reaching {most_activities} activities')
        

        print("\n\nTask 6: b) Find the year with the most recorded hours \n")
        

        # Insert code here

        print("\n-----------------------------------------------\n")
        # Function used for calculating difference between two dates in the mongoDB format of our database
        def hourDifference(d1, d2):
                    date_format = '%Y/%m/%d %H:%M:%S'
                    datetime1 = datetime.strptime(d1, date_format)
                    datetime2 = datetime.strptime(d2, date_format)
                    difference = datetime2 - datetime1
                    hours_difference = difference.total_seconds() / 3600  # There are 3600 seconds in one hour.
                    return hours_difference


        # Example of the aforementioned function
        # d1 = '2008/11/12 09:14:00'
        # d2 = '2008/11/12 09:16:10'

        # print(hourDifference(d1, d2))

        # ----------------------------------------
        # Task 7
        # ----------------------------------------

        # print("Task 7: Find the total distance (in km) walked in 2008, by user with id=112 ")
        # Insert code here

        # print("\n-----------------------------------------------\n")


        # ----------------------------------------
        # Task 8
        # ----------------------------------------

        print("Task 8: Find the top 20 users who have gained the most altitude meters (This query takes a long time) \n")

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
        
        print("User | Altitude gain")
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

        print("Task 11: Find all users who have registered transportation_mode and their most used transportation_mode ")
        
        result = db["activity"].aggregate([
        { # all activites with a transportation mode not none
            "$match": {
                "transportation_mode": {
                    "$ne": "none"
                }
            }
        }, { # group on user and transportation mode
            "$group": {
                "_id": {
                    "user": "$user", 
                    "mode": "$transportation_mode"
                },
                "count": { "$sum": 1 }
            }
        }, { # sort count highest first
            "$sort": { "count": -1 }
        }, { # reformat to list only first entry (highest)
            "$group": {
                "_id": "$_id.user",
                "most_used_mode": { "$first": "$_id.mode" },
            }
        }, { # sort so lowest id is first, aestetic
            "$sort": { "_id": 1 }
        }])

        print("User | Mode")
        print(tabulate(result))

        print("\n-----------------------------------------------\n")

        connection.close_connection()
    except Exception as e:
        print("ERROR: Failed to use database:", e)

if __name__ == '__main__':
    main()

# https://gitlab.stud.idi.ntnu.no/tdt4225lukrik/assignment3/-/tree/d37d03db4b643f018d234e0b1ad4210350b3e995
