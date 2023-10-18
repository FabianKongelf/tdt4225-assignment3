from DbConnector import DbConnector
from tabulate import tabulate
from pprint import pprint
from haversine import haversine, Unit
import numpy as np
import os


def main():
    # init program
    try:
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
            # these dictionaries are parsed and the number of activities are added to the activity_counts list
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


        # ----------------------------------------
        # Task 7
        # ----------------------------------------

        print("Task 7: Find the total distance (in km) walked in 2008, by user with id=112 ")

        user_112 = db['activity'].find({
            'user': '112', 
            'start_date': { '$gte': '2008/01/01' }, 
            'end_date': { '$lt': '2009/01/01' }, 
            'transportation_mode': 'walk'
        })

        sum = 0
        for document in user_112:
            trackpoints = document['trackpoints']
            for i in range(len(trackpoints) - 1):
                coord1 = map(float, trackpoints[i]['coordinate'])
                coord2 = map(float, trackpoints[i + 1]['coordinate'])
                distance = haversine(coord1, coord2, unit=Unit.KILOMETERS)
                sum += distance
        
        print('\nUser 112 has walked', round(sum, 2), 'km in 2008')

        print("\n-----------------------------------------------\n")


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
            print(sorted_result[i]["id"],"\t", str(round(sorted_result[i]["altitude"], 0)), "feet")

        print("\n-----------------------------------------------\n")


        # ----------------------------------------
        # Task 9
        # ----------------------------------------

        print("Task 9: Find all users who have invalid activities, and the number of invalid activities per user ")

        activities = db["activity"].find()
        users = db["users"].find()

        result = np.zeros(len(list(users)), dtype=np.int8)

        for activity in activities:
            error = False
            for i in range(1, len(activity["trackpoints"])):
                time1 = float(activity["trackpoints"][i-1]["date_days"])
                time2 = float(activity["trackpoints"][i]["date_days"])

                if abs(time1 - time2) > 0.003472222: # approximate five minutes
                    error = True
                    break

            if error:
                user = int(activity["user"])
                result[user] += 1
                continue

        print("User | Error")
        for i in range(0, len(result)):
            print(str(i), "\t", str(result[i]))

        print("\n-----------------------------------------------\n")


        # ----------------------------------------
        # Task 10
        # ----------------------------------------

        print("Task 10: Find the users who have tracked an activity in the Forbidden City of Beijing ")

        # Query the collection to find users in Beijing within the specified radius
        user_coord = db['activity'].find({}, {
            "_id": 0,
            "user": 1,
            "trackpoints.coordinate": 1
        })

        # Create a dictionary to store users who have tracked the Forbidden City
        users_in_beijing = []

        # Iterate through the query results
        hei = set()
        for user_data in user_coord:
            user = user_data.get("user")
            trackpoints = user_data.get("trackpoints", [])
            hei.add(user)
            # Check if any trackpoint is within the Forbidden City's vicinity
            for trackpoint in trackpoints:
                lat, lon = map(float, trackpoint["coordinate"])
                if abs(lat - 39.916) < 0.009 and abs(lon - 116.397) < 0.009: # approxemently 1 km radius
                    if user not in users_in_beijing:
                        users_in_beijing.append(user)
                        print('user',user, 'has been in forbidden city in Beijing')

        print("\n-----------------------------------------------\n")


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
        }, { # count users and modes
            "$group": {
                "_id": {
                    "user": "$user", 
                    "mode": "$transportation_mode"
                },
                "count": { "$sum": 1 }
            }
        }, { # sort on count, highest first
            "$sort": { 
                "count": -1,
                "_id.mode": 1
            }
        }, { # group by user
            "$group": {
                "_id": "$_id.user",
                "most_used_mode": { "$first": "$_id.mode" }, # pick first entry for user (from sorting above)
            }
        }, { # sort by id, lowest user id first
            "$sort": { "_id": 1 }
        }])

        print("User | Mode")
        print(tabulate(result))

        # ----------------------------------------
        # Tasks finished
        # ----------------------------------------

        connection.close_connection()
    except Exception as e:
        print("ERROR: Failed to use database:", e)

if __name__ == '__main__':
    main()

# https://gitlab.stud.idi.ntnu.no/tdt4225lukrik/assignment3/-/tree/d37d03db4b643f018d234e0b1ad4210350b3e995
