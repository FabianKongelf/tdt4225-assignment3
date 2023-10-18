from DbConnector import DbConnector
from tabulate import tabulate
from pprint import pprint
import numpy as np
import os


def main():
    # init program
    try:
        connection = DbConnector()
        client = connection.client
        db = connection.db

        print("TEST TEST TEST \n" )

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

        
        
    except Exception as e:
        print("ERROR: Failed to use database:", e)
    
    finally:
        connection.close_connection()

if __name__ == '__main__':
    main()