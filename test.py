from DbConnector import DbConnector
from tabulate import tabulate
from pprint import pprint
import numpy as np
import os


def main():
    # init program
    connection = DbConnector()
    client = connection.client
    db = connection.db

    print("TEST TEST TEST \n\n\n" )

    print("Task 9: Find all users who have invalid activities, and the number of invalid activities per user ")

    activities = db["activity"].find()
    users = db["users"].find()
    id_list = list(users)
    user_list = [user['_id'] for user in id_list]

    result = np.zeros(len(user_list), dtype=np.int16)

    for activity in activities:
        error = False
        for i in range(1, len(activity["trackpoints"])):
            time1 = float(activity["trackpoints"][i-1]["date_days"])
            time2 = float(activity["trackpoints"][i]["date_days"])

            if abs(time1 - time2) > 0.003472222:  # approximate five minutes
                error = True
                break

        if error:
            user = int(activity["user"])
            result[user] += 1
            continue

    table_data = [[user_list[i], result[i]] for i in range(len(user_list))]
    print(tabulate(table_data, headers=[
            "User", "Invalid activities"], tablefmt="grid"))



    
    connection.close_connection()

if __name__ == '__main__':
    main()