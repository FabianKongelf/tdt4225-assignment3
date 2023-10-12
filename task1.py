from DbConnector import DbConnector
from tabulate import tabulate
import os

class Task1():
    def __init__(self):
        self.connection = DbConnector()
        self.client = self.connection.client
        self.db = self.connection.db
    
    def create_coll(self, collection_name):
        collection = self.db.create_collection(collection_name)    
        print('Created collection: ', collection)
    
    def insert_documents(self, collection_name, docs):
        collection = self.db[collection_name]
        collection.insert_many(docs)


def main():
    program = None

    # init program
    try:
        program = Task1()

        # insert data
        try:
            current_location = os.path.dirname(__file__)
            dataset_location = os.path.join(current_location, "dataset", "dataset")
            user_data = os.path.join(dataset_location, "Data")
            user_list = os.listdir(user_data)
            user_docs = []

            # --------------------------------------------------
            # Insert Users
            # --------------------------------------------------

            # read labels
            label_file_path = os.path.join(dataset_location, "labeled_ids.txt")
            labels = None
            try:
                with open(label_file_path, "r") as file:
                    labels = file.read()
            except FileNotFoundError:
                print("ERROR cant find file")
            except Exception as e:
                print("ERROR cant read labels file: ", e)

            # reformate users
            for user_id in user_list:
                has_label = 1 if user_id in labels else 0
                user_docs.append({
                    '_id':          user_id, 
                    'has_label':    has_label
                })
            
            # insert users
            program.create_coll("users")
            program.insert_documents("users", user_docs)
            print("-- inserted users")

            # --------------------------------------------------
            # Insert Activites
            # --------------------------------------------------

            # create activity collection (table)
            # program.create_coll("activity")
            activity_id = 0

            # formate activites
            for user in user_docs:
                user_id = user["_id"]
                has_label = user["has_label"]
                user_data_path = os.path.join(user_data, user_id)
                
                # if user has labels read them
                labels = []
                if has_label == 1:
                    # inseter labels    
                    labels_path = os.path.join(user_data_path, "labels.txt")
                    with open(labels_path, "r") as file:
                        lines = file.readlines()[1:]
                        for line in lines:
                            array_line = line.strip().split("\t")
                            array_line.append(user_id)
                            labels.append(array_line)

                # find all activity files assosiaded with user
                files_path = os.path.join(user_data_path, "Trajectory")
                files = os.listdir(files_path)

                # create user activity array
                activity_docs = []

                # loop on all activity files for user
                for file_name in files:
                    file_path = os.path.join(files_path, file_name)

                    # read activity trackpoints
                    trackpoints = []
                    with open(file_path, "r") as file:
                        lines = file.readlines()[6:]
                        for line in lines:
                            converted_line = line.strip().split(",")
                            
                            # correct datatime 
                            datetime = converted_line[-2] + " " + converted_line[-1]
                            datetime = datetime.replace("-", "/")

                            # ajust altitute
                            if converted_line[3] == -777:
                                if len(trackpoints) > 0:
                                    altitude = trackpoints[-1][2]
                                else:
                                    altitude = 0 # might be an issue
                            else:
                                altitude = converted_line[3]

                            # convert lon lat to coord
                            coord = [converted_line[0], converted_line[1]]
                            
                            # date_days
                            date_days = converted_line[4]
                            
                            # add trackpoint
                            trackpoints.append({
                                "coordinate":   coord,
                                "altitude":     altitude,
                                "date_days":    date_days,
                                "datetime":     datetime
                            })
                    
                    # ignorre data if longer then 2500
                    if len(trackpoints) > 2500:
                        continue
            
                    activity_start_data = trackpoints[0]["datetime"]
                    activity_end_data = trackpoints[-1]["datetime"]
                    activity_transportation_mode = "none"

                    # if user has labels
                    if has_label == 1:
                        for row in labels:
                            if row[0] == activity_start_data and row[1] == activity_end_data:
                                activity_transportation_mode = row[2]

                    # create activity
                    activity_docs.append({
                        "_id":                  activity_id,
                        "user":                 user_id,
                        "start_date":           activity_start_data,
                        "end_date":             activity_end_data,
                        "transportation_mode":  activity_transportation_mode,
                        "trackpoints":          trackpoints
                    })

                    activity_id += 1

                # after loop all activities per user, insert activities
                if len(activity_docs) > 0:
                    program.insert_documents("activity", activity_docs)
                print("-- inserted activities for ", user_id)

        except Exception as e:
            print("ERROR: Failed inserting data: ", e)
        finally:
            print("-- insertes done")
    
    # failed init program
    except Exception as e:
        print("ERROR: Failed to use database:", e)
    
    # after try-catch block is finished
    finally:
        if program:
            program.connection.close_connection()


if __name__ == '__main__':
    main()