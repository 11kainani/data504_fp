import boto3
import pandas as pd

from Extracter import Extracter
from db.SQLConnector import SQLConnector

class SQLController():
    def __init__(self):
        self.cloud_extracter = Extracter()
        self.sql_connector = SQLConnector()

    def create_course_table(self):
        for course in self.cloud_extracter.courses_list:
            self.sql_connector.create_course(course)

    def create_week_table(self):
        self.sql_connector.create_week(max_number_weeks=10)
    def create_skill_table(self):
        skills = self.cloud_extracter.skills_list
        self.sql_connector.create_skills(skill_list=skills)

    def create_trainer_table(self,name):
        self.sql_connector.create_trainer(name)

def main():
    controller = SQLController()

    controller.create_skill_table()
    controller.create_week_table()
    #create_course_table(ext, sql_server)
    #




if __name__ == "__main__":
    main()