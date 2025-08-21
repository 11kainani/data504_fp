import re
from collections import defaultdict

from cloud.Extracter import Extracter
from db.academy.SQLConnector import SQLConnector
import time


def skill_column_parser(text):
    pattern = r'([a-zA-Z]+)_W(\d+)'
    match = re.match(pattern, text)
    if match:
        letters = match.group(1)
        digits = match.group(2)
        print(letters)  # Something
        print(digits)


class Transformer:
    def __init__(self):
        self.cloud_extractor = Extracter()
        self.sqlConnector = SQLConnector()

    @staticmethod
    def academy_key_transformer(key_file):
        filename = key_file.split('/')[-1]
        match = re.match(r'(.+)_(\d+)_(\d{4}-\d{2}-\d{2})\.csv', filename)
        course_name, course_number, start_date = match.groups()
        return {'course_name': course_name, 'cohort_number': course_number, 'start_date': start_date}

    def dataframe_refactor(self, key_file):
        """
        Presumption : There is only one trainer in each file
        :param key_file:
        :return:
        """
        pattern = r'([a-zA-Z]+)_W(\d+)'
        data = self.cloud_extractor.import_s3_csv_file(key_file)
        trainer = data['trainer'].unique()[0]
        data.fillna(-1, inplace=True)
        data = data.to_dict(orient='records')
        transformed = []
        for student in data:

            new_student = {
                'name': student['name']
            }
            skill_scores = defaultdict(dict)
            for key, value in student.items():
                match = re.match(pattern, key)
                if match:
                    skill, week = match.groups()
                    skill_scores[skill][int(week)] = value
            new_student['skills'] = dict(skill_scores)
            transformed.append(new_student)

        return trainer, transformed

    def data_transform(self, key_file):
        key_info = transformer.academy_key_transformer(key_file)
        trainer_info, data = transformer.dataframe_refactor(key_file)

        print(key_info['start_date'])

        course = self.sqlConnector.create_course(key_info['course_name'])
        trainer = self.sqlConnector.create_trainer(trainer_info)
        cohort = self.sqlConnector.create_cohort(course_name=course.name, cohort_id=key_info['cohort_number'],
                                                    start_date=key_info['start_date'], trainer_id=trainer.trainerID)
        for entry in data:
            student = self.sqlConnector.create_student(cohort_id=cohort.cohortID, name=entry['name'],
                                                          course_id=cohort.courseID)
            for skill_name in entry['skills']:
                skill = self.sqlConnector.create_skill(skill_name=skill_name)
                skills_dict = entry['skills'][skill_name]
                days = self.sqlConnector.create_week(max(skills_dict.keys()))
                for key, value in skills_dict.items():

                    self.sqlConnector.create_score(skill_id=skill.skillID, student_id=student.studentID, week_id=key,
                                                      grade=value if value != -1 else None)

    def academy_data_injection(self):
        for key_file in self.cloud_extractor.academy_keys:
            self.data_transform(key_file)


if __name__ == '__main__':

    start = time.perf_counter()

    transformer = Transformer()
    transformer.academy_data_injection()

    end = time.perf_counter()
    print(f"Execution time: {end - start:.4f} seconds")
