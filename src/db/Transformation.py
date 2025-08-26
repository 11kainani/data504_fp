import re
from collections import defaultdict

from src.cloud.Extractor import Extractor
from src.db.SQLConnector import SQLConnector
import time

class Transformer:
    def __init__(self):
        self.cloud_extractor = Extractor()
        self.sqlConnector = SQLConnector()

    @staticmethod
    def academy_key_transformer(key_file):
        """
        From the key of the file, separate it into course name, cohort  name and the start date.
        Academy/Data_30_2019-04-08.csv --> course_name : Data, cohort_number : 30, start_date : 2019-04-08
        :param key_file:
        :return: dictionary with the course name, cohort_number and the start date
        """
        filename = key_file.split('/')[-1]
        match = re.match(r'(.+)_(\d+)_(\d{4}-\d{2}-\d{2})\.csv', filename)
        course_name, course_number, start_date = match.groups()
        return {'course_name': course_name, 'cohort_number': course_number, 'start_date': start_date}

    def student_data_refactor(self, key_file) :
        """
        Refactor the academy dataframe into list of dict with the folowing data :
        {'name': '',
        'skills':
            {'Analytic': {1: 1, 2: -1.0, 3: -1.0, 4: -1.0, 5: -1.0, 6: -1.0, 7: -1.0, 8: -1.0},
            'Determined': {1: 2, 2: -1.0, 3: -1.0, 4: -1.0, 5: -1.0, 6: -1.0, 7: -1.0, 8: -1.0},
            'Imaginative': {1: 2, 2: -1.0, 3: -1.0, 4: -1.0, 5: -1.0, 6: -1.0, 7: -1.0, 8: -1.0},
            'Independent': {1: 2, 2: -1.0, 3: -1.0, 4: -1.0, 5: -1.0, 6: -1.0, 7: -1.0, 8: -1.0},
            'Professional': {1: 1, 2: -1.0, 3: -1.0, 4: -1.0, 5: -1.0, 6: -1.0, 7: -1.0, 8: -1.0},
            'Studious': {1: 2, 2: -1.0, 3: -1.0, 4: -1.0, 5: -1.0, 6: -1.0, 7: -1.0, 8: -1.0}}
        }
        for each skill, we have the grade for each week. If a grade is equal to -1, then student didn't attend for the week  (later changed to Null in the database)
        :param key_file:
        :return: trainer name , transformed student data lisd
        """
        pattern = r'([a-zA-Z]+)_W(\d+)'
        data = self.cloud_extractor.import_s3_csv_file(key_file)
        trainer = data['trainer'].unique()[0]
        data.fillna(-1, inplace=True)
        data = data.to_dict(orient='records')
        student_data = []
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
            student_data.append(new_student)
            print(student)

        return trainer, student_data

    def data_transform(self, key_file):
        session = self.sqlConnector.session()
        try:
            # Get the
            key_info = self.academy_key_transformer(key_file)
            trainer_info, data = self.student_data_refactor(key_file)

            course = self.sqlConnector.create_course(key_info['course_name'])
            trainer = self.sqlConnector.create_trainer(trainer_info)
            cohort = self.sqlConnector.create_cohort(course_name=course.name, cohort_id=key_info['cohort_number'],
                                                     start_date=key_info['start_date'], trainer_id=trainer.trainer_id)

            unique_skills = {s for entry in data for s in entry['skills']}
            unique_weeks = {w for entry in data for scores in entry['skills'].values() for w in scores}
            maximum_week = max(unique_weeks)

            skills_map = {s: self.sqlConnector.create_skill(skill_name=s) for s in unique_skills}
            self.sqlConnector.create_week(maximum_week)

            for entry in data:
                student = self.sqlConnector.create_student(cohort_id=cohort.cohort_id, name=entry['name'],
                                                           course_id=cohort.course_id)
                for skill_name, scores in entry['skills'].items():
                    skill = skills_map[skill_name]

                    for key, value in scores.items():
                        self.sqlConnector.create_score(skill_id=skill.skill_id, student_id=student.student_id,
                                                       week_id=key,
                                                       grade=value if value != -1 else None)
            session.commit()
        except Exception as e:
            session.rollback()
            print(e)
        finally:
            session.close()


    def academy_data_injection(self):
        """
        Automatically injects all the data from Academy sub-bucket into the database.
        Later add a cacheing mechanism to not inject file that are already present in the database by writing all the key into a file one injected into the database. Before, injecting, we check the same file can be found in the file. If found, it means that the file has already been injected so we skip, else we inject.
        :return: None
        """
        for key_file in self.cloud_extractor.academy_keys:
            self.data_transform(key_file)


if __name__ == '__main__':

    start = time.perf_counter()

    self = Transformer()
    self.academy_data_injection()

    end = time.perf_counter()
    print(f"Execution time: {end - start:.4f} seconds")
