from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from src.db.connexion import init_connexion
from src.db.base import Base
from src.db.models import Course, Week, Skill, Trainer, Cohort, Student, Score


class SQLConnector:
    """
    Object responsible for communicating with the database
    """
    def __init__(self):
        engine = create_engine(init_connexion())
        # create tables
        Base.metadata.create_all(bind=engine)
        # Begins sessions and make it possible to use transactions
        self.session = sessionmaker(bind=engine)


    def create_course(self,name):
        session = self.session()
        try:
            existing_course = session.query(Course).filter_by(name=name.lower()).first()
            if existing_course:
                print(f"Course {existing_course.name} already exists")
                return existing_course
            else:
                course = Course(name=name.lower())
                session.add(course)
                session.commit()
                print(f"Course '{name}' added with ID {course.course_id}")
                return course
        except Exception as e:
            session.rollback()
            print("Error adding course:", e)
        finally:
            session.close()

    def create_week(self, max_number_weeks : int):
        """
        We already know that we have around 10 week maximum so alleviate the coding process, we can hard code it then add a method after to add extra weeks.
        """
        session = self.session()
        try:
            # Create only if there is no data
            count = session.query(Week).count()
            print(f"There are {count} weeks")
            if count >= max_number_weeks:
                print(f'Data already exists. \n There are already more {count} weeks')

                return count
            else:
                # weekID 1 to 10
                weeks = [Week(weekID=i) for i in range(count+1, max_number_weeks + 1)]
                session.add_all(weeks)
                session.commit()
                print(f"Weeks {count+1} to '{max_number_weeks}' have been added ")
                return weeks
        except Exception as e:
            session.rollback()
            print("Error adding week:", e)
        finally:
            session.close()

    def create_skills(self, skill_list):
        session = self.session()
        try:

            # Get all existing skill names
            existing_skills = {s.skill_name for s in
                               session.query(Skill).filter(Skill.skill_name.in_(skill_list)).all()}

            unique_skill_names = [Skill(skill_name=i.lower()) for i in skill_list if i not in existing_skills]
            if unique_skill_names:
                session.add_all(unique_skill_names)
                session.commit()
                for skill in unique_skill_names:
                    print(f'{skill.skill_name} added with ID {skill.skill_id}')
            else:
                print("No new skills to add; all already exist.")
            return unique_skill_names
        except Exception as e:
            session.rollback()
            print("Error adding skill:", e)
        finally:
            session.close()

    def create_skill(self, skill_name):
        session = self.session()
        try:

            # Get all existing skill names
            existing_skill = session.query(Skill).filter_by(skill_name=skill_name.lower()).first()


            if not existing_skill:
                skill = Skill(skill_name=skill_name.lower())
                session.add(skill)
                session.commit()

                print(f'{skill.skill_name} added with ID {skill.skill_id}')
                return skill
            else:
                print("Skill already exist.")
            return existing_skill
        except Exception as e:
            session.rollback()
            print("Error adding skill:", e)
        finally:
            session.close()
    def create_trainer(self, name):
        session = self.session()
        try:

            existing_trainer = session.query(Trainer).filter(Trainer.name == name).first()
            if existing_trainer:
              print(f'Trainer {name} already exists')
              return existing_trainer
            else:
                trainer = Trainer(name=name)
                session.add(trainer)
                session.commit()
                print(f'Trainer {name} added with ID {trainer.trainer_id}')
                return trainer
        except Exception as e:
            session.rollback()
            print("Error adding trainer:", e)
        finally:
            session.close()

    def create_cohort(self, course_name, cohort_id, start_date, trainer_id):
        session = self.session()
        existing_trainer = session.query(Trainer).filter(Trainer.trainer_id == trainer_id).first()
        existing_course = session.query(Course).filter(Course.name == course_name.lower()).first()
        existing_cohort = session.query(Cohort).filter(Cohort.cohort_id == cohort_id).first()
        if not existing_course:
            print(f'Course {course_name} does not exist')
        elif not existing_trainer:
            print(f'Trainer {trainer_id} does not exist')
        elif existing_cohort:
            print(f'Cohort {cohort_id} already exists')
            return existing_cohort
        else:
            if cohort_id and start_date:
                cohort = Cohort(cohortID = cohort_id, courseID=existing_course.course_id, trainerID = trainer_id, start_date=start_date)
                session.add(cohort)
                session.commit()
                print(f'Cohort added with ID {cohort.cohort_id}')
                return cohort
            else:
                print(f'{cohort_id} and {start_date} are required')
        try:
            pass
        except Exception as e:
            session.rollback()
            print("Error adding cohort:", e)
        finally:
            session.close()

    def create_student(self, name, course_id, cohort_id):
        session = self.session()
        try:
            exisiting_course = session.query(Course).filter(Course.course_id == course_id).first()
            existing_cohort = session.query(Cohort).filter(Cohort.cohort_id == cohort_id).first()
            existing_student = session.query(Student).filter(Student.name == name).first()

            if existing_student:
                print(f'Student {name} already exists')
                return existing_student
            if not exisiting_course:
                print(f'Course {course_id} does not exist')
            if not existing_cohort:
                print(f'Cohort {cohort_id} does not exist')
            else:
                student = Student(name=name, courseID=exisiting_course.course_id, cohortID=existing_cohort.cohort_id)
                session.add(student)
                session.commit()
                print(f'Student added with ID {student.student_id}')
                return student
        except Exception as e:
            session.rollback()
            print("Error adding student:", e)
        finally:
            session.close()

    def create_score(self, student_id, skill_id, week_id, grade:int):
        session = self.session()
        try:
            exisiting_student = session.query(Student).filter(Student.student_id == student_id).first()
            existing_week = session.query(Week).filter(Week.week_id == week_id).first()
            exisiting_skill = session.query(Skill).filter(Skill.skill_id == skill_id).first()

            existing_score = session.query(Score).filter(
                Score.student_id == student_id,
                Score.week_id == week_id,
                Score.skill_id == skill_id
            ).first()

            if existing_score:
                print(f'Score for this {existing_score.student_id}, on week {existing_score.week_id} and for skill {existing_score.skill_id} already exists')
                return existing_score
            if not exisiting_student:
                print(f'Student {student_id} does not exist')
            if not existing_week:
                print(f'Week {week_id} does not exist')
            if not exisiting_skill:
                print(f'Skill {skill_id} does not exist')
            else:
                score = Score(skillID=exisiting_skill.skill_id, studentID=exisiting_student.student_id, weekID=existing_week.week_id, grade=grade)
                session.add(score)
                session.commit()
                print(f'Score added for {student_id}, week {week_id} with grade {grade} for skill {skill_id}')
        except Exception as e:
            session.rollback()
            print("Error adding Score:", e)
        finally:
            session.close()
if __name__ == "__main__":
    sql = SQLConnector()
    #sql.create_cohort_table('Data',3,'2023-03-22',1)
    #sql.create_student_table('Data',3,'3')
    #sql.create_score(1, 19, 2, 5)
