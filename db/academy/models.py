from sqlalchemy import Column, Integer, String, ForeignKey, Date, ForeignKeyConstraint, Float
from sqlalchemy.orm import relationship

from .base import Base

class Course(Base):
    __tablename__ = "course"

    courseID = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String, nullable=False)

    # One-to-many: a course can have many cohorts
    cohorts = relationship("Cohort", back_populates="course")


class Trainer(Base):
    __tablename__ = "trainer"
    trainerID = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String, nullable=False)

    # One trainer -> many cohorts
    cohorts = relationship("Cohort", back_populates="trainer")

class Cohort(Base):
    __tablename__ = "cohort"

    cohortID = Column(Integer, primary_key=True)
    courseID = Column(Integer,  ForeignKey("course.courseID"), primary_key=True)
    trainerID = Column(Integer, ForeignKey("trainer.trainerID"), nullable=False)
    start_date = Column(Date, nullable=False)

    # Link back to the course
    course = relationship("Course", back_populates="cohorts")
    trainer = relationship("Trainer", back_populates="cohorts")
    students = relationship("Student", back_populates="cohort")

class Student(Base):
    __tablename__ = "student"
    studentID = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String, nullable=False)

    cohortID = Column(Integer, nullable=False)
    courseID = Column(Integer, nullable=False)

    __table_args__ = (
        ForeignKeyConstraint(
            ["cohortID", "courseID"],
            ["cohort.cohortID", "cohort.courseID"]
        ),
    )
    cohort = relationship("Cohort", back_populates="students")
    scores = relationship("Score", back_populates="student")

class Week(Base):
    __tablename__ = "week"
    weekID = Column(Integer, primary_key=True)

    scores = relationship("Score", back_populates="week")

class Skill(Base):
    __tablename__ = "skill"
    skillID = Column(Integer, primary_key=True, autoincrement=True)
    skill_name = Column(String, nullable=False)

    scores = relationship("Score", back_populates="skill")

class Score(Base):
    __tablename__ = "score"
    studentID = Column(Integer, ForeignKey("student.studentID"), nullable=False, primary_key=True)
    weekID = Column(Integer, ForeignKey("week.weekID"), nullable=False, primary_key=True)
    skillID = Column(Integer, ForeignKey("skill.skillID"), nullable=False, primary_key=True)
    grade = Column(Integer, nullable=True)

    student = relationship("Student", back_populates="scores")
    week = relationship("Week", back_populates="scores")
    skill = relationship("Skill", back_populates="scores")