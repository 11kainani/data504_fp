from sqlalchemy import Column, Integer, String, ForeignKey, Date
from sqlalchemy.orm import relationship

from .base import Base

class Course(Base):
    __tablename__ = "course"

    courseID = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String, nullable=False)

    # One-to-many: a course can have many promotions
    promotions = relationship("Promotion", back_populates="cohort")

class Cohort(Base):
    __tablename__ = "cohort"

    cohortID = Column(Integer, primary_key=True, autoincrement=True)
    courseID = Column(Integer,  ForeignKey("course.courseID"), primary_key=True)
    start_date = Column(Date, nullable=False)

    # Link back to the course
    course = relationship("Course", back_populates="promotions")
