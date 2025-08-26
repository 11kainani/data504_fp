from sqlalchemy import Column, Integer, String, ForeignKey, Date, ForeignKeyConstraint,Boolean
from sqlalchemy.orm import relationship

from .base import Base

"""
Models class using the centralized base to validate the database tables schema
"""


class Course(Base):
    __tablename__ = "course"

    course_id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String, nullable=False)

    # One-to-many: a course can have many cohorts
    cohorts = relationship("Cohort", back_populates="course")


class Trainer(Base):
    __tablename__ = "trainer"
    trainer_id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String, nullable=False)

    # One trainer -> many cohorts
    cohorts = relationship("Cohort", back_populates="trainer")

class Cohort(Base):
    __tablename__ = "cohort"

    cohort_id = Column(Integer, primary_key=True)
    course_id = Column(Integer, ForeignKey("course.course_id"), primary_key=True)
    trainer_id = Column(Integer, ForeignKey("trainer.trainer_id"), nullable=False)
    start_date = Column(Date, nullable=False)

    # Relationships
    course = relationship("Course", back_populates="cohorts")
    trainer = relationship("Trainer", back_populates="cohorts")
    students = relationship("Student", back_populates="cohort")

class Student(Base):
    __tablename__ = "student"
    student_id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String, nullable=False)

    cohort_id = Column(Integer, nullable=False)
    course_id = Column(Integer, nullable=False)

    __table_args__ = (
        ForeignKeyConstraint(
            ["cohort_id", "course_id"],
            ["cohort.cohort_id", "cohort.course_id"]
        ),
    )
    # Relationships
    cohort = relationship("Cohort", back_populates="students")
    scores = relationship("Score", back_populates="student")

class Week(Base):
    __tablename__ = "week"
    week_id = Column(Integer, primary_key=True)

    scores = relationship("Score", back_populates="week")

class Skill(Base):
    __tablename__ = "skill"
    skill_id = Column(Integer, primary_key=True, autoincrement=True)
    skill_name = Column(String, nullable=False)

    # Relationships
    scores = relationship("Score", back_populates="skill")

class Score(Base):
    __tablename__ = "score"
    student_id = Column(Integer, ForeignKey("student.student_id"), nullable=False, primary_key=True)
    week_id = Column(Integer, ForeignKey("week.week_id"), nullable=False, primary_key=True)
    skill_id = Column(Integer, ForeignKey("skill.skill_id"), nullable=False, primary_key=True)
    grade = Column(Integer, nullable=True)

    # Relationships
    student = relationship("Student", back_populates="scores")
    week = relationship("Week", back_populates="scores")
    skill = relationship("Skill", back_populates="scores")

####### Sparta day

class SpartaDay(Base):
    __tablename__ = "spartaday"
    spartaday_id = Column(Integer, primary_key=True, autoincrement=True)
    academy_name = Column(String, nullable=False)

    candidates = relationship("Candidate", back_populates="spartaday")

class Address(Base):
    __tablename__ = "address"
    address_id = Column(Integer, primary_key=True, autoincrement=True)
    street_name = Column(String, nullable=False)
    postcode = Column(String, nullable=False)
    city = Column(String, nullable=False)

    candidates = relationship("Candidate", back_populates="address")

class Candidate(Base):
    __tablename__ = "candidate"
    candidate_id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String, nullable=False)
    gender = Column(String, nullable=False)
    dob = Column(Date, nullable=False)
    email = Column(String, nullable=False)
    phone = Column(String, nullable=False)

    spartaday_id = Column(Integer, ForeignKey("spartaday.spartaday_id"))
    spartaday = relationship('SpartaDay', back_populates='candidate')

    address_id = Column(Integer, ForeignKey("address.address_id"))
    address = relationship("Address", back_populates="spartaday")

    universities = relationship("CandidateUniversity", back_populates="candidate")

    invitation = relationship("Invitation", back_populates="candidate")
class CandidateStudent(Base):
    __tablename__ = "candidatestudent"
    candidate_id = Column(Integer, ForeignKey("candidate.candidate_id") ,primary_key=True)

class University(Base):
    __tablename__ = "university"

    university_id = Column(Integer, primary_key=True, autoincrement=True)
    university_name = Column(String, nullable=False)

    candidates = relationship("CandidateUniversity", back_populates="university")

class CandidateUniversity(Base):
    __tablename__ = "candidateuniversity"
    candidate_id = Column(Integer, ForeignKey("candidate.candidate_id")  , primary_key=True)
    classification = Column(String, nullable=False)
    university_id = Column(Integer, ForeignKey("university.university_id"))

    candidate = relationship("Candidate", back_populates="universities")
    university = relationship("University", back_populates="candidates")

class TalentMember(Base):
    __tablename__ = "talentmember"

    talentmember_id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String, nullable=False)

    invitation = relationship("Invitation", back_populates="talentmember")

class Invitation(Base):
    __tablename__ = "invitation"
    invitation_id = Column(Integer, primary_key=True, autoincrement=True)
    invitation_date = Column(Date, nullable=False)
    talentmember_id = Column(Integer, ForeignKey("talentmember.talentmember_id"))
    candidate_id = Column(Integer, ForeignKey("candidate.candidate_id"))

    candidate = relationship("Candidate", back_populates="invitations")
    talentmember = relationship("TalentMember", back_populates="invitations")

## Interview

class Interview(Base):
    __tablename__ = "interview"
    interview_id = Column(Integer, primary_key=True, autoincrement=True)
    interview_date = Column(Date, nullable=False)
    self_development = Column(Boolean, nullable=False)
    geo_flex = Column(Boolean, nullable=False)
    financial_support = Column(Boolean, nullable=False)
    interview_result = Column(Boolean, nullable=False)
    course_interest = Column(String, nullable=False)

    candidate_id = Column(Integer, ForeignKey("candidate.candidate_id"))
    candidate = relationship("Candidate", back_populates="interviews")

    interview_strengths = relationship("InterviewStrength", back_populates="interview")
    interview_weaknesses = relationship("InterviewWeakness", back_populates="interview")

    tech_skill_scores = relationship("TechSkillScores", back_populates="interview")

class Strength(Base):
    __tablename__ = "strength"
    strength_id = Column(Integer, primary_key=True, autoincrement=True)
    strength_name = Column(String, nullable=False)

    interviewStrength = relationship("InterviewStrength", back_populates="strength")

class Weakness(Base):
    __tablename__ = "weakness"
    weakness_id = Column(Integer, primary_key=True)
    weakness_name = Column(String, nullable=False)

    interview_weakness = relationship("InterviewWeakness", back_populates="weakness")

class InterviewStrength(Base):
    __tablename__ = "interviewstrength"
    interview_id = Column(Integer, ForeignKey("interview.interview_id"), primary_key=True)
    strength_id = Column(Integer, ForeignKey("strength.strength_id"), primary_key=True)

    interview = relationship("Interview", back_populates="interview_strengths")
    strength = relationship("Strength", back_populates="interview_strengths")

class InterviewWeakness(Base):
    __tablename__ = "interviewweakness"
    interview_id = Column(Integer, ForeignKey("interview.interview_id"), primary_key=True)
    weakness_id = Column(Integer, ForeignKey("weakness.weakness_id"), primary_key=True)

    interview = relationship("Interview", back_populates="interview_weaknesses")
    weakness = relationship("Weakness", back_populates="interview_weaknesses")

## Tests
class Test(Base):
    __tablename__ = "test"
    test_id = Column(Integer, primary_key=True, autoincrement=True)
    test_name = Column(String, nullable=False)

    test_results = relationship("TestResult", back_populates="test")
    candidates = relationship("Candidate", back_populates="test")

class TestResult(Base):
    __tablename__ = "testresult"
    test_id = Column(Integer, ForeignKey("test.test_id"), primary_key=True)
    test = relationship("Test", back_populates="test_results")


    candidate_id = Column(Integer, ForeignKey("candidate.candidate_id"), primary_key=True)
    candidate = relationship("Candidate", back_populates="test_results")

    score = Column(Integer, nullable=False)

class TechSkill(Base):
    __tablename__ = "techskill"
    tech_skill_id = Column(Integer, primary_key=True, autoincrement=True)
    tech_skill_name = Column(String, nullable=False)

    tech_skill_scores = relationship("TechSkillScores", back_populates="tech_skill")
class TechSkillScore(Base):
    __tablename__ = "techskillscore"
    tech_skill_id = Column(Integer, ForeignKey("techskill.tech_skill_id"), primary_key=True)
    techSkill = relationship("TechSkill", back_populates="tech_skill_score")

    interview_id = Column(Integer, ForeignKey("interview.interview_id"),  primary_key=True)
    interview = relationship("Interview", back_populates="tech_skill_score")

    score = Column(Integer, nullable=False)