IF EXISTS (SELECT name FROM sys.databases WHERE name = N'Sparta')
BEGIN
    ALTER DATABASE Sparta SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE Sparta;
END
GO

CREATE DATABASE Sparta;
GO

USE Sparta
-- Create tables

-- University
CREATE TABLE University (
    university_id INT IDENTITY(1,1) PRIMARY KEY,
    university_name VARCHAR(50)
);

-- Address
CREATE TABLE Address (
    address_id INT IDENTITY(1,1) PRIMARY KEY,
    postcode VARCHAR(10),
    street_name VARCHAR(50),
    city VARCHAR(50)
);

-- TalentMember
CREATE TABLE TalentMember (
    talent_member_id INT IDENTITY(1,1) PRIMARY KEY,
    first_name VARCHAR(50),
    last_name VARCHAR(50)
);

-- Candidate
CREATE TABLE Candidate (
    candidate_id INT IDENTITY(1,1) PRIMARY KEY,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    email VARCHAR(50),
    phone VARCHAR(20),
    dob DATE,
    gender VARCHAR(20),
    address_id INT,
    FOREIGN KEY (address_id) REFERENCES Address(address_id)
);

-- CandidateUniversity
CREATE TABLE CandidateUniversity (
    candidate_id INT,
    university_id INT,
    classification VARCHAR(10),
    PRIMARY KEY (candidate_id, university_id),
    FOREIGN KEY (candidate_id) REFERENCES Candidate(candidate_id),
    FOREIGN KEY (university_id) REFERENCES University(university_id)
);

-- Invitation
CREATE TABLE Invitation (
    invitation_id INT IDENTITY(1,1) PRIMARY KEY,
    talent_member_id INT,
    candidate_id INT,
    date DATE,
    FOREIGN KEY (talent_member_id) REFERENCES TalentMember(talent_member_id),
    FOREIGN KEY (candidate_id) REFERENCES Candidate(candidate_id)
);

-- SpartaDay
CREATE TABLE SpartaDay (
    candidate_id INT PRIMARY KEY,
    event_date DATE,
    academy VARCHAR(50),
    presentation_result INT,
    psychometric_result INT,
    FOREIGN KEY (candidate_id) REFERENCES Candidate(candidate_id)
);

-- Course
CREATE TABLE Course (
    course_id INT IDENTITY(1,1) PRIMARY KEY,
    course_name VARCHAR(50)
);

-- Trainer
CREATE TABLE Trainer (
    trainer_id INT IDENTITY(1,1) PRIMARY KEY,
    first_name VARCHAR(50),
    last_name VARCHAR(50)
);

-- Cohort
CREATE TABLE Cohort (
    cohort_id INT IDENTITY(1,1) PRIMARY KEY,
    trainer_id INT,
    course_id INT,
    start_date DATE,
    FOREIGN KEY (trainer_id) REFERENCES Trainer(trainer_id),
    FOREIGN KEY (course_id) REFERENCES Course(course_id)
);

-- Student
CREATE TABLE Student (
    candidate_id INT,
    cohort_id INT,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    PRIMARY KEY (candidate_id, cohort_id),
    FOREIGN KEY (candidate_id) REFERENCES Candidate(candidate_id),
    FOREIGN KEY (cohort_id) REFERENCES Cohort(cohort_id)
);

-- Week
CREATE TABLE Week (
    week_id INT IDENTITY(1,1) PRIMARY KEY,
    week_no INT
);

-- Skill
CREATE TABLE Skill (
    skill_id INT IDENTITY(1,1) PRIMARY KEY,
    skill_name VARCHAR(50)
);

-- Score
CREATE TABLE Score (
    skill_id INT,
    week_id INT,
    candidate_id INT,
    grade FLOAT,
    PRIMARY KEY (skill_id, week_id, candidate_id),
    FOREIGN KEY (skill_id) REFERENCES Skill(skill_id),
    FOREIGN KEY (week_id) REFERENCES Week(week_id),
    FOREIGN KEY (candidate_id) REFERENCES Candidate(candidate_id)
);

-- Interview
CREATE TABLE Interview (
    interview_id INT IDENTITY(1,1) PRIMARY KEY,
    candidate_id INT,
    interview_date DATE,
    self_development VARCHAR(3),
    financial_support VARCHAR(3),
    geo_flex VARCHAR(3),
    interview_result VARCHAR(4),
    course_interest VARCHAR(50),
    FOREIGN KEY (candidate_id) REFERENCES Candidate(candidate_id)
);

-- TechSkill
CREATE TABLE TechSkill (
    skill_id INT IDENTITY(1,1) PRIMARY KEY,
    skill_name VARCHAR(50)
);

-- CandidateTechSkill
CREATE TABLE CandidateTechSkill (
    interview_id INT,
    skill_id INT,
    score INT,
    PRIMARY KEY (interview_id, skill_id),
    FOREIGN KEY (interview_id) REFERENCES Interview(interview_id),
    FOREIGN KEY (skill_id) REFERENCES TechSkill(skill_id)
);

-- Weakness
CREATE TABLE Weakness (
    weakness_id INT IDENTITY(1,1) PRIMARY KEY,
    weakness_name VARCHAR(50)
);

-- CandidateWeakness
CREATE TABLE CandidateWeakness (
    interview_id INT,
    weakness_id INT,
    PRIMARY KEY (interview_id, weakness_id),
    FOREIGN KEY (interview_id) REFERENCES Interview(interview_id),
    FOREIGN KEY (weakness_id) REFERENCES Weakness(weakness_id)
);

-- Strength
CREATE TABLE Strength (
    strength_id INT IDENTITY(1,1) PRIMARY KEY,
    strength_name VARCHAR(50)
);

-- CandidateStrength
CREATE TABLE CandidateStrength (
    interview_id INT,
    strength_id INT,
    PRIMARY KEY (interview_id, strength_id),
    FOREIGN KEY (interview_id) REFERENCES Interview(interview_id),
    FOREIGN KEY (strength_id) REFERENCES Strength(strength_id)
);
