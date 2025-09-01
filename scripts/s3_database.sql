IF EXISTS (SELECT name FROM sys.databases WHERE name = N'SPARTA_DATABASE')
BEGIN
    ALTER DATABASE SPARTA_DATABASE SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE SPARTA_DATABASE;
END
GO

CREATE DATABASE SPARTA_DATABASE;
GO

USE SPARTA_DATABASE;
GO

CREATE TABLE university (
    university_id INT IDENTITY(1,1) PRIMARY KEY,
    university_name VARCHAR(200)
);
GO

CREATE TABLE address (
    address_id INT IDENTITY(1,1) PRIMARY KEY,
    postcode VARCHAR(20),
    street_name VARCHAR(100),
    city VARCHAR(100)  
);
GO

CREATE TABLE talent_member (
    talent_member_id INT IDENTITY(1,1) PRIMARY KEY,
    talent_member_first_name VARCHAR(50),
    talent_member_last_name VARCHAR(50)
);
GO

CREATE TABLE candidate (
    candidate_id INT IDENTITY(1,1) PRIMARY KEY,
    candidate_first_name VARCHAR(50),
    candidate_last_name VARCHAR(50),
    email VARCHAR(50),
    phone_number VARCHAR(20),
    date_of_birth DATE,
    gender VARCHAR(20),
    address_id INT,
    FOREIGN KEY (address_id) REFERENCES address(address_id)
);
GO

CREATE TABLE candidate_university (
    candidate_id INT,
    university_id INT,
    classification VARCHAR(10),
    PRIMARY KEY (candidate_id, university_id),
    FOREIGN KEY (candidate_id) REFERENCES candidate(candidate_id),
    FOREIGN KEY (university_id) REFERENCES university(university_id)
);
GO

CREATE TABLE invitation (
    invitation_id INT IDENTITY(1,1) PRIMARY KEY,
    talent_member_id INT,
    candidate_id INT,
    invitation_date DATE,
    FOREIGN KEY (talent_member_id) REFERENCES talent_member(talent_member_id),
    FOREIGN KEY (candidate_id) REFERENCES candidate(candidate_id)
);
GO

CREATE TABLE sparta_day (
    candidate_id INT PRIMARY KEY,
    event_date DATE,
    academy VARCHAR(50),
    presentation_result INT,
    psychometric_result INT,
    FOREIGN KEY (candidate_id) REFERENCES candidate(candidate_id)
);
GO

CREATE TABLE course (
    course_id INT IDENTITY(1,1) PRIMARY KEY,
    course_name VARCHAR(50)
);
GO

CREATE TABLE trainer (
    trainer_id INT IDENTITY(1,1) PRIMARY KEY,
    trainer_first_name VARCHAR(50),
    trainer_last_name VARCHAR(50)
);
GO

CREATE TABLE cohort (
    cohort_id INT IDENTITY(1,1) PRIMARY KEY,
    trainer_id INT,
    course_id INT,
    start_date DATE,
    FOREIGN KEY (trainer_id) REFERENCES trainer(trainer_id),
    FOREIGN KEY (course_id) REFERENCES course(course_id)
);
GO

CREATE TABLE student (
    candidate_id INT,
    cohort_id INT,
    candidate_first_name VARCHAR(50),
    candidate_last_name VARCHAR(50),
    PRIMARY KEY (candidate_id, cohort_id),
    FOREIGN KEY (candidate_id) REFERENCES candidate(candidate_id),
    FOREIGN KEY (cohort_id) REFERENCES cohort(cohort_id)
);
GO

CREATE TABLE week (
    week_id INT IDENTITY(1,1) PRIMARY KEY
);
GO

SET IDENTITY_INSERT week ON;

INSERT INTO week (week_id) VALUES
(1),(2),(3),(4),(5),(6),(7),(8),(9),(10);

SET IDENTITY_INSERT week OFF;
GO

CREATE TABLE skill (
    skill_id INT IDENTITY(1,1) PRIMARY KEY,
    skill_name VARCHAR(50)
);
GO

CREATE TABLE score (
    skill_id INT,
    week_id INT,
    candidate_id INT,
    grade FLOAT,
    PRIMARY KEY (skill_id, week_id, candidate_id),
    FOREIGN KEY (skill_id) REFERENCES skill(skill_id),
    FOREIGN KEY (week_id) REFERENCES week(week_id),
    FOREIGN KEY (candidate_id) REFERENCES candidate(candidate_id)
);
GO

CREATE TABLE interview (
    interview_id INT IDENTITY(1,1) PRIMARY KEY,
    candidate_id INT,
    interview_date DATE,
    self_development VARCHAR(3),
    financial_support VARCHAR(3),
    geo_flex VARCHAR(3),
    interview_result VARCHAR(4),
    course_interest VARCHAR(50),
    FOREIGN KEY (candidate_id) REFERENCES candidate(candidate_id)
);
GO

CREATE TABLE tech_skill (
    tech_skill_id INT IDENTITY(1,1) PRIMARY KEY,
    tech_skill_name VARCHAR(50)
);
GO

CREATE TABLE candidate_tech_skill (
    interview_id INT,
    tech_skill_id INT,
    tech_self_score INT,
    PRIMARY KEY (interview_id, tech_skill_id),
    FOREIGN KEY (interview_id) REFERENCES interview(interview_id),
    FOREIGN KEY (tech_skill_id) REFERENCES tech_skill(tech_skill_id)
);
GO

CREATE TABLE weakness (
    weakness_id INT IDENTITY(1,1) PRIMARY KEY,
    weakness_name VARCHAR(50)
);
GO

CREATE TABLE candidate_weakness (
    interview_id INT,
    weakness_id INT,
    PRIMARY KEY (interview_id, weakness_id),
    FOREIGN KEY (interview_id) REFERENCES interview(interview_id),
    FOREIGN KEY (weakness_id) REFERENCES weakness(weakness_id)
);
GO

CREATE TABLE strength (
    strength_id INT IDENTITY(1,1) PRIMARY KEY,
    strength_name VARCHAR(50)
);
GO

CREATE TABLE candidate_strength (
    interview_id INT,
    strength_id INT,
    PRIMARY KEY (interview_id, strength_id),
    FOREIGN KEY (interview_id) REFERENCES interview(interview_id),
    FOREIGN KEY (strength_id) REFERENCES strength(strength_id)
);
GO
