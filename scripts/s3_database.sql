-- Create database if it doesn't exist
IF NOT EXISTS (SELECT name FROM sys.databases WHERE name = N'sparta_db')
BEGIN
    CREATE DATABASE sparta_db;
END
GO

USE sparta_db;
GO

-- University
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'university')
BEGIN
    CREATE TABLE university (
        university_id INT IDENTITY(1,1) PRIMARY KEY,
        university_name VARCHAR(50)
    );
END

-- Address
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'address')
BEGIN
    CREATE TABLE address (
        address_id INT IDENTITY(1,1) PRIMARY KEY,
        postcode VARCHAR(10),
        street_name VARCHAR(50),
        city VARCHAR(50)
    );
END

-- Talent Member
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'talent_member')
BEGIN
    CREATE TABLE talent_member (
        talent_member_id INT IDENTITY(1,1) PRIMARY KEY,
        first_name VARCHAR(50),
        last_name VARCHAR(50)
    );
END

-- Candidate
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'candidate')
BEGIN
    CREATE TABLE candidate (
        candidate_id INT IDENTITY(1,1) PRIMARY KEY,
        first_name VARCHAR(50),
        last_name VARCHAR(50),
        email VARCHAR(50),
        phone VARCHAR(20),
        dob DATE,
        gender VARCHAR(20),
        address_id INT,
        FOREIGN KEY (address_id) REFERENCES address(address_id)
    );
END

-- Candidate University
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'candidate_university')
BEGIN
    CREATE TABLE candidate_university (
        candidate_id INT,
        university_id INT,
        classification VARCHAR(10),
        PRIMARY KEY (candidate_id, university_id),
        FOREIGN KEY (candidate_id) REFERENCES candidate(candidate_id),
        FOREIGN KEY (university_id) REFERENCES university(university_id)
    );
END

-- Invitation
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'invitation')
BEGIN
    CREATE TABLE invitation (
        invitation_id INT IDENTITY(1,1) PRIMARY KEY,
        talent_member_id INT,
        candidate_id INT,
        date DATE,
        FOREIGN KEY (talent_member_id) REFERENCES talent_member(talent_member_id),
        FOREIGN KEY (candidate_id) REFERENCES candidate(candidate_id)
    );
END

-- Sparta Day
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'sparta_day')
BEGIN
    CREATE TABLE sparta_day (
        candidate_id INT PRIMARY KEY,
        event_date DATE,
        academy VARCHAR(50),
        presentation_result INT,
        psychometric_result INT,
        FOREIGN KEY (candidate_id) REFERENCES candidate(candidate_id)
    );
END

-- Course
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'course')
BEGIN
    CREATE TABLE course (
        course_id INT IDENTITY(1,1) PRIMARY KEY,
        course_name VARCHAR(50)
    );
END

-- Trainer
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'trainer')
BEGIN
    CREATE TABLE trainer (
        trainer_id INT IDENTITY(1,1) PRIMARY KEY,
        first_name VARCHAR(50),
        last_name VARCHAR(50)
    );
END

-- Cohort
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'cohort')
BEGIN
    CREATE TABLE cohort (
        cohort_id INT IDENTITY(1,1) PRIMARY KEY,
        trainer_id INT,
        course_id INT,
        start_date DATE,
        FOREIGN KEY (trainer_id) REFERENCES trainer(trainer_id),
        FOREIGN KEY (course_id) REFERENCES course(course_id)
    );
END

-- Student
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'student')
BEGIN
    CREATE TABLE student (
        candidate_id INT,
        cohort_id INT,
        first_name VARCHAR(50),
        last_name VARCHAR(50),
        PRIMARY KEY (candidate_id, cohort_id),
        FOREIGN KEY (candidate_id) REFERENCES candidate(candidate_id),
        FOREIGN KEY (cohort_id) REFERENCES cohort(cohort_id)
    );
END

-- Week
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'week')
BEGIN
    CREATE TABLE week (
        week_id INT IDENTITY(1,1) PRIMARY KEY,
        week_no INT
    );
END

-- Skill
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'skill')
BEGIN
    CREATE TABLE skill (
        skill_id INT IDENTITY(1,1) PRIMARY KEY,
        skill_name VARCHAR(50)
    );
END

-- Score
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'score')
BEGIN
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
END

-- Interview
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'interview')
BEGIN
    CREATE TABLE interview (
        interview_id INT IDENTITY(1,1) PRIMARY KEY,
        candidate_id INT,
        interview_date DATE,
        self_development VARCHAR(3),
        financial_support VARCHAR(3),
        geo_flex VARCHAR(3),
        result VARCHAR(4),
        course_interest VARCHAR(50),
        FOREIGN KEY (candidate_id) REFERENCES candidate(candidate_id)
    );
END

-- Tech Skill
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'tech_skill')
BEGIN
    CREATE TABLE tech_skill (
        skill_id INT IDENTITY(1,1) PRIMARY KEY,
        skill_name VARCHAR(50)
    );
END

-- Candidate Tech Skill
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'candidate_tech_skill')
BEGIN
    CREATE TABLE candidate_tech_skill (
        interview_id INT,
        skill_id INT,
        score INT,
        PRIMARY KEY (interview_id, skill_id),
        FOREIGN KEY (interview_id) REFERENCES interview(interview_id),
        FOREIGN KEY (skill_id) REFERENCES tech_skill(skill_id)
    );
END

-- Weakness
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'weakness')
BEGIN
    CREATE TABLE weakness (
        weakness_id INT IDENTITY(1,1) PRIMARY KEY,
        weakness_name VARCHAR(50)
    );
END

-- Candidate Weakness
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'candidate_weakness')
BEGIN
    CREATE TABLE candidate_weakness (
        interview_id INT,
        weakness_id INT,
        PRIMARY KEY (interview_id, weakness_id),
        FOREIGN KEY (interview_id) REFERENCES interview(interview_id),
        FOREIGN KEY (weakness_id) REFERENCES weakness(weakness_id)
    );
END

-- Strength
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'strength')
BEGIN
    CREATE TABLE strength (
        strength_id INT IDENTITY(1,1) PRIMARY KEY,
        strength_name VARCHAR(50)
    );
END

-- Candidate Strength
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'candidate_strength')
BEGIN
    CREATE TABLE candidate_strength (
        interview_id INT,
        strength_id INT,
        PRIMARY KEY (interview_id, strength_id),
        FOREIGN KEY (interview_id) REFERENCES interview(interview_id),
        FOREIGN KEY (strength_id) REFERENCES strength(strength_id)
    );
END