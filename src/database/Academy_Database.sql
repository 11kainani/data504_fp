-- Create database if it doesn't exist
IF NOT EXISTS (SELECT * FROM sys.databases WHERE name = 'ACADEMY_DATABASE')
BEGIN
    CREATE DATABASE ACADEMY_DATABASE;
END
GO

-- Switch to the database
USE ACADEMY_DATABASE;
GO

-- Create Course table
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'Course')
BEGIN
    CREATE TABLE Course(
       CourseID INT PRIMARY KEY,
       Course_Name VARCHAR(50)
    );
END

-- Create Trainer table
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'Trainer')
BEGIN
    CREATE TABLE Trainer(
       TrainerID INT PRIMARY KEY,
       Trainer_Name VARCHAR(50)
    );
END

-- Create Skill table
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'Skill')
BEGIN
    CREATE TABLE Skill(
       SkillID INT PRIMARY KEY,
       Skill_Name VARCHAR(50) NOT NULL
    );
END

-- Create Week table
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'Week')
BEGIN
    CREATE TABLE Week(
       WeekID INT PRIMARY KEY
    );
END

-- Create Cohort table
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'Cohort')
BEGIN
    CREATE TABLE Cohort(
       CourseID INT,
       CohortID INT,
       Start_Date DATE,
       TrainerID INT NOT NULL,
       PRIMARY KEY(CourseID, CohortID),
       FOREIGN KEY(CourseID) REFERENCES Course(CourseID),
       FOREIGN KEY(TrainerID) REFERENCES Trainer(TrainerID)
    );
END

-- Create Student table
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'Student')
BEGIN
    CREATE TABLE Student(
       StudentID INT PRIMARY KEY,
       Student_Name VARCHAR(50),
       CourseID INT NOT NULL,
       CohortID INT NOT NULL,
       FOREIGN KEY(CourseID, CohortID) REFERENCES Cohort(CourseID, CohortID)
    );
END

-- Create Score table
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'Score')
BEGIN
    CREATE TABLE Score(
       StudentID INT,
       SkillID INT,
       WeekID INT,
       Grade INT,
       PRIMARY KEY(StudentID, SkillID, WeekID),
       FOREIGN KEY(StudentID) REFERENCES Student(StudentID),
       FOREIGN KEY(SkillID) REFERENCES Skill(SkillID),
       FOREIGN KEY(WeekID) REFERENCES Week(WeekID)
    );
END


 

