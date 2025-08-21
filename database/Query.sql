USE ACADEMY_DATABASE
GO

SELECT Trainer_Name, Student_Name, Grade, Skill_Name
FROM Trainer t
JOIN Cohort c
ON t.TrainerID = c.TrainerID
JOIN Student s
ON s.CourseID = c.CourseID
JOIN Score sc
ON sc.StudentID = s.StudentID
JOIN Skill sk
ON sk.SkillID = sc.SkillID