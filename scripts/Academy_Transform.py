from Academy_DataFrames import S3DataFrames
import pandas as pd


handler = S3DataFrames()
dfs = handler.get_csvs()

def transform_dfs_for_db(dfs):
    
    # Step 0: rename columns consistently
    for df in dfs.values():
        if 'name' in df.columns:
            df.rename(columns={'name': 'Student_Name'}, inplace=True)
        if 'trainer' in df.columns:
            df.rename(columns={'trainer': 'Trainer_Name'}, inplace=True)
        if 'course' in df.columns:
            df.rename(columns={'course': 'Course_Name'}, inplace=True)
        if 'cohort_number' in df.columns:
            df.rename(columns={'cohort_number': 'CohortID'}, inplace=True)
    
    tables = {}

    # 1. Courses
    all_courses = sorted({c for df in dfs.values() if 'Course_Name' in df.columns for c in df['Course_Name'].dropna().unique()})
    course_df = pd.DataFrame({'Course_Name': all_courses})
    course_df['CourseID'] = range(1, len(course_df)+1)
    tables['Course'] = course_df
    course_name_to_id = dict(zip(course_df['Course_Name'], course_df['CourseID']))

    # 2. Trainers
    all_trainers = sorted({t for df in dfs.values() if 'Trainer_Name' in df.columns for t in df['Trainer_Name'].dropna().unique()})
    trainer_df = pd.DataFrame({'Trainer_Name': all_trainers})
    trainer_df['TrainerID'] = range(1, len(trainer_df)+1)
    tables['Trainer'] = trainer_df
    trainer_name_to_id = dict(zip(trainer_df['Trainer_Name'], trainer_df['TrainerID']))

    # 3. Skills
    all_skills = sorted({c.split('_W')[0] for df in dfs.values() for c in df.columns if '_W' in c})
    skill_df = pd.DataFrame({'Skill_Name': all_skills})
    skill_df['SkillID'] = range(1, len(skill_df)+1)
    tables['Skill'] = skill_df
    skill_name_to_id = dict(zip(skill_df['Skill_Name'], skill_df['SkillID']))

    # 4. Weeks
    all_weeks = sorted({int(c.split('_W')[1]) for df in dfs.values() for c in df.columns if '_W' in c})
    week_df = pd.DataFrame({'WeekID': all_weeks})
    tables['Week'] = week_df

    # 5. Cohorts
    cohort_rows = []
    for df in dfs.values():
        if {'Course_Name', 'CohortID', 'Trainer_Name'}.issubset(df.columns):
            cohort_rows.append(df[['Course_Name', 'CohortID', 'Trainer_Name']])
    if cohort_rows:
        cohort_df = pd.concat(cohort_rows).drop_duplicates()
        cohort_df['CourseID'] = cohort_df['Course_Name'].map(course_name_to_id)
        cohort_df['TrainerID'] = cohort_df['Trainer_Name'].map(trainer_name_to_id)
        cohort_df = cohort_df[['CourseID','CohortID','TrainerID']]
        tables['Cohort'] = cohort_df
    else:
        tables['Cohort'] = pd.DataFrame(columns=['CourseID','CohortID','TrainerID'])

    # 6. Students
    student_rows = []
    for df in dfs.values():
        if {'Student_Name', 'Course_Name', 'CohortID'}.issubset(df.columns):
            student_rows.append(df[['Student_Name','Course_Name','CohortID']])
    if student_rows:
        student_df = pd.concat(student_rows).drop_duplicates()
        student_df = student_df.sort_values(by=['Student_Name','Course_Name','CohortID']).reset_index(drop=True)
        student_df['StudentID'] = range(1, len(student_df)+1)
        student_df['CourseID'] = student_df['Course_Name'].map(course_name_to_id)
        student_df = student_df[['StudentID','Student_Name','CourseID','CohortID']]
        tables['Student'] = student_df
    else:
        tables['Student'] = pd.DataFrame(columns=['StudentID','Student_Name','CourseID','CohortID'])
    student_name_to_id = dict(zip(tables['Student']['Student_Name'], tables['Student']['StudentID']))

    # 7. Scores
    score_rows = []
    for df in dfs.values():
        skill_cols = [c for c in df.columns if '_W' in c]
        for col in skill_cols:
            skill, week = col.split('_W')
            temp_df = df[['Student_Name', col]].copy()
            temp_df = temp_df.dropna(subset=[col])
            temp_df['SkillID'] = skill_name_to_id[skill]
            temp_df['WeekID'] = int(week)
            temp_df['Grade'] = temp_df[col]
            temp_df['StudentID'] = temp_df['Student_Name'].map(student_name_to_id)
            score_rows.append(temp_df[['StudentID','SkillID','WeekID','Grade']])
    if score_rows:
        score_df = pd.concat(score_rows).reset_index(drop=True)
        tables['Score'] = score_df
    else:
        tables['Score'] = pd.DataFrame(columns=['StudentID','SkillID','WeekID','Grade'])

    return tables

transform_dfs_for_db(dfs)
print(dfs)

