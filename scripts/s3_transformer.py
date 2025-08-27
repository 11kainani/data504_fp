from s3_extractor import S3Extractor
from s3_cleaner import S3Cleaner
import pandas as pd


class S3Transformer:
    
    """
    Class for transforming DataFrames from S3Cleaner.
    """
    
    def transform_to_tables(self, dfs):
        
        """
        Transforms each DataFrame into a table formatted DataFrame.

        Args:
            dfs (dict): A dictionary of a DataFrames to be transformed.

        Returns:
            tables (dict): A dictionary of DataFrames(table formatted).
            
        Transformation Order:
            - Independant Tables
            - Dependant Tables
            
        """
        # Store transformed DataFrames
        tables = {}
        
        # Access to specific DataFrame
        applicants_df = dfs['combined_applicants_details']
        talent_df = dfs['combined_talent_decision_scores']
        sparta_day_df = dfs['combined_sparta_day_test_score']
        
        # =================================================== Independant Tables =======================================================
        
        # ---------------------------- Course -----------------------------------------------------------
        
        # Course ID start at 1 
        all_courses = sorted({c for df in dfs.values() if 'course_name' in df.columns for c in df['course_name'].dropna().unique()})
        course_df = pd.DataFrame({'course_name': all_courses})
        course_df['course_id'] = range(1, len(course_df)+1)
        tables['course'] = course_df
        
        # ------------------------------ Trainer --------------------------------------------------------
        
        # # Trainer ID starts at 1
        all_trainers = {(row['trainer_first_name'], row['trainer_last_name'])
        for df in dfs.values()
            if {'trainer_first_name', 'trainer_last_name'}.issubset(df.columns)
            for row in df[['trainer_first_name', 'trainer_last_name']].dropna().to_dict(orient='records')}
        trainers_df = pd.DataFrame(sorted(all_trainers), columns=['trainer_first_name', 'trainer_last_name'])
        trainers_df['trainer_id'] = range(1, len(trainers_df) + 1)
        tables['trainer'] = trainers_df
        
        # ------------------------------- Week -----------------------------------------------------------
        
        # Week ID is taken from Skill_W[0] in Academy_Combined
        all_weeks = sorted({int(col.split('_w')[1]) for df in dfs.values() for col in df.columns if '_w' in col})
        week_df = pd.DataFrame({'week_id': all_weeks})
        tables['week'] = week_df
       
        # --------------------------------- Skill ---------------------------------------------------
        
        # Skill ID starts at 1
        all_skills = sorted({c.split('_w')[0].title() for df in dfs.values() for c in df.columns if '_w' in c })
        skill_df = pd.DataFrame({'skill_name': all_skills})
        skill_df['skill_id'] = range(1, len(skill_df)+1)
        tables['skill'] = skill_df
        
        # --------------------------------- Weakness -----------------------------------------------
        
        # Weakness ID starts at 1
        all_weaknesses = sorted({w.strip() for df in dfs.values() if 'weaknesses' in df.columns for weaknesses in df['weaknesses'].dropna().unique() for w in weaknesses.split(',')})
        weakness_df = pd.DataFrame({'weakness_name': all_weaknesses})
        weakness_df['weakness_id'] = range(1, len(weakness_df)+1)
        tables['weakness'] = weakness_df
        
        # ---------------------------------- Strengths --------------------------------------------
        
        # Strengths ID starts at 1
        all_strengths = sorted({s.strip() for df in dfs.values() if 'strengths' in df.columns for strengths in df['strengths'].dropna().unique() for s in strengths.split(',')})
        strength_df = pd.DataFrame({'strength_name': all_strengths})
        strength_df['strength_id'] = range(1, len(strength_df) + 1)
        tables['strength'] = strength_df
        
        # ---------------------------------- University ------------------------------------------
        
        # University ID starts at 1
        all_universities = sorted({u.strip() for df in dfs.values() if 'university_name' in df.columns for universities in df['university_name'].dropna().unique() for u in universities.split(',')})
        university_df = pd.DataFrame({'university_name': all_universities})
        university_df['university_id'] = range(1, len(university_df) + 1)
        tables['university'] = university_df
        
        # ---------------------------------- Talent Member ----------------------------------------
        
        # Talent Member ID starts at 1
        all_talent_members = {(row['talent_member_first_name'], row['talent_member_last_name'])
        for df in dfs.values()
            if {'talent_member_first_name', 'talent_member_last_name'}.issubset(df.columns)
            for row in df[['talent_member_first_name', 'talent_member_last_name']].dropna().to_dict(orient='records')}
        trainers_df = pd.DataFrame(sorted(all_talent_members), columns=['talent_member_first_name', 'talent_member_last_name'])
        trainers_df['talent_member_id'] = range(1, len(trainers_df) + 1)
        tables['talent_member'] = trainers_df
        
        # --------------------------------- Tech Skill  -----------------------------------------
        
        # Tech Skill ID starts at 1
        all_tech_skills = sorted({t.split('tech_self_score.')[1].title() for df in dfs.values() for t in df.columns if 'tech_self_score.' in t})
        tech_skills_df = pd.DataFrame({'tech_skill_name': all_tech_skills})
        tech_skills_df['tech_skill_id'] = range(1, len(tech_skills_df) + 1)
        tables['tech_skill'] = tech_skills_df
        
        # -------------------------------- Address ----------------------------------------------
        
        # Address ID starts at 1
        all_addresses = sorted({(row['street_name'].strip(), row['city'].strip(), row['postcode'].strip()) 
            for df in dfs.values() if {'street_name', 'city', 'postcode'}.issubset(df.columns)for row in df[['street_name','city','postcode']].dropna().to_dict(orient='records')})
        address_df = pd.DataFrame(all_addresses, columns=['street_name','city','postcode'])
        address_df['address_id'] = range(1, len(address_df)+1)
        tables['address'] = address_df
        
        
        
        # ======================================================== Foreign Key Dependant Tables =================================================================
        
        # --------------------------------------------------- Candidate ---------------------------------------------------
        
        candidate_df = applicants_df[['candidate_id', 'candidate_first_name', 'candidate_last_name','email', 'phone_number', 'date_of_birth', 'gender', 'street_name', 'city', 'postcode']].dropna()
        candidate_df = candidate_df.merge(tables['address'], on=['street_name', 'city', 'postcode'], how='left')
        candidate_df = candidate_df[['candidate_id', 'candidate_first_name', 'candidate_last_name','email', 'phone_number', 'date_of_birth', 'gender', 'address_id']]
        tables['candidate'] = candidate_df
        
        # ---------------------------------------------------- Candidate University --------------------------------
        
        cand_uni_rows = applicants_df[['candidate_id', 'classification', 'university_name']].dropna()
        cand_uni_rows['university_name'] = cand_uni_rows['university_name'].astype(str).str.split(',')
        cand_uni_rows = cand_uni_rows.explode('university_name')
        cand_uni_df = cand_uni_rows.merge(university_df[['university_id', 'university_name']], on='university_name')
        tables['candidate_university'] = cand_uni_df
        
        # ------------------------------------------------------- Invitation ------------------------------------------
        
        # Mapping
        talent_member_lookup = {(row['talent_member_first_name'], row['talent_member_last_name']): row['talent_member_id']
            for _, row in tables['talent_member'].iterrows()}
        
        # Collect invitation rows from all dfs
        inv_rows = []
        for df in dfs.values():
            if {'candidate_id', 'talent_member_first_name', 'talent_member_last_name', 'invitation_date'}.issubset(df.columns):
                inv_rows.append(df[['candidate_id', 'talent_member_first_name', 'talent_member_last_name', 'invitation_date']].dropna())
                
        # Combine all invitation rows
        if inv_rows:
            invitation_df = pd.concat(inv_rows, ignore_index=True)
        else:
            invitation_df = pd.DataFrame(columns=['candidate_id', 'talent_member_first_name', 'talent_member_last_name', 'invitation_date'])

        # Map talent names to talent_member_id 
        invitation_df['talent_member_id'] = invitation_df.apply(
            lambda row: talent_member_lookup.get((row['talent_member_first_name'], row['talent_member_last_name'])), axis=1)

        # Assign primary key for invitations
        invitation_df['invitation_id'] = range(1, len(invitation_df) + 1)

        # Keep only relevant columns
        invitation_df = invitation_df[['invitation_id', 'candidate_id', 'talent_member_id', 'invitation_date']]
        tables['invitation'] = invitation_df
        
        # --------------------------------------------------- Cohort ------------------------------------------------------
        # Collect cohort information: course_name, trainer names, and start_date
        cohort_rows = []
        for df in dfs.values():
            if {'course_name', 'trainer_first_name', 'trainer_last_name', 'start_date'}.issubset(df.columns):
                cohort_rows.append(
                    df[['course_name', 'trainer_first_name', 'trainer_last_name', 'start_date']].dropna())

        if cohort_rows:
            cohort_df = pd.concat(cohort_rows, ignore_index=True)
        else:
            cohort_df = pd.DataFrame(columns=['course_name', 'trainer_first_name', 'trainer_last_name', 'start_date'])

        # Map course_id (FK)
        cohort_df = cohort_df.merge(course_df[['course_id', 'course_name']], on='course_name', how='left')

        # Map trainer_id (FK)
        cohort_df = cohort_df.merge(tables['trainer'][['trainer_id', 'trainer_first_name', 'trainer_last_name']],
                                    on=['trainer_first_name', 'trainer_last_name'], how='left')

        # Keep only relevant columns for Cohort table
        cohort_df = cohort_df[['trainer_id', 'course_id', 'start_date']].drop_duplicates().reset_index(drop=True)

        # Assign cohort_id as primary key AFTER selecting relevant columns
        cohort_df['cohort_id'] = range(1, len(cohort_df) + 1)

        # Reorder columns: cohort_id first
        cohort_df = cohort_df[['cohort_id', 'trainer_id', 'course_id', 'start_date']]

        tables['cohort'] = cohort_df
        # --------------------------------------------------- Student -------------------------------------------------------
        talent_df = dfs['combined_talent_decision_scores']

        # Filter only candidates that passed the interview through results
        passed = talent_df[talent_df['interview_result'] == True].copy()

        # Merge with candidate table for candidate id
        passed = passed.merge(
            tables['candidate'][['candidate_id', 'candidate_first_name', 'candidate_last_name']],
            on=['candidate_first_name', 'candidate_last_name'],
            how='inner'
        )

        # Map course interest to cohort id
        cohort_mapping = {
            tables['course'].loc[tables['course']['course_id'] == row['course_id'], 'course_name'].iloc[0]: row[
                'cohort_id']
            for _, row in tables['cohort'].iterrows()
        }
        passed['cohort_id'] = passed['course_interest'].map(cohort_mapping)

        # Handle missing cohort assigmnets
        if passed['cohort_id'].isna().any():
            course_cohorts = tables['cohort'].merge(tables['course'], on='course_id')
            for idx, row in passed[passed['cohort_id'].isna()].iterrows():
                cc = course_cohorts[course_cohorts['course_name'] == row['course_interest']]
                passed.at[idx, 'cohort_id'] = cc['cohort_id'].iloc[0] if not cc.empty else \
                tables['cohort']['cohort_id'].iloc[0]

        student_df = passed[
            ['candidate_id', 'cohort_id', 'candidate_first_name', 'candidate_last_name']].drop_duplicates(
            'candidate_id')
        student_df = student_df.rename(columns={
            'candidate_first_name': 'first_name',
            'candidate_last_name': 'last_name'
        })

        tables['student'] = student_df

        # ============================================================= Junction Tables ======================================================
        
        # ------------------------------------------------- Candidate Tech Skill ---------------------------------------------
        
        
        
        # ------------------------------------------------- Interview -------------------------------------------------------
        
        
        
        # ----------------------------------------------------- Candidate Weakness ----------------------------------------------
        
        
        
        # ------------------------------------------------------ Candidate Strength --------------------------------------------
        
        
        
        # ------------------------------------------------------- Score -------------------------------------------------------
        
        
        
        # -------------------------------------------------------- Sparta Day --------------------------------------------------
        

        return tables
    

# Extract
extractor = S3Extractor()
dfs = extractor.get_csvs_to_dfs()

# Clean
cleaner = S3Cleaner()
clean_dfs = cleaner.clean_dfs(dfs)

# Transfrom
transform = S3Transformer()
transform_dfs = transform.transform_to_tables(clean_dfs)

print(transform_dfs)