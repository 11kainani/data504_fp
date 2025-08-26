import boto3
from botocore.exceptions import BotoCoreError, ClientError
import pandas as pd
import numpy as np
import io
from sqlalchemy import create_engine
from sqlalchemy import text



'''Kyle's Extractor'''

class S3Extractor:
    """
    Class for extracting data from the S3 bucket 'data-504-final-project-v2'.
    """

    def __init__(self):
        """
        Initializes S3 client, bucket name, and target folders.
        """
        self.bucket_name = 'data-504-final-project-v2'
        self.folders = ['Academy_Combined/', 'Talent_Combined/']
        self.s3_client = boto3.client('s3')

    def get_csvs_to_dfs(self):
        """
        Extracts CSV files from the specified folders and converts them into DataFrames.

        Returns:
            dict: Dictionary of DataFrames keyed by filename without extension.
        """
        dfs = {}
        for folder in self.folders:
            response = self.s3_client.list_objects_v2(Bucket=self.bucket_name, Prefix=folder)
            for obj in response.get("Contents", []):
                key = obj['Key']
                if key.endswith(".csv"):
                    csv_obj = self.s3_client.get_object(Bucket=self.bucket_name, Key=key)
                    df = pd.read_csv(io.StringIO(csv_obj['Body'].read().decode('utf-8')))
                    dfs[key.split("/")[-1].replace(".csv", "")] = df
        return dfs

'''Kyle's Cleaner'''

class S3Cleaner:
    
    """
    Class for cleaning DataFrames retrieved from S3Extractor
    """

    def clean_dfs(self, dfs):

        """
        Clean each DataFrame by applying cleaning transformations.

        Args:
            dfs (dict): A dictionary of a DataFrames to be cleaned.

        Returns:
            dfs (dict): A dictionary of cleaned DataFrames.
        
        Cleaning Order:
            - Global Standardisation
            - Global Dropping / Filling
            - Global Cleaning
            - Academy
            - Applicant Details
            - Sparta Day
            - Talent Decision
            
        Dropped or Ignored rows:
            - 'invited_day', 'month' if NULL
            - 'date_of_birth', errors=coerce
            - 'date' in talent_df, errors=coerce
        """
        
        # ======================= Global Standardisation =========================
        
        for df in dfs.values():

            # Standardise all columns
            df.columns = df.columns.str.strip().str.lower()
            
        # ======================== Global Dropping / Filling =====================
            
        # From the notebooks, were there any rows that were dropped or filled?
        # Mabye due to duplicates, missing values etc
        # If so add to subset list in the correct DataFrame
        
        # Applicants Data
        if 'combined_applicants_details' in dfs:
          
            df = dfs['combined_applicants_details']
            
            # Add more columns to list if required
            df.dropna(subset=['invited_date', 'month'], inplace=True)
        
        # Talent Data
        if 'combined_talent-decision_scores' in dfs:
            
            df = dfs['combined_talent-decision_scores']
            
            # Add more columns to list if required
            
        
        if 'combined_sparta_day_test_score' in dfs:
            
            df = dfs['combined_sparta_day_test_score']
            
            # Add more columns to list if required
            
            
        # ========================== Global Cleaning ==============================
        
        for name, df in dfs.items():

            # Candidate Name
            if 'name' in df.columns:
                df['name'] = df['name'].str.title()
                df[['candidate_first_name', 'candidate_last_name']] = df['name'].str.split(' ', n=1, expand=True)
                df.drop(columns=['name'], inplace=True)
                
        # ========================= Academy ==============================
            
        # Done globally - however columns only belong to academy data
        for name, df in dfs.items():
            
            # Trainer
            if 'trainer' in df.columns:
                df[['trainer_first_name', 'trainer_last_name']] = df['trainer'].str.split(' ', n=1, expand=True)
                df.drop(columns=['trainer'], inplace=True)
                
            # Cohort ID
            if 'cohort_number' in df.columns:
                df.rename(columns={'cohort_number': 'cohort_id'}, inplace=True)
            
            # Course
            if 'course' in df.columns:
                df.rename(columns={'course': 'course_name'}, inplace=True)
        
        # ========================= Applicant Details =======================
        
        # Specific DataFrame cleaning
        if 'combined_applicants_details' in dfs:
            
            df = dfs['combined_applicants_details']
            
            # Candidate Name (done globaly)

            # Candidate ID
            if 'id' in df.columns:
                df.rename(columns={'id': 'candidate_id'}, inplace=True)
            
            # Date of birth
            if 'dob' in df.columns:
                df['dob'] = pd.to_datetime(df['dob'], format="%d/%m/%Y", errors='coerce').dt.strftime("%Y-%m-%d")
                df.rename(columns={'dob': 'date_of_birth'}, inplace=True)

            # Street name
            if 'address' in df.columns:
                df.rename(columns={'address': 'street_name'}, inplace=True)
            
            # Phone number
            if 'phone_number' in df.columns:
                df['phone_number'] = df['phone_number'].str.replace(r'^=', '+', regex=True)
                df['phone_number'] = df['phone_number'].str.replace(r'[\(\)\s\-]', '', regex=True)
                df['phone_number'] = df['phone_number'].str.replace(
                    r'(\+44)(\d{3})(\d{3})(\d{4})', r'\1-\2-\3-\4', regex=True
                )

            # University
            if 'uni' in df.columns:
                df.rename(columns={'uni': 'university_name'}, inplace=True)

            # Degree
            if 'degree' in df.columns:
                df['degree'] = df['degree'].replace({"1st": "1:1", "3rd": "3:0"})
                df.rename(columns={'degree': 'classification'}, inplace=True)

            # Invitation date Interview date
            if 'invited_date' in df.columns and 'month' in df.columns:

                # Split month string into month and year
                month_parts = df['month'].str.strip().str.split(' ', expand=True)
                month_str = month_parts[0].str.title().replace("Sept", "September")
                # Convert month name to numeric month
                month = pd.to_datetime(month_str, format='%B').dt.month
                year = month_parts[1]
                day = df['invited_date']

                # Combine day, month, year into datetime
                df['invitation_date'] = pd.to_datetime({'year': year, 'month': month, 'day': day})
                
                df.drop(columns=['invited_date', 'month'], inplace=True)
                
            # Talent Member name
            if 'invited_by' in df.columns:
                df[['talent_member_first_name', 'talent_member_last_name']] = df['invited_by'].str.split(' ', n=1, expand=True)
                df.drop(columns=['invited_by'], inplace=True)
            
        # ============================= Sparta Day ======================================
        
        # Specific DataFrame cleaning
        if 'combined_sparta_day_test_score' in dfs:

            df = dfs['combined_sparta_day_test_score']
            
            # Date
            if 'date' in df.columns:
                df['date'] = pd.to_datetime(df['date'])
                df.rename(columns={'date': 'event_date'})
        
        # ============================== Talent Decision ================================
        
        # Specific DataFrame cleaning
        if 'combined_talent_decision_scores' in dfs:
            
            df = dfs['combined_talent_decision_scores']
            
            # Date
            if 'date' in df.columns:
                df['date'] = pd.to_datetime(df['date'], dayfirst=True, errors='coerce')
                df['date'] = df['date'].dt.strftime("%Y-%m-%d")
                df.rename(columns={'date': 'interview_date'}, inplace=True)            
            # Strengths
            if 'strengths' in df.columns:
                df['strengths'] = df['strengths'].str.strip("[]").str.replace("'", "").str.replace('"', "")
                
            # Weaknesses
            if 'weaknesses' in df.columns:
                df['weaknesses'] = df['weaknesses'].str.strip("[]").str.replace("'", "").str.replace('"', "")
                
            # Self Development
            if 'self_development' in df.columns:
                df['self_development'] = df['self_development'].map({'Yes': True, 'No': False})
                
            # Geo-Flex
            if 'geo_flex' in df.columns:
                df['geo_flex'] = df['geo_flex'].map({'Yes': True, 'No': False})
                
            # Result
            if 'result' in df.columns:
                df['result'] = df['result'].map({'Pass': True, 'Fail': False})
                df.rename(columns={'result': 'interview_result'}, inplace=True)

        return dfs
 

# Extract
extractor = S3Extractor()
dfs = extractor.get_csvs_to_dfs()

# Clean
cleaner = S3Cleaner()
clean_dfs = cleaner.clean_dfs(dfs)
    




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
        
        
        
        # --------------------------------------------------- Student -------------------------------------------------------
        
        
        
        
        # ============================================================= Junction Tables ======================================================
        
        # ------------------------------------------------- Candidate Tech Skill ---------------------------------------------
        
        
        
        # ------------------------------------------------- Interview -------------------------------------------------------
        
        
        
        # ----------------------------------------------------- Candidate Weakness ----------------------------------------------
        
        
        
        # ------------------------------------------------------ Candidate Strength --------------------------------------------
        
        
        
        # ------------------------------------------------------- Score -------------------------------------------------------
        
        
        
        # -------------------------------------------------------- Sparta Day --------------------------------------------------
        

        return tables
    

'''Luke's SQL Inserter'''

class SpartaDBInserter:
    def __init__(self, server="localhost", database="Sparta", username=None, password=None, driver="ODBC+Driver+17+for+SQL+Server"):
        """
        Initialize SQLAlchemy engine for SQL Server.
        """
        
        if username and password:
            conn_str = f"mssql+pyodbc://{username}:{password}@{server}/{database}?driver={driver}"
        else:
            # Trusted connection (Windows Auth)
            conn_str = f"mssql+pyodbc://@{server}/{database}?driver={driver}&trusted_connection=yes"
        
        self.engine = create_engine(conn_str, fast_executemany=True)

           
           # Test connection
        try:
            with self.engine.connect() as conn:
                conn.execute("SELECT 1")
            print(f"Connected to database '{database}' on server '{server}'")
        except Exception as e:
            print(f"Connection failed: {e}")

    def insert_dataframe(self, df: pd.DataFrame, table_name: str, if_exists="append"):
        """
        Insert letting SQL insert ids
        """
        df.to_sql(table_name, con=self.engine, if_exists=if_exists, index=False)
        print(f" Inserted {len(df)} rows into {table_name}")


    def insert_dataframe_with_id(self, df: pd.DataFrame, table_name: str):
        """
        Insert into SQL using our ids
        """
        df = df.where(pd.notnull(df), None)  # Replace NaN with None

        with self.engine.begin() as conn:  # begin a transaction
            # Enable explicit identity insert
            conn.execute(text(f"SET IDENTITY_INSERT {table_name} ON"))

            # Insert the DataFrame using the same connection
            df.to_sql(table_name, con=conn, if_exists="append", index=False, method='multi')

            # Disable explicit identity insert
            conn.execute(text(f"SET IDENTITY_INSERT {table_name} OFF"))

        print(f" Inserted {len(df)} rows into '{table_name}' with explicit IDs")
    

# Extract
extractor = S3Extractor()
dfs = extractor.get_csvs_to_dfs()

# Clean
cleaner = S3Cleaner()
clean_dfs = cleaner.clean_dfs(dfs)

# Transfrom
transform = S3Transformer()
transform_dfs = transform.transform_to_tables(clean_dfs)

# print(transform_dfs)

print(transform_dfs["skill"])


inserter = SpartaDBInserter(
    server="localhost\\SQLEXPRESS", 
    database="Sparta",
    username=None,
    password=None
)

inserter.insert_dataframe_with_id(transform_dfs['skill'], "Skill")