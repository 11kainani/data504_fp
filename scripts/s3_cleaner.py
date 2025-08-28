from s3_extractor import S3Extractor
import pandas as pd
import boto3
import io
from pandasgui import show

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
            - Academy
            - Applicant Details
            - Sparta Day
            - Talent Decision
            
        Missing Data:
            - NaN values in dates are left NaN
            - String data is filled "Unknown"
            
        """
        
        # ======================= Global Standardisation =========================
        
        for df in dfs.values():

            # Standardise all columns
            df.columns = df.columns.str.strip().str.lower()
            
        
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
                df['trainer'] = df['trainer'].replace('Ely Kely', 'Elly Kelly')
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
            
            # List of columns where missing values should become "Unknown"
            string_cols = ["month", "address", "email", "phone_number", "degree", "uni", "city", "postcode", "gender"]
            
            # Fill missing values
            for col in string_cols:
                # Change from Nan to Unkown
                df[col] = df[col].fillna("Unknown")
            
            # Repeated IDs
            df['id'].duplicated(keep=False)
            df['id'] = range(1, len(df) + 1)
            
            # Bruno Bellbrook - More records on Bellbrook
            df['invited_by'] = df['invited_by'].replace('Bruno Belbrook', 'Bruno Bellbrook')
            
            # Fifi Etton - Assumption based on the chance of two people with same name
            df['invited_by'] = df['invited_by'].replace('Fifi Eton', 'Fifi Etton')

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
                df['phone_number'] = (df['phone_number'].str.replace(r"[ \-\(\)]", "", regex=True).str.replace(r"^\+?44", "+44", regex=True))

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

                # Convert month name to numeric month, safely ignoring NaN
                month = pd.to_datetime(month_str, format='%B', errors='coerce').dt.month
                year = pd.to_numeric(month_parts[1], errors='coerce')
                day = pd.to_numeric(df['invited_date'], errors='coerce')

                df['invitation_date'] = pd.to_datetime({'year': year, 'month': month, 'day': day}, errors='coerce')
                df['invitation_date'] = df['invitation_date'].dt.date

                df.drop(columns=['invited_date', 'month'], inplace=True)
                
            # Talent Member name - Name split into two
            if 'invited_by' in df.columns:
                df[['talent_member_first_name', 'talent_member_last_name']] = df['invited_by'].str.split(' ', n=1, expand=True)
                df[['talent_member_first_name', 'talent_member_last_name']] = df[['talent_member_first_name', 'talent_member_last_name']].fillna("Unknown")
                df.drop(columns=['invited_by'], inplace=True)
            
        # ============================= Sparta Day ======================================
        
        # Specific DataFrame cleaning
        if 'combined_sparta_day_test_score' in dfs:

            df = dfs['combined_sparta_day_test_score']
            
            # Date
            if 'date' in df.columns:
                df['date'] = pd.to_datetime(df['date'])
                df['date'] = df['date'].dt.date
                df.rename(columns={'date': 'event_date'}, inplace=True)
                
            # Presentation
            if 'presentation' in df.columns:
                df.rename(columns={'presentation': 'presentation_result'}, inplace=True)

            # Physcometric
            if 'psychometrics' in df.columns:
                df.rename(columns={'psychometrics': 'psychometric_result'}, inplace=True)
        
        # ============================== Talent Decision ================================
        
        # Specific DataFrame cleaning
        if 'combined_talent_decision_scores' in dfs:
            
            df = dfs['combined_talent_decision_scores']
            
            # Date
            if 'date' in df.columns:
                df['date'] = df['date'].str.replace('//', '/', regex=False)
                df['date'] = pd.to_datetime(df['date'], dayfirst=True)
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
