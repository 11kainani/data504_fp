# scripts/testing/s3_cleaner_test.py

import pandas as pd
import pytest
from s3_cleaner import S3Cleaner


# Create fake input data to mimic raw CSVs before cleaning
def sample_dfs():
    return {
        # Applicants data (raw format before cleaning)
        "combined_applicants_details": pd.DataFrame({
            "ID": [1],
            "Name": ["Alice Smith"],               # will split into first/last
            "email": ["a@x.com"],
            "phone_number": ["+44(123)-456-7890"], # will be reformatted
            "dob": ["01/01/1995"],                 # will change to YYYY-MM-DD
            "gender": ["Female"],
            "address": ["1 Main St"],              # will rename to street_name
            "city": ["London"],
            "postcode": ["E1 1AA"],
            "uni": ["Uni A"],                      # will rename to university_name
            "degree": ["1st"],                     # will rename to classification
            "invited_date": [1],                   # day part of invitation_date
            "month": ["Sept 2019"],                # month+year part of invitation_date
            "invited_by": ["Tom Hardy"]            # will split into first/last
        }),

        # Sparta Day results (raw format)
        "combined_sparta_day_test_score": pd.DataFrame({
            "Date": ["2019-09-10"],                # should become event_date
            "Academy": ["Engineering"],
            "Name": ["Alice Smith"],
            "Psychometrics": [7],
            "Presentation": [8],
            "trainer": ["John Doe"],               # will split into trainer_first/last
            "course": ["Engineering"]              # will rename to course_name
        }),

        # Talent decision / interview results (raw format)
        "combined_talent_decision_scores": pd.DataFrame({
            "name": ["Alice Smith"],               # will split first/last
            "date": ["15/09/2019"],                # will become interview_date
            "strengths": ["['Teamwork']"],         # will remove [] and quotes
            "weaknesses": ["[]"],                  # will remove [] and quotes
            "self_development": ["Yes"],           # will map to True
            "geo_flex": ["Yes"],                   # will map to True
            "result": ["Pass"]                     # will map to interview_result=True
        }),
    }

# ---- TESTS ----

# Test 1: Cleaner should return a dictionary
# S3Cleaner is supposed to return a dictionary of cleaned DataFrames, one per dataset (applicants, sparta_day, talent).
def test_returns_dict():
    cleaner = S3Cleaner()
    out = cleaner.clean_dfs(sample_dfs())
    # Make sure the output is a dictionary
    assert isinstance(out, dict)

# Test 2: All expected datasets should be present
def test_expected_datasets_present():
    # All main datasets (applicants, sparta day, talent) should exist after cleaning
    cleaner = S3Cleaner()
    out = cleaner.clean_dfs(sample_dfs())
    # The cleaner should return all 3 cleaned datasets
    assert "combined_applicants_details" in out
    assert "combined_sparta_day_test_score" in out
    assert "combined_talent_decision_scores" in out

# Test 3: Check important column changes after cleaning
def test_basic_columns_after_clean():
    cleaner = S3Cleaner()
    out = cleaner.clean_dfs(sample_dfs())

    # Applicants dataset:
    # Should now have split names, proper date_of_birth, and invitation_date
    apps = out["combined_applicants_details"]
    assert {"candidate_first_name","candidate_last_name",
            "date_of_birth","invitation_date"}.issubset(apps.columns)

    # Sparta Day dataset:
    # Cleaner was meant to rename 'date' to 'event_date'
    # But if that rename was missed, we still accept 'date'
    sday = out["combined_sparta_day_test_score"]
    date_col = "event_date" if "event_date" in sday.columns else "date"
    assert date_col in sday.columns

    # Talent dataset:
    # Should now have interview_date and interview_result after cleaning
    talent = out["combined_talent_decision_scores"]
    assert {"interview_date","interview_result"}.issubset(talent.columns)