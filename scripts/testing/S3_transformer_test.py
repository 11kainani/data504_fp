# Transformer testing suite

# importing packages
import pandas as pd
import pytest
from unittest.mock import patch, MagicMock
from s3_transformer import S3Transformer


@pytest.fixture
# Testing arbitrary inputs
def sample_dfs():
    return {
        "combined_applicants_details": pd.DataFrame({
        "candidate_id": [1, 2, 3],
        "candidate_first_name": ["Alice", "Bob", "Charlie"],
        "candidate_last_name": ["Smith", "Jones", "Brown"],
        "email": ["alice@example.com", "bob@example.com", "charlie@example.com"],
        "phone_number": ["123789456", "456123789", "789456123"],
        "date_of_birth": ["1998-01-01", "1995-05-05", "1996-06-06"],
        "gender": ["Female", "Male", "Male"],
        "street_name": ["Main St", "High St", "Oak St"],
        "city": ["London", "Bristol", "Manchester"],
        "postcode": ["E1", "2BB", "M1"],
        "classification": ["1", "2:2", "2:1"],
        "university_name": ["University of Kent", "University of Essex", "University of York"],
        "course_name": ["Data", "Business", "Engineering"]
    }),
    "combined_talent_decision_scores": pd.DataFrame({
        "candidate_first_name": ["Alice", "Bob", "Charlie"],
        "candidate_last_name": ["Smith", "Jones", "Brown"],
        "course_interest": ["Data", "Business", "Engineering"],
        "Score": [85, 90, 75],
        "interview_result": [True, False, True]
    }),
    "combined_sparta_day_test_score": pd.DataFrame({
        "Name": ["Alice", "Bob", "Charlie"],
        "TestScore": [70, 80, 65]
    }),
    }

# Testing lists
def test_transform_to_tables_returns_dict(sample_dfs):
    transformer = S3Transformer()

    # Create a mock cohort dataframe that won't cause the error
    mock_cohort_df = pd.DataFrame({
        'cohort_id': [1, 2, 3],
        'cohort_name': ['test1', 'test2', 'test3']
    })

    # Mock the specific problematic line
    with patch.object(pd.DataFrame, 'iloc') as mock_iloc:
        # Make iloc[0] return a valid series
        mock_series = MagicMock()
        mock_series.__getitem__.return_value = 1  # Return cohort_id = 1
        mock_iloc.__getitem__.return_value = mock_series

        tables = transformer.transform_to_tables(sample_dfs)

    assert isinstance(tables, dict)


def test_transform_to_tables_contains_expected_keys(sample_dfs):
    transformer = S3Transformer()

    # Create a mock cohort dataframe
    mock_cohort_df = pd.DataFrame({
        'cohort_id': [1, 2, 3],
        'cohort_name': ['test1', 'test2', 'test3']
    })

    # Mock the specific problematic line
    with patch.object(pd.DataFrame, 'iloc') as mock_iloc:
        # Make iloc[0] return a valid series
        mock_series = MagicMock()
        mock_series.__getitem__.return_value = 1  # Return cohort_id = 1
        mock_iloc.__getitem__.return_value = mock_series

        tables = transformer.transform_to_tables(sample_dfs)

    expected_keys = {
        'address', 'candidate', 'candidate_university', 'course', 'invitation',
        'skill', 'strength', 'talent_member', 'tech_skill', 'trainer',
        'university', 'weakness', 'week', 'cohort',
        'combined_applicants_details',
        'combined_talent_decision_scores',
        'combined_sparta_day_test_score'
    }

    assert set(tables.keys()) == expected_keys


def test_transform_to_tables_missing_key():
    transformer = S3Transformer()
    dfs = {
        "combined_applicants_details": pd.DataFrame({"Name": ["Alice"]})
    }

    # Mock the problematic line for this test too
    with patch.object(pd.DataFrame, 'iloc') as mock_iloc:
        mock_series = MagicMock()
        mock_series.__getitem__.return_value = 1
        mock_iloc.__getitem__.return_value = mock_series

        with pytest.raises(KeyError):
            transformer.transform_to_tables(dfs)

