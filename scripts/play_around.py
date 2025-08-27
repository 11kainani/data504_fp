## INVITATION
# Mapping
talent_member_lookup = {(row['talent_member_first_name'], row['talent_member_last_name']): row['talent_member_id']
                        for _, row in tables['talent_member'].iterrows()}

# Collect invitation rows from all dfs
inv_rows = []
for df in dfs.values():
    if {'candidate_id', 'talent_member_first_name', 'talent_member_last_name', 'invitation_date'}.issubset(df.columns):
        inv_rows.append(
            df[['candidate_id', 'talent_member_first_name', 'talent_member_last_name', 'invitation_date']].dropna())

# Combine all invitation rows
if inv_rows:
    invitation_df = pd.concat(inv_rows, ignore_index=True)
else:
    invitation_df = pd.DataFrame(
        columns=['candidate_id', 'talent_member_first_name', 'talent_member_last_name', 'invitation_date'])

# Map talent names to talent_member_id
invitation_df['talent_member_id'] = invitation_df.apply(
    lambda row: talent_member_lookup.get((row['talent_member_first_name'], row['talent_member_last_name'])), axis=1)

# Assign primary key for invitations
invitation_df['invitation_id'] = range(1, len(invitation_df) + 1)

# Keep only relevant columns
invitation_df = invitation_df[['invitation_id', 'candidate_id', 'talent_member_id', 'invitation_date']]
tables['invitation'] = invitation_df

# --------------------------------------------------- Student -------------------------------------------------------
# Student ID starts at 1, and students are candidates who have been accepted (result = 'Pass')
student_rows = []
for df_name, df in dfs.items():
    if 'result' in df.columns and 'candidate_id' in df.columns:
        # Filter for candidates who passed
        passed_candidates = df[df['result'].str.upper() == 'PASS'][['candidate_id', 'course_interest']].dropna()
        student_rows.append(passed_candidates)

if student_rows:
    student_df = pd.concat(student_rows, ignore_index=True).drop_duplicates()

    # Map course names to course IDs
    course_lookup = {row['course_name']: row['course_id'] for _, row in tables['course'].iterrows()}
    student_df['course_id'] = student_df['course_interest'].map(course_lookup)

    # Assign student IDs
    student_df['student_id'] = range(1, len(student_df) + 1)

    # Keep only relevant columns
    student_df = student_df[['student_id', 'candidate_id', 'course_id']]
    tables['student'] = student_df
else:
    tables['student'] = pd.DataFrame(columns=['student_id', 'candidate_id', 'course_id'])