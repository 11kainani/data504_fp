from s3_extractor import S3Extractor
from s3_cleaner import S3Cleaner
from s3_transformer import S3Transformer
from s3_loader import S3Loader

# Extract CSVs from S3 into DataFrames
dfs = S3Extractor().get_csvs_to_dfs()

# Clean the DataFrames
clean_dfs = S3Cleaner().clean_dfs(dfs)

# Transform into table-ready DataFrames
transformed_dfs = S3Transformer().transform_to_tables(clean_dfs)

# Load into SQL
inserter = S3Loader()  # Uses .env to determine server type, connection, driver, etc.

# Load by table name
for table_name, df in transformed_dfs.items():
    inserter.insert_dataframe(df, table_name)
