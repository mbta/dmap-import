import os

# bucket constants
S3_ARCHIVE = os.getenv("ARCHIVE_BUCKET", "unset_ARCHIVE_BUCKET")
S3_ERROR = os.getenv("ERROR_BUCKET", "unset_ERROR_BUCKET")

# prefix constants
QLIK = "cubic/ods_qlik"
ODS_SCHEMA = "ods"
ODS_STATUS = os.path.join(S3_ARCHIVE, QLIK, "rds_load_status")
ODIN_PROCESSED = "odin/archive/cubic_qlik/processed"

# db constants
DB_HOST = os.getenv("DB_HOST", "unset_DB_HOST")
