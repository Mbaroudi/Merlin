Slowly Changing Dimension(SCD) Type 2 processing

The "Slowly Changing Dimension" problem is a common one particular to data warehousing. In a nutshell, this applies to cases where the attribute for a record varies over time.
Type 2 slowly changing dimension should be used when it is necessary for the data warehouse to track historical changes. This method tracks historical data by creating multiple records for a given natural key

Running Flow

Please follow the next steps to run flow:

1. Run setup.py to install necessary packages and copy required resources to appropriate locations
2. Run flow.py. Flow steps:
                Step 1. Upload files with updated SCD from local file system to HDFS
                Step 2. Run Pig job to merge active SCD snapshot with provided updates
                Step 3. Upload job result back to Local File System
3. Run cleanup.py to clean up environment

TAGS:  copyFromLocal, pig, getmerge




