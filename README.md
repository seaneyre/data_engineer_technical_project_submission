# Readme

This submission has been written in Python 3.8.11.

## How to Run
- Clone or download the repository.
- Open console and cd to the location repository.
- Enter `python submission.py` in the console and run.

```
> python submission.py
```
Note: you may need to use `> python3 submission.py` if the above isn't working.

If the script is running you'll see `Script running...` output to stdout.

Once completed the following output will be printed (with # replaced by actual \
counts):
```
Script complete (# documents processed)
Count of documents with HTML stripped from body:  #
Count of documents active on 2017-02-01:  #
For count of documents by SOC2, please query the output database with this SQL: 
SELECT soc2, count(*) FROM POSTINGS GROUP BY soc2 ORDER BY count(*) DESC
```

A SQLite database will have been created in the active directory with the file\
name `output.db`.

## Packages Used
- gzip
- json
- csv
- re
- sqlite3

All of these packages are pre-installed as part of the Python3 standard library.

## Submission Notes
- In soc_hierarchy.csv, some SOC5 codes did not have a full child-parent relationship through to SOC2. 
    - The output in these cases is a populated `soc5` field, and a NULL `soc2` field.
- Some rough profiling results (see `memory_usage_analysis.py`):
    - `submission.py` average peak memory usage = 1.4MB
    - loading full sample.gz into memory = 178MB
