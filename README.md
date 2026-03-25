# Spark Empire

A collection of Spark scripts built for practical data engineering use cases.

This repository is meant to document useful Spark jobs, patterns, and examples so others can learn from them, reuse them, or adapt them for their own pipelines.

## What is in this repo

- Reusable Spark scripts
- Real-world data loading patterns
- Incremental and full-load examples
- API-to-Spark ingestion patterns
- Databricks-friendly notebook/job logic

## Spark scripts

- [Generic MuleSoft to Databricks Loader](./generic-mulesoft-to-databricks-loader.py)  
  A config-driven Spark job that reads object metadata from Databricks tables, calls MuleSoft APIs, loads records into Delta tables, updates watermarks, and logs each run.

- [Salesforce to Databricks Loader](./salesforce-to-databricks-loader.py)  
  Spark job for loading Salesforce objects into Databricks tables with incremental watermark handling.

- [Multi-Object Incremental Loader](./multi-object-incremental-loader.py)  
  Generic Spark pattern for loading multiple source objects sequentially using a metadata-driven approach.

- [Delta Watermark Manager](./delta-watermark-manager.py)  
  Helper logic for reading and updating watermark timestamps in Databricks Delta tables.

- [Job Run Logger](./job-run-logger.py)  
  Utility script for logging Spark job execution details, status, row counts, timestamps, and errors into a Delta control table.

## Design principles

- Keep jobs generic and easy to extend
- Separate environment-specific settings from core logic
- Prefer metadata-driven configuration over hardcoding
- Log every run for audit and troubleshooting
- Support incremental processing through watermarks
- Stay compatible with Databricks notebook jobs and Delta tables

## Repository structure

- `*.py`  
  Spark job scripts and helpers

- `README.md`  
  Repository overview and script index

## How to use

- Clone the repo
- Review the script you want to run
- Update environment-specific values
- Configure your Databricks secrets, tables, and metadata
- Run the script as a notebook job or standard Spark job

## Who this is for

- Data engineers
- Integration engineers
- Databricks users
- People building practical Spark ingestion jobs
- Anyone looking for reusable Spark patterns

## Notes

- Some scripts assume Databricks and Delta Lake
- Some scripts assume metadata and control tables already exist
- Adjust file names and paths if your actual script names differ

## Contributing

- Add new Spark scripts
- Keep naming clear and practical
- Update this README with links and short descriptions
- Prefer reusable patterns over one-off code

## Cheers

If this repo helps you, great.  
If you improve something, even better.
