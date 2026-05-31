#!/usr/bin/env python3
"""Backward-compatible wrapper for the batch cleaning job."""

from app.spark_jobs.cleaning_job import clean_data


if __name__ == "__main__":
    clean_data()
