#!/usr/bin/env python3
"""Backward-compatible wrapper for the structured streaming job."""

from app.spark_jobs.streaming_job import run_stream


if __name__ == "__main__":
    run_stream()
