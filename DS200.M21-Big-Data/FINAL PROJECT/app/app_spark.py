#!/usr/bin/env python3
"""
Legacy entry point kept for compatibility.

This now forwards to the leakage-free V2 pipeline so callers cannot
accidentally run the old like_ratio/comment_ratio/engagement-based flow.
"""
from app.app_spark_v2_fixed import main


if __name__ == '__main__':
    main()
