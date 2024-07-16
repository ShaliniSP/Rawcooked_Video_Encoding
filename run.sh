#!/bin/bash


# Run assessment
echo "Running dpx_assessment.py..."
python3 dpx_assessment.py

# Run rawcooked
echo "Running dpx_rawcook.py..."
python3 dpx_rawcook.py

# Run post_rawcooked
echo "Running dpx_post_rawcook.py..."
python3 dpx_post_rawcook.py

echo "All scripts executed."
