#!/usr/bin/env python3
"""
Setup script for the Real-Time Economic Analytics Platform.
"""

from setuptools import setup, find_packages

setup(
    name="real-time-analytics-platform",
    version="1.0.0",
    description="Real-Time Economic Analytics Platform with OOP architecture",
    author="Yahya Bouchak",
    author_email="yahya.bouchak@example.com",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    install_requires=[
        "streamlit==1.30.0",
        "pandas==2.1.4",
        "plotly==5.18.0",
        "openpyxl==3.1.2",
        "streamlit-option-menu==0.3.6",
        "pyspark==3.5.0",
        "confluent-kafka==2.3.0",
        "pymongo==4.6.1",
        "numpy==1.26.4"
    ],
    python_requires=">=3.8",
    entry_points={
        "console_scripts": [
            "analytics-platform=main:main",
        ],
    },
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Financial and Insurance Industry",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
    ],
)