from setuptools import setup, find_packages

setup(
    name="snow_pipeline_pkg",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[
        "snowflake-snowpark-python",
        "pandas",
    ],
    author="Cory Purkis",
    description="Reusable Avro-to-Snowflake staging pipeline",
)
