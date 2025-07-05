from setuptools import setup, find_packages

setup(
    name='skewbalancer',
    version='0.1.0',
    packages=find_packages(),
    install_requires=[
        'pyspark',
        'pandas',
        'matplotlib',
        'seaborn'
    ],
    author='Omar Attia',
    description='Auto skew detection and value-based salting for Spark DataFrames',
    license='MIT',
)