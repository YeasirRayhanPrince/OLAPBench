"""
Setup script for calcite-plangen-py package.
"""

from setuptools import setup, find_packages
from pathlib import Path

# Read README
readme_path = Path(__file__).parent / "README.md"
long_description = (
    readme_path.read_text() if readme_path.exists() else "Calcite Plan Generator Python Wrapper"
)

setup(
    name="calcite-plangen",
    version="0.1.0",
    author="Apache Calcite Contributors",
    description="Python wrapper for Apache Calcite's unoptimized logical plan generator",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/apache/calcite",
    packages=find_packages(),
    python_requires=">=3.7",
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "Intended Audience :: Science/Research",
        "License :: OSI Approved :: Apache Software License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "Topic :: Database",
        "Topic :: Scientific/Engineering",
    ],
)
