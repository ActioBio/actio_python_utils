#!/usr/bin/env python

"""The setup script."""

from setuptools import setup, find_packages

with open("README.rst") as readme_file:
    readme = readme_file.read()

with open("HISTORY.rst") as history_file:
    history = history_file.read()

with open("requirements.txt") as requirements_file:
    requirements = [
        req for req in requirements_file.read().splitlines() if not req.startswith("#")
    ]

test_requirements = []

setup(
    author="Brett Copeland",
    author_email="brcopeland@gmail.com",
    python_requires=">=3.9",
    classifiers=[
        "Development Status :: 2 - Pre-Alpha",
        "Intended Audience :: Developers",
        "Natural Language :: English",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
    ],
    description="Python utility functions.",
    entry_points={
        "console_scripts": [
            "actio_python_utils=actio_python_utils.cli:main",
        ],
    },
    install_requires=requirements,
    long_description=readme + "\n\n" + history,
    include_package_data=True,
    keywords="actio_python_utils",
    name="actio_python_utils",
    packages=find_packages(include=["actio_python_utils", "actio_python_utils.*"]),
    test_suite="tests",
    tests_require=test_requirements,
    url="https://github.com/ActioBio/actio_python_utils",
    version="0.1.0",
    zip_safe=False,
)
