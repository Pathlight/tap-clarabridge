#!/usr/bin/env python
from setuptools import setup

setup(
    name="tap-clarabridge",
    version="0.2.2",
    description="Singer.io tap for extracting data",
    author="Stitch",
    url="http://singer.io",
    classifiers=["Programming Language :: Python :: 3 :: Only"],
    py_modules=["tap_clarabridge"],
    install_requires=[
        # NB: Pin these to a more specific version for tap reliability
        "singer-python",
        "requests",
    ],
    entry_points="""
    [console_scripts]
    tap-clarabridge=tap_clarabridge:main
    """,
    packages=["tap_clarabridge"],
    package_data = {
        "schemas": ["tap_clarabridge/schemas/*.json"]
    },
    include_package_data=True,
)
