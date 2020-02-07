"""
    Flywheel De-Id Export
"""

import os
from setuptools import setup, find_packages

NAME = "flywheel-deid-export"

VERSION = os.getenv("CI_COMMIT_TAG", "1.0.0")

setup(
    name=NAME,
    version=VERSION,
    description="Utilities for de-identification/anonymization of files within Flywheel",
    long_description="Utilities for de-identification/anonymization of files within Flywheel",
    project_urls={"Source": "https://github.com/flywheel-apps/GRP-13/"},
    author="Flywheel",
    author_email="support@flywheel.io",
    license="MIT",
    install_requires=[

    ],
    extras_require={
        "dev": [

        ],
    },
    packages=find_packages(),
    include_package_data=True,
    zip_safe=False,
)
