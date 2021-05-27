#!/usr/bin/env python
# -*- coding: utf-8 -*-
# flake8: noqa
"""The setup script."""
from setuptools import find_packages
from setuptools import setup

setup_requirements = ["pip"]

setup(
    author="Ryan Murray",
    author_email="nessie-release-builder@dremio.com",
    python_requires=">=3.5",
    classifiers=[
        "Development Status :: 2 - Pre-Alpha",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: Apache Software License",
        "Natural Language :: English",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
    ],
    description="Project Nessie: A Git-like Experience for your Data Lake",
    install_requires=["pynessie>0.6.0"],
    license="Apache Software License 2.0",
    long_description="foo",
    include_package_data=True,
    keywords="airflow-nessie-provider",
    name="airflow-nessie-provider",
    packages=find_packages(include=["nessie_provider", "nessie_provider.*"]),
    entry_points={
        "apache_airflow_provider": [
            "provider_info=nessie_provider.__init__:get_provider_info"
        ]
    },
    setup_requires=setup_requirements,
    url="https://github.com/projectnessie/nessie",
    version="0.0.1",
    zip_safe=False,
)
