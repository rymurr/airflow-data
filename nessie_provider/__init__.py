#!/usr/bin/env python
# -*- coding: utf-8 -*-


def get_provider_info():
    return {
        "package-name": "airflow-nessie-provider",
        "name": "Nessie Airflow Provider",
        "description": "An Airflow provider for Project Nessie",
        "hook-class-names": ["nessie_provider.hooks.nessie_hook.NessieHook"],
        "versions": ["0.0.1"]
    }
