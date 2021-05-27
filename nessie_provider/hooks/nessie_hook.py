#!/usr/bin/env python
# -*- coding: utf-8 -*-
from typing import Dict, Any
from pynessie import init, NessieClient

from airflow.hooks.base import BaseHook


class NessieHook(BaseHook):
    """
    """

    conn_name_attr = 'nessie_conn_id'
    default_conn_name = 'nessie_default'
    conn_type = 'nessie'
    hook_name = 'Nessie'

    @staticmethod
    def get_ui_field_behaviour() -> Dict:
        """Returns custom field behaviour"""
        return {
            "hidden_fields": ['port'],
            "relabeling": {'schema': 'Branch', 'host': 'Nessie URI'},
        }

    def __init__(self, nessie_conn_id: str = default_conn_name) -> None:
        super().__init__()
        self.nessie_conn_id = nessie_conn_id

    def get_conn(self) -> NessieClient:
        conn = self.get_connection(self.nessie_conn_id)

        return init(config_dict={'endpoint': conn.host, 'default_branch': conn.schema})

    def create_reference(self,
                         name: str,
                         source_ref: str = 'main',
                         is_tag=False) -> str:
        hash_ = self.get_conn().get_reference(source_ref).hash_
        return self.get_conn().create_branch(name, hash_).name

    def merge(self,
              source_ref: str,
              destination_branch: str) -> None:
        self.get_conn().merge(destination_branch, source_ref)
