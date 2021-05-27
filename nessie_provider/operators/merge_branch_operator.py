from typing import Any, Callable, Dict, Optional

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from nessie_provider.hooks.nessie_hook import NessieHook


class MergeOperator(BaseOperator):
    """
    """

    def __init__(self, nessie_conn_id: str, branch: str, source_branch: str = "main", **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.nessie_conn_id = nessie_conn_id
        self.branch = branch
        self.source_branch = source_branch

    def execute(self, context: Dict[str, Any]) -> Any:
        hook = NessieHook(nessie_conn_id=self.nessie_conn_id)

        hook.merge(self.branch, self.source_branch)

