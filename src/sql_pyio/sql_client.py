#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

from typing import Optional, Callable, Dict, List
import json

import daft
from daft import DataFrame
import daft.context
from daft.datatype import DataType
import sqlalchemy
from sqlalchemy import Connection


__all__ = ["SqlClient", "SqlClientError"]


class SqlClientError(Exception):
    def __init__(self, message=None):
        self.message = message


class SqlClient(object):
    """
    Wrapper for daft package.
    """

    def __init__(
        self,
        db_url: str,
        use_native_runner: bool = True,
        **kwargs,
    ):
        """Constructor of the SqlClient.

        Args:
            db_url (str): Database URL
            use_native_runner (bool, optional): Indicator whether to use the native runner of daft. Defaults to True.
            **kwargs: Arguments used to be added to SQL alchema engine connection.
        """
        self.use_native_runner = use_native_runner
        self.conn = lambda: sqlalchemy.create_engine(db_url, **kwargs).connect()

    def set_conn(self, db_url: str, **kwargs) -> Callable[[], "Connection"]:
        """Sets SQL alchemy connection via a connection factory.

        Args:
            db_url (str): Database URL.
            **kwargs: Arguments used to be added to SQL alchemy engine connection.

        Raises:
            SqlClientError: SQL client error.

        Returns:
            Callable[[], Connection]: SQLAlchemy connection factory.
        """
        try:
            return lambda: sqlalchemy.create_engine(db_url, **kwargs).connect()
        except Exception as e:
            raise SqlClientError(
                f"Failed to set connection. db_url: {db_url}, error: {e}"
            )

    def return_queries(
        self,
        sql: str,
        partition_col: Optional[str] = None,
        num_partitions: Optional[int] = None,
        partition_bound_strategy: str = "min-max",
        disable_pushdowns_to_sql: bool = False,
        infer_schema: bool = True,
        infer_schema_length: int = 10,
        schema: Optional[Dict[str, DataType]] = None,
    ) -> List[str]:
        """Returns one or more query strings from SQL query configurations.

        Args:
            sql (str): SQL query to execute.
            partition_col (Optional[str], optional): Column to partition the data. Defaults to None.
            num_partitions (Optional[int], optional): Number of partitions to read the data. Defaults to None.
            partition_bound_strategy (str, optional): Strategy to determine partition bounds, either "min-max" or "percentile". Defaults to "min-max".
            disable_pushdowns_to_sql (bool, optional): Whether to disable pushdowns to the SQL query. Defaults to False.
            infer_schema (bool, optional):  Whether to turn on schema inference. Defaults to True.
            infer_schema_length (int, optional): The number of rows to scan when inferring the schema. Defaults to 10.
            schema (Optional[Dict[str, DataType]], optional): A mapping of column names to datatypes. Defaults to None.

        Raises:
            SqlClientError: SQL client error.

        Returns:
            List[str]: List of query strings.
        """
        try:
            if self.use_native_runner:
                daft.context.set_runner_native()
            df = daft.read_sql(
                sql=sql,
                conn=self.conn,
                partition_col=partition_col,
                num_partitions=num_partitions,
                partition_bound_strategy=partition_bound_strategy,
                disable_pushdowns_to_sql=disable_pushdowns_to_sql,
                infer_schema=infer_schema,
                infer_schema_length=infer_schema_length,
                schema=schema,
            )

            physical_plan_scheduler = df._builder.to_physical_plan_scheduler(
                daft.context.get_context().daft_execution_config
            )
            physical_plan_dict = json.loads(physical_plan_scheduler.to_json_string())

            sql_queries = []
            for task in physical_plan_dict["TabularScan"]["scan_tasks"]:
                sql_queries.append(task["file_format_config"]["Database"]["sql"])
            return sql_queries
        except Exception as e:
            raise SqlClientError(str(e))

    def return_df(
        self,
        sql: str,
        partition_bound_strategy: str = "min-max",
        disable_pushdowns_to_sql: bool = False,
        infer_schema: bool = True,
        infer_schema_length: int = 10,
        schema: Optional[Dict[str, DataType]] = None,
    ) -> DataFrame:
        """Creates a DataFrame from the results of a SQL query.

        Args:
            sql (str): SQL query to execute.
            partition_bound_strategy (str, optional): Strategy to determine partition bounds, either "min-max" or "percentile". Defaults to "min-max".
            disable_pushdowns_to_sql (bool, optional): Whether to disable pushdowns to the SQL query. Defaults to False.
            infer_schema (bool, optional):  Whether to turn on schema inference. Defaults to True.
            infer_schema_length (int, optional): The number of rows to scan when inferring the schema. Defaults to 10.
            schema (Optional[Dict[str, DataType]], optional): A mapping of column names to datatypes. Defaults to None.

        Raises:
            SqlClientError: SQL client error.

        Returns:
            DataFrame: Dataframe containing the results of the query.
        """
        try:
            if self.use_native_runner:
                daft.context.set_runner_native()
            return daft.read_sql(
                sql=sql,
                conn=self.conn,
                partition_bound_strategy=partition_bound_strategy,
                disable_pushdowns_to_sql=disable_pushdowns_to_sql,
                infer_schema=infer_schema,
                infer_schema_length=infer_schema_length,
                schema=schema,
            )
        except Exception as e:
            raise SqlClientError(str(e))
