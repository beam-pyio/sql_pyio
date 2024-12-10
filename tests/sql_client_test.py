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

import os
import random
import string
import uuid
from datetime import date, timedelta
import unittest

import sqlalchemy
import pandas as pd
from parameterized import parameterized

from sql_pyio.sql_client import SqlClient, SqlClientError


def generate_df(num_rec: int = 100):
    random.seed(1237)
    users = {
        "id": range(num_rec),
        "name": [
            "".join(random.choices(string.ascii_lowercase, k=5)) for _ in range(num_rec)
        ],
        "created_at": [
            date(2024, 1, 1) + timedelta(days=random.randint(0, 100))
            for _ in range(num_rec)
        ],
    }
    orders = {
        "id": str(uuid.uuid4()),
        "user_id": [random.randrange(0, num_rec) for _ in range(num_rec * 5)],
        "num_items": [random.randrange(0, 10) for _ in range(num_rec * 5)],
        "created_at": [
            date(2024, 1, 1) + timedelta(days=random.randint(0, 100))
            for _ in range(num_rec * 5)
        ],
    }
    return pd.DataFrame(users), pd.DataFrame(orders)


class TestSqlClient(unittest.TestCase):
    current_path = os.path.dirname(os.path.abspath(__file__))
    db_url = f"sqlite:///{current_path}/develop.db"
    users, orders = generate_df(num_rec=1000)

    @classmethod
    def setUpClass(self):
        print(os.path.dirname(os.path.abspath(__file__)))
        self.users.to_sql(
            name="users",
            con=sqlalchemy.create_engine(self.db_url).connect(),
            if_exists="replace",
        )
        self.orders.to_sql(
            name="orders",
            con=sqlalchemy.create_engine(self.db_url).connect(),
            if_exists="replace",
        )
        self.client = SqlClient(db_url=self.db_url)

    @classmethod
    def tearDownClass(self):
        os.remove(f"{self.current_path}/develop.db")
        os.remove(f"{self.current_path}/non-existing-db.db")

    def test_sql_client_error(self):
        with self.assertRaises(SqlClientError) as cm:
            client = SqlClient(
                db_url=f"sqlite:///{self.current_path}/non-existing-db.db"
            )
            client.return_queries(
                sql="SELECT * FROM users", partition_col="id", num_partitions=4
            )
        self.assertRegex(cm.exception.message, r"no such table: users")

    @parameterized.expand(
        [
            ("no_partition", None, None, 1),
            ("single_partition", "id", 1, 1),
            ("multiple_partitions", "id", 4, 4),
        ]
    )
    def test_num_of_queries_and_records(
        self, name, partition_col, num_partitions, num_queries
    ):
        if partition_col is not None:
            queries = self.client.return_queries(
                sql="SELECT * FROM users",
                partition_col=partition_col,
                num_partitions=num_partitions,
            )
        else:
            queries = self.client.return_queries(sql="SELECT * FROM users")

        self.assertEqual(len(queries), num_queries)

        num_rec = 0
        for qry in queries:
            df = self.client.return_df(sql=qry)
            num_rec += len(df.to_pylist())
        self.assertEqual(num_rec, len(self.users.index))

    def test_number_of_queries_and_records_with_limit(self):
        queries = self.client.return_queries(
            sql="SELECT * FROM users LIMIT 700",
            partition_col="id",
            num_partitions=4,
        )
        self.assertEqual(len(queries), 4)

        num_rec = 0
        for qry in queries:
            df = self.client.return_df(sql=qry)
            num_rec += len(df.to_pylist())
        self.assertEqual(num_rec, 700)

    def test_number_of_queries_and_records_with_synthetic_col(self):
        queries = self.client.return_queries(
            sql="SELECT ROW_NUMBER() OVER() AS rn, * FROM users",
            partition_col="rn",
            num_partitions=4,
        )
        self.assertEqual(len(queries), 4)

        num_rec = 0
        for qry in queries:
            df = self.client.return_df(sql=qry)
            num_rec += len(df.to_pylist())
        self.assertEqual(num_rec, 1000)

    def test_number_of_queries_and_records_with_join(self):
        queries = self.client.return_queries(
            sql="SELECT ROW_NUMBER() OVER() AS rn, * FROM orders AS o JOIN users AS u ON o.user_id = u.id",
            partition_col="rn",
            num_partitions=4,
        )
        self.assertEqual(len(queries), 4)

        num_rec = 0
        for qry in queries:
            df = self.client.return_df(sql=qry)
            num_rec += len(df.to_pylist())
        self.assertEqual(num_rec, 5000)
