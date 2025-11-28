import unittest

from QueryParser import QueryParser

TEST_CASES = [
    {
        "name": "basic_query",
        "query": """
        SELECT
            t1.A,
            B
        FROM EXAMPLE_DB.EXAMPLE_SCHEMA.TABLE_A t1
        LEFT JOIN EXAMPLE_DB.EXAMPLE_SCHEMA.TABLE_B t2
            ON t1.ID = t2.ID
        """,
        "expected_columns": [
            {"name": "A", "potential_tables": ["EXAMPLE_DB.EXAMPLE_SCHEMA.TABLE_A"]},
            {
                "name": "B",
                "potential_tables": [
                    "EXAMPLE_DB.EXAMPLE_SCHEMA.TABLE_A",
                    "EXAMPLE_DB.EXAMPLE_SCHEMA.TABLE_B",
                ],
            },
            {"name": "ID", "potential_tables": ["EXAMPLE_DB.EXAMPLE_SCHEMA.TABLE_A"]},
            {"name": "ID", "potential_tables": ["EXAMPLE_DB.EXAMPLE_SCHEMA.TABLE_B"]},
        ],
        "expected_joins": [
            {
                "join_type": "LEFT JOIN",
                "column_left": {
                    "name": "ID",
                    "potential_tables": ["EXAMPLE_DB.EXAMPLE_SCHEMA.TABLE_A"],
                },
                "column_right": {
                    "name": "ID",
                    "potential_tables": ["EXAMPLE_DB.EXAMPLE_SCHEMA.TABLE_B"],
                },
            }
        ],
    },
    {
        "name": "nested_cte_depth_3",
        "query": """
        WITH level1 AS (
            SELECT order_id, revenue FROM SALES.L1
        ),
        level2 AS (
            SELECT order_id, revenue FROM level1
        ),
        level3 AS (
            SELECT order_id, revenue FROM level2
        )
        SELECT order_id FROM level3
        """,
        "expected_columns": [
            {"name": "order_id", "potential_tables": ["SALES.L1"]},
            {"name": "revenue", "potential_tables": ["SALES.L1"]},
        ],
        "expected_joins": [],
    },
    {
        "name": "nested_cte_depth_4",
        "query": """
        WITH cte_a AS (
            SELECT id, amount FROM FIN.A
        ),
        cte_b AS (
            SELECT id, amount FROM cte_a
        ),
        cte_c AS (
            SELECT id, amount FROM cte_b
        ),
        cte_d AS (
            SELECT id, amount FROM cte_c
        )
        SELECT id FROM cte_d
        """,
        "expected_columns": [
            {"name": "id", "potential_tables": ["FIN.A"]},
            {"name": "amount", "potential_tables": ["FIN.A"]},
        ],
        "expected_joins": [],
    },
    {
        "name": "nested_cte_depth_5",
        "query": """
        WITH c1 AS (
            SELECT product_id, qty FROM INV.C1
        ),
        c2 AS (
            SELECT product_id, qty FROM c1
        ),
        c3 AS (
            SELECT product_id, qty FROM c2
        ),
        c4 AS (
            SELECT product_id, qty FROM c3
        ),
        c5 AS (
            SELECT product_id, qty FROM c4
        )
        SELECT product_id FROM c5
        """,
        "expected_columns": [
            {"name": "product_id", "potential_tables": ["INV.C1"]},
            {"name": "qty", "potential_tables": ["INV.C1"]},
        ],
        "expected_joins": [],
    },
    {
        "name": "nested_cte_depth_6_with_subquery",
        "query": """
        WITH base AS (
            SELECT account_id, balance FROM CORE.BASE
        ),
        cte_2 AS (SELECT * FROM base),
        cte_3 AS (SELECT * FROM cte_2),
        cte_4 AS (
            SELECT account_id, balance FROM (
                SELECT account_id, balance FROM cte_3
            )
        ),
        cte_5 AS (
            SELECT account_id, balance FROM cte_4
        ),
        cte_6 AS (
            SELECT account_id, balance FROM cte_5
        )
        SELECT account_id, b.balance
        FROM cte_6 a
        JOIN CORE.BALANCE_LOOKUP b
            ON a.account_id = b.account_id
        """,
        "expected_columns": [
            {"name": "account_id", "potential_tables": ["CORE.BASE"]},
            {"name": "balance", "potential_tables": ["CORE.BASE"]},
            {"name": "balance", "potential_tables": ["CORE.BALANCE_LOOKUP"]},
            {"name": "account_id", "potential_tables": ["CORE.BALANCE_LOOKUP"]},
        ],
        "expected_joins": [
            {
                "join_type": "INNER JOIN",
                "column_left": {
                    "name": "account_id",
                    "potential_tables": ["CORE.BASE"],
                },
                "column_right": {
                    "name": "account_id",
                    "potential_tables": ["CORE.BALANCE_LOOKUP"],
                },
            }
        ],
    },
    {
        "name": "select_star_with_aliases",
        "query": """
        SELECT
            main.*,
            detail.extra_value
        FROM (
            SELECT
                customer_id,
                order_id,
                amount
            FROM SALES_DB.PUBLIC.ORDERS
        ) main
        LEFT JOIN (
            SELECT
                order_id,
                extra_value
            FROM SALES_DB.PUBLIC.ORDER_DETAILS
        ) detail
            ON main.order_id = detail.order_id
        """,
        "expected_columns": [
            {"name": "customer_id", "potential_tables": ["SALES_DB.PUBLIC.ORDERS"]},
            {"name": "order_id", "potential_tables": ["SALES_DB.PUBLIC.ORDERS"]},
            {"name": "amount", "potential_tables": ["SALES_DB.PUBLIC.ORDERS"]},
            {
                "name": "extra_value",
                "potential_tables": ["SALES_DB.PUBLIC.ORDER_DETAILS"],
            },
            {"name": "order_id", "potential_tables": ["SALES_DB.PUBLIC.ORDER_DETAILS"]},
        ],
        "expected_joins": [
            {
                "join_type": "LEFT JOIN",
                "column_left": {
                    "name": "order_id",
                    "potential_tables": ["SALES_DB.PUBLIC.ORDERS"],
                },
                "column_right": {
                    "name": "order_id",
                    "potential_tables": ["SALES_DB.PUBLIC.ORDER_DETAILS"],
                },
            }
        ],
    },
    {
        "name": "nested_union_with_cte",
        "query": """
        WITH regional_sales AS (
            SELECT store_id, revenue FROM NORTH_REGION.SALES
            UNION ALL
            SELECT store_id, revenue FROM SOUTH_REGION.SALES
        )
        SELECT
            rs.store_id,
            rs.revenue,
            targets.target_revenue
        FROM (
            SELECT store_id, SUM(revenue) AS revenue
            FROM regional_sales
            GROUP BY store_id
        ) rs
        INNER JOIN CORPORATE.TARGETS targets
            ON rs.store_id = targets.store_id
        WHERE rs.revenue > 100000
        """,
        "expected_columns": [
            {"name": "store_id", "potential_tables": ["NORTH_REGION.SALES"]},
            {"name": "store_id", "potential_tables": ["SOUTH_REGION.SALES"]},
            {"name": "revenue", "potential_tables": ["NORTH_REGION.SALES"]},
            {"name": "revenue", "potential_tables": ["SOUTH_REGION.SALES"]},
            {"name": "store_id", "potential_tables": ["CORPORATE.TARGETS"]},
            {"name": "target_revenue", "potential_tables": ["CORPORATE.TARGETS"]},
        ],
        "expected_joins": [
            {
                "join_type": "INNER JOIN",
                "column_left": {
                    "name": "store_id",
                    "potential_tables": ["NORTH_REGION.SALES", "SOUTH_REGION.SALES"],
                },
                "column_right": {
                    "name": "store_id",
                    "potential_tables": ["CORPORATE.TARGETS"],
                },
            }
        ],
    },
    {
        "name": "cascading_cte",
        "query": """
        WITH base AS (
            SELECT
                id,
                amount
            FROM PROD_DB.FINANCE.SALES_TRANSACTIONS st
        ),
        filtered AS (
            SELECT id FROM base WHERE amount > 100
        )
        SELECT id FROM filtered
        """,
        "expected_columns": [
            {"name": "id", "potential_tables": ["PROD_DB.FINANCE.SALES_TRANSACTIONS"]},
            {
                "name": "amount",
                "potential_tables": ["PROD_DB.FINANCE.SALES_TRANSACTIONS"],
            },
        ],
        "expected_joins": [],
    },
    {
        "name": "subquery_alias_and_join_tracking",
        "query": """
        SELECT
            derived.account_id,
            t3.status
        FROM (
            SELECT
                t1.account_id,
                t2.lookup_value
            FROM CORE.ACCOUNTS t1
            INNER JOIN CORE.ACCOUNT_LOOKUP t2
                ON t1.account_id = t2.account_id
        ) derived
        JOIN CORE.ACCOUNT_STATUS t3
            ON derived.account_id = t3.account_id
        """,
        "expected_columns": [
            {"name": "status", "potential_tables": ["CORE.ACCOUNT_STATUS"]},
            {"name": "account_id", "potential_tables": ["CORE.ACCOUNT_STATUS"]},
            {"name": "account_id", "potential_tables": ["CORE.ACCOUNTS"]},
            {"name": "lookup_value", "potential_tables": ["CORE.ACCOUNT_LOOKUP"]},
            {"name": "account_id", "potential_tables": ["CORE.ACCOUNT_LOOKUP"]},
        ],
        "expected_joins": [
            {
                "join_type": "INNER JOIN",
                "column_left": {
                    "name": "account_id",
                    "potential_tables": ["CORE.ACCOUNTS"],
                },
                "column_right": {
                    "name": "account_id",
                    "potential_tables": ["CORE.ACCOUNT_STATUS"],
                },
            },
            {
                "join_type": "INNER JOIN",
                "column_left": {
                    "name": "account_id",
                    "potential_tables": ["CORE.ACCOUNTS"],
                },
                "column_right": {
                    "name": "account_id",
                    "potential_tables": ["CORE.ACCOUNT_LOOKUP"],
                },
            },
        ],
    },
    {
        "name": "join_using_clause",
        "query": """
        SELECT *
        FROM CRM.CONTACTS c
        JOIN CRM.CONTACT_DETAILS d USING (CONTACT_ID)
        """,
        "expected_columns": [],
        "expected_joins": [
            {
                "join_type": "INNER JOIN",
                "column_left": {
                    "name": "CONTACT_ID",
                    "potential_tables": ["CRM.CONTACTS"],
                },
                "column_right": {
                    "name": "CONTACT_ID",
                    "potential_tables": ["CRM.CONTACT_DETAILS"],
                },
            }
        ],
    },
    {
        "name": "join_with_or_and_inequalities",
        "query": """
        SELECT *
        FROM INVENTORY.PRODUCTS p
        LEFT JOIN INVENTORY.STOCK s
            ON p.PRODUCT_ID = s.PRODUCT_ID OR p.LAST_UPDATED >= s.LAST_UPDATED
        """,
        "expected_columns": [
            {"name": "PRODUCT_ID", "potential_tables": ["INVENTORY.PRODUCTS"]},
            {"name": "PRODUCT_ID", "potential_tables": ["INVENTORY.STOCK"]},
            {"name": "LAST_UPDATED", "potential_tables": ["INVENTORY.PRODUCTS"]},
            {"name": "LAST_UPDATED", "potential_tables": ["INVENTORY.STOCK"]},
        ],
        "expected_joins": [
            {
                "join_type": "LEFT JOIN",
                "column_left": {
                    "name": "PRODUCT_ID",
                    "potential_tables": ["INVENTORY.PRODUCTS"],
                },
                "column_right": {
                    "name": "PRODUCT_ID",
                    "potential_tables": ["INVENTORY.STOCK"],
                },
            },
            {
                "join_type": "LEFT JOIN",
                "column_left": {
                    "name": "LAST_UPDATED",
                    "potential_tables": ["INVENTORY.PRODUCTS"],
                },
                "column_right": {
                    "name": "LAST_UPDATED",
                    "potential_tables": ["INVENTORY.STOCK"],
                },
            },
        ],
    },
    {
        "name": "complex_cte_query",
        "query": """
        WITH CTE1 AS (
            SELECT
                CUST_ID,
                CUST_NM,
                CUST_ATTRIBUTE
            FROM (
                SELECT
                    CUST_ID,
                    CUST_NM,
                    CUST_ATTRIBUTE
                FROM
                    COMPANY_DB.CUST_SCHEMA.CUSTOMER_TABLE
                WHERE
                    EFF_DT > DATE'2025-01-01'
            ) AS inner_cte
        ),
        CTE2 AS (
            SELECT
                c.CUST_ID,
                c.CUST_NM,
                c.CUST_ATTRIBUTE + d.CUST_ATTRIBUTE AS COMBINED_ATTRIBUTE,
                d.START_DT
            FROM
                CTE1 c
            INNER JOIN
                COMPANY_DB.CUST_SCHEMA.TRANS_TBL d
                ON d.CUST_ID = c.CUST_ID
        )
        SELECT
            CUST_ID,
            CUST_NM,
            COMBINED_ATTRIBUTE
        FROM CTE2
        WHERE COMBINED_ATTRIBUTE > 10;
        """,
        "expected_columns": [
            {
                "name": "CUST_ID",
                "potential_tables": ["COMPANY_DB.CUST_SCHEMA.CUSTOMER_TABLE"],
            },
            {
                "name": "CUST_ID",
                "potential_tables": ["COMPANY_DB.CUST_SCHEMA.TRANS_TBL"],
            },
            {
                "name": "CUST_NM",
                "potential_tables": ["COMPANY_DB.CUST_SCHEMA.CUSTOMER_TABLE"],
            },
            {
                "name": "CUST_ATTRIBUTE",
                "potential_tables": ["COMPANY_DB.CUST_SCHEMA.CUSTOMER_TABLE"],
            },
            {
                "name": "CUST_ATTRIBUTE",
                "potential_tables": ["COMPANY_DB.CUST_SCHEMA.TRANS_TBL"],
            },
            {
                "name": "START_DT",
                "potential_tables": ["COMPANY_DB.CUST_SCHEMA.TRANS_TBL"],
            },
            {
                "name": "EFF_DT",
                "potential_tables": ["COMPANY_DB.CUST_SCHEMA.CUSTOMER_TABLE"],
            },
        ],
        "expected_joins": [
            {
                "join_type": "INNER JOIN",
                "column_left": {
                    "name": "CUST_ID",
                    "potential_tables": ["COMPANY_DB.CUST_SCHEMA.CUSTOMER_TABLE"],
                },
                "column_right": {
                    "name": "CUST_ID",
                    "potential_tables": ["COMPANY_DB.CUST_SCHEMA.TRANS_TBL"],
                },
            }
        ],
        "expected_filters": [
            {
                "query": "EFF_DT > CAST('2025-01-01' AS DATE)",
                "filter_type": "WHERE",
                "operator": ">",
                "columns": [
                    {
                        "name": "EFF_DT",
                        "potential_tables": ["COMPANY_DB.CUST_SCHEMA.CUSTOMER_TABLE"],
                    }
                ],
            },
            {
                "query": "COMBINED_ATTRIBUTE > 10",
                "filter_type": "WHERE",
                "operator": ">",
                "columns": [
                    {
                        "name": "COMBINED_ATTRIBUTE",
                        "potential_tables": [
                            "COMPANY_DB.CUST_SCHEMA.CUSTOMER_TABLE",
                            "COMPANY_DB.CUST_SCHEMA.TRANS_TBL",
                        ],
                    }
                ],
            },
        ],
    },
    {
        "name": "include_unused_column_query",
        "query": """
        WITH CTE1 AS (
            SELECT
                COLUMN_A1,
                COLUMN_A2,
                COLUMN_B1,
                COLUMN_B2
            FROM
                TABLE_1
            INNER JOIN
                TABLE_2
            ON TABLE_1.ID = TABLE_2.ID
            WHERE
                TABLE_1.DT <= TABLE_2.DT
        )
        SELECT
            ANOTHER_COL
        FROM
            ANOTHER_DB.ANOTHER_SCHEMA.ANOTHER_TABLE;
        """,
        "expected_columns": [
            {
                "name": "ANOTHER_COL",
                "potential_tables": ["ANOTHER_DB.ANOTHER_SCHEMA.ANOTHER_TABLE"],
            },
            {"name": "COLUMN_A1", "potential_tables": ["TABLE_1", "TABLE_2"]},
            {"name": "COLUMN_A2", "potential_tables": ["TABLE_1", "TABLE_2"]},
            {"name": "COLUMN_B1", "potential_tables": ["TABLE_1", "TABLE_2"]},
            {"name": "COLUMN_B2", "potential_tables": ["TABLE_1", "TABLE_2"]},
            {"name": "ID", "potential_tables": ["TABLE_1"]},
            {"name": "ID", "potential_tables": ["TABLE_2"]},
            {"name": "DT", "potential_tables": ["TABLE_1"]},
            {"name": "DT", "potential_tables": ["TABLE_2"]},
        ],
        "expected_joins": [
            {
                "join_type": "INNER JOIN",
                "column_left": {
                    "name": "ID",
                    "potential_tables": ["TABLE_1"],
                },
                "column_right": {
                    "name": "ID",
                    "potential_tables": ["TABLE_2"],
                },
            }
        ],
        "expected_filters": [
            {
                "query": "TABLE_1.DT <= TABLE_2.DT",
                "filter_type": "WHERE",
                "operator": "<=",
                "columns": [
                    {"name": "DT", "potential_tables": ["TABLE_1"]},
                    {"name": "DT", "potential_tables": ["TABLE_2"]},
                ],
            }
        ],
    },
    {
        "name": "complex_join_query",
        "query": """
        SELECT
            t1.ID,
            t2.AGE
        FROM
            MY_TABLE t1
        INNER JOIN
            AGE_TBL t2
        ON
            t1.CUST_ID + 10 = t2.CUSTOMER_NB
        """,
        "expected_columns": [
            {"name": "ID", "potential_tables": ["MY_TABLE"]},
            {"name": "AGE", "potential_tables": ["AGE_TBL"]},
            {"name": "CUST_ID", "potential_tables": ["MY_TABLE"]},
            {"name": "CUSTOMER_NB", "potential_tables": ["AGE_TBL"]},
        ],
        "expected_joins": [
            {
                "join_type": "INNER JOIN",
                "column_left": {
                    "name": "CUST_ID",
                    "potential_tables": ["MY_TABLE"],
                },
                "column_right": {
                    "name": "CUSTOMER_NB",
                    "potential_tables": ["AGE_TBL"],
                },
                "complex_left": "t1.CUST_ID + 10",
            }
        ],
    },
    {
        "name": "complex_join_query_ambiguous_col",
        "query": """WITH CTE_A AS (
            SELECT
                CUSTOMER_ID,
                JOIN_COL
            FROM
                CTE_TABLE
        ),
        CTE_B AS (
            SELECT * FROM
            (SELECT * FROM CTE_A)
        )
        SELECT
            OTHER_ID,
            a.CUSTOMER_ID
        FROM CTE_B a
        LEFT JOIN OTHER_TABLE t
            ON a.JOIN_COL = t.JOIN_COL
        """,
        "expected_columns": [
            {"name": "CUSTOMER_ID", "potential_tables": ["CTE_TABLE"]},
            {"name": "JOIN_COL", "potential_tables": ["CTE_TABLE"]},
            {"name": "OTHER_ID", "potential_tables": ["CTE_TABLE", "OTHER_TABLE"]},
            {"name": "JOIN_COL", "potential_tables": ["OTHER_TABLE"]},
        ],
        "expected_joins": [
            {
                "join_type": "LEFT JOIN",
                "column_left": {
                    "name": "JOIN_COL",
                    "potential_tables": ["CTE_TABLE"],
                },
                "column_right": {
                    "name": "JOIN_COL",
                    "potential_tables": ["OTHER_TABLE"],
                },
            }
        ],
    },
    {
        "name": "recent_balance_bucket_join",
        "query": """
        WITH recent AS (
            SELECT account_id, balance
            FROM core.accounts
            WHERE updated_at >= DATEADD('day', -7, CURRENT_DATE)
        )
        SELECT
            r.account_id,
            b.balance_bucket
        FROM recent r
        JOIN analytics.balance_lookup b
            ON r.balance + 10 = b.bucket_start
        """,
        "expected_columns": [
            {"name": "account_id", "potential_tables": ["core.accounts"]},
            {"name": "balance", "potential_tables": ["core.accounts"]},
            {"name": "updated_at", "potential_tables": ["core.accounts"]},
            {
                "name": "balance_bucket",
                "potential_tables": ["analytics.balance_lookup"],
            },
            {"name": "bucket_start", "potential_tables": ["analytics.balance_lookup"]},
        ],
        "expected_joins": [
            {
                "join_type": "INNER JOIN",
                "column_left": {
                    "name": "balance",
                    "potential_tables": ["core.accounts"],
                },
                "column_right": {
                    "name": "bucket_start",
                    "potential_tables": ["analytics.balance_lookup"],
                },
                "complex_left": "r.balance + 10",
            }
        ],
        "expected_filters": [
            {
                "query": "updated_at >= DATEADD(DAY, -7, CURRENT_DATE)",
                "filter_type": "WHERE",
                "operator": ">=",
                "columns": [
                    {
                        "name": "updated_at",
                        "potential_tables": ["core.accounts"],
                    }
                ],
            }
        ],
    },
    {
        "name": "summed_field_lineage",
        "query": """
        SELECT
            SUMMED_FIELD
        FROM
            (SELECT
                a.A + b.B AS SUMMED_FIELD
            FROM
                MYDB.MYSCHEMA.TABLE_A a
            JOIN
                MYDB.MYSCHEMA.TABLE_B b
            ON a.ID = b.ID
            )
        WHERE
            SUMMED_FIELD > 10;
        """,
        "expected_columns": [
            {"name": "A", "potential_tables": ["MYDB.MYSCHEMA.TABLE_A"]},
            {"name": "B", "potential_tables": ["MYDB.MYSCHEMA.TABLE_B"]},
            {"name": "ID", "potential_tables": ["MYDB.MYSCHEMA.TABLE_A"]},
            {"name": "ID", "potential_tables": ["MYDB.MYSCHEMA.TABLE_B"]},
        ],
        "expected_joins": [
            {
                "join_type": "INNER JOIN",
                "column_left": {
                    "name": "ID",
                    "potential_tables": ["MYDB.MYSCHEMA.TABLE_A"],
                },
                "column_right": {
                    "name": "ID",
                    "potential_tables": ["MYDB.MYSCHEMA.TABLE_B"],
                },
            }
        ],
        "expected_filters": [
            {
                "query": "SUMMED_FIELD > 10",
                "filter_type": "WHERE",
                "operator": ">",
                "columns": [
                    {
                        "name": "SUMMED_FIELD",
                        "potential_tables": [
                            "MYDB.MYSCHEMA.TABLE_A",
                            "MYDB.MYSCHEMA.TABLE_B",
                        ],
                    }
                ],
            }
        ],
    },
    {
        "name": "subquery_with_group_by",
        "query": """
        SELECT
            COUNT(1)
        FROM
            (SELECT
                A.MY_COL,
                CASE WHEN OTHER_COL = 1 THEN '1' ELSE '0' END AS ANOTHER_THING
            FROM MYDB.MYSCHEMA.MY_TABLE A
            ) A
        GROUP BY 1
        """,
        "expected_columns": [
            {"name": "MY_COL", "potential_tables": ["MYDB.MYSCHEMA.MY_TABLE"]},
            {"name": "OTHER_COL", "potential_tables": ["MYDB.MYSCHEMA.MY_TABLE"]},
        ],
        "expected_joins": [],
    },
    {
        "name": "aggregate_alias_in_having",
        "query": """
        SELECT
            A_CNT, COUNT(EP_ID) CUST_CNT
        FROM (
            SELECT
                EP_ID, COUNT(DISTINCT ADN) A_CNT
            FROM
                MYDB.MYSCHEMA.MYTABLE
            WHERE END_DT > dateadd(month, -6, current_date )
            GROUP BY 1
            HAVING A_CNT > 2
        )
        GROUP BY 1
        """,
        "expected_columns": [
            {"name": "ADN", "potential_tables": ["MYDB.MYSCHEMA.MYTABLE"]},
            {"name": "EP_ID", "potential_tables": ["MYDB.MYSCHEMA.MYTABLE"]},
            {"name": "END_DT", "potential_tables": ["MYDB.MYSCHEMA.MYTABLE"]},
        ],
        "expected_joins": [],
        "expected_filters": [
            {
                "query": "END_DT > DATEADD(MONTH, -6, CURRENT_DATE)",
                "filter_type": "WHERE",
                "operator": ">",
                "columns": [
                    {"name": "END_DT", "potential_tables": ["MYDB.MYSCHEMA.MYTABLE"]}
                ],
            },
            {
                "query": "A_CNT > 2",
                "filter_type": "HAVING",
                "operator": ">",
                "columns": [
                    {"name": "A_CNT", "potential_tables": ["MYDB.MYSCHEMA.MYTABLE"]}
                ],
            },
        ],
        "expected_filter_lineage": [
            {
                "query": "A_CNT > 2",
                "column": {
                    "name": "A_CNT",
                    "potential_tables": ["MYDB.MYSCHEMA.MYTABLE"],
                },
                "depends_on": [
                    {"name": "ADN", "potential_tables": ["MYDB.MYSCHEMA.MYTABLE"]}
                ],
            }
        ],
    },
    {
        "name": "filter_operator_variety",
        "query": """
        SELECT
            customer_id,
            SUM(balance) AS total_balance
        FROM analytics.sales s
        WHERE status IN ('A','B')
          AND balance BETWEEN 10 AND 20
          AND promo_code LIKE 'NY%'
          AND deleted IS NULL
          AND EXISTS (
                SELECT 1
                FROM analytics.audit a
                WHERE a.customer_id = s.customer_id
            )
        GROUP BY 1
        HAVING COUNT(*) > 1
        """,
        "expected_columns": [
            {"name": "customer_id", "potential_tables": ["analytics.sales"]},
            {"name": "customer_id", "potential_tables": ["analytics.audit"]},
            {"name": "balance", "potential_tables": ["analytics.sales"]},
            {"name": "deleted", "potential_tables": ["analytics.sales"]},
            {"name": "promo_code", "potential_tables": ["analytics.sales"]},
            {"name": "status", "potential_tables": ["analytics.sales"]},
        ],
        "expected_joins": [],
        "expected_filters": [
            {
                "query": "status IN ('A', 'B')",
                "filter_type": "WHERE",
                "operator": "IN",
                "columns": [
                    {"name": "status", "potential_tables": ["analytics.sales"]}
                ],
            },
            {
                "query": "balance BETWEEN 10 AND 20",
                "filter_type": "WHERE",
                "operator": "BETWEEN",
                "columns": [
                    {"name": "balance", "potential_tables": ["analytics.sales"]}
                ],
            },
            {
                "query": "promo_code LIKE 'NY%'",
                "filter_type": "WHERE",
                "operator": "LIKE",
                "columns": [
                    {"name": "promo_code", "potential_tables": ["analytics.sales"]}
                ],
            },
            {
                "query": "deleted IS NULL",
                "filter_type": "WHERE",
                "operator": "IS",
                "columns": [
                    {"name": "deleted", "potential_tables": ["analytics.sales"]}
                ],
            },
            {
                "query": "EXISTS(SELECT 1 FROM analytics.audit AS a WHERE a.customer_id = s.customer_id)",
                "filter_type": "WHERE",
                "operator": "EXISTS",
                "columns": [
                    {"name": "customer_id", "potential_tables": ["analytics.audit"]},
                    {"name": "customer_id", "potential_tables": ["analytics.sales"]},
                ],
            },
            {
                "query": "COUNT(*) > 1",
                "filter_type": "HAVING",
                "operator": ">",
                "columns": [],
            },
            {
                "query": "a.customer_id = s.customer_id",
                "filter_type": "WHERE",
                "operator": "=",
                "columns": [
                    {"name": "customer_id", "potential_tables": ["analytics.audit"]},
                    {"name": "customer_id", "potential_tables": ["analytics.sales"]},
                ],
            },
        ],
    },
]


class QueryParserTests(unittest.TestCase):
    def _normalize_columns(self, columns):
        normalized = []
        for column in columns:
            normalized.append(
                {
                    "name": column["name"],
                    "potential_tables": sorted(column["potential_tables"]),
                }
            )
        return sorted(
            normalized, key=lambda col: (col["name"], tuple(col["potential_tables"]))
        )

    def _normalize_join_entry(self, join):
        def _extract(column):
            if hasattr(column, "col_name"):
                name = column.col_name
                tables = list(column.potential_tables)
            else:
                name = column["name"]
                tables = list(column["potential_tables"])
            return {"name": name, "potential_tables": sorted(tables)}

        normalized = {
            "join_type": join["join_type"],
            "column_left": _extract(join["column_left"]),
            "column_right": _extract(join["column_right"]),
        }
        complex_left = join.get("complex_left")
        complex_right = join.get("complex_right")
        if complex_left:
            normalized["complex_left"] = complex_left
        if complex_right:
            normalized["complex_right"] = complex_right
        return normalized

    def _normalize_joins(self, joins):
        normalized = [self._normalize_join_entry(join) for join in joins]
        return sorted(
            normalized,
            key=lambda join: (
                join["join_type"],
                join["column_left"]["name"],
                tuple(join["column_left"]["potential_tables"]),
                join["column_right"]["name"],
                tuple(join["column_right"]["potential_tables"]),
                join.get("complex_left") or "",
                join.get("complex_right") or "",
            ),
        )

    def _normalize_lineage_columns(self, lineage):
        normalized = []
        for column in lineage or []:
            entry = {
                "name": column.col_name,
                "potential_tables": sorted(column.potential_tables),
            }
            nested = self._normalize_lineage_columns(column.lineage)
            if nested:
                entry["lineage"] = nested
            normalized.append(entry)
        return sorted(
            normalized,
            key=lambda col: (
                col["name"],
                tuple(col["potential_tables"]),
                tuple(col.get("lineage") or []),
            ),
        )

    def _normalize_filter_column(self, column):
        if hasattr(column, "col_name"):
            name = column.col_name
            tables = sorted(column.potential_tables)
        else:
            name = column["name"]
            tables = sorted(column["potential_tables"])
        return {"name": name, "potential_tables": tables}

    def _normalize_filters(self, filters):
        normalized = []
        for entry in filters:
            normalized_entry = {
                "query": entry["query"],
                "filter_type": entry["filter_type"],
                "operator": entry["operator"],
                "columns": [
                    self._normalize_filter_column(column) for column in entry["columns"]
                ],
            }
            normalized_entry["columns"].sort(
                key=lambda col: (col["name"], tuple(col["potential_tables"]))
            )
            normalized.append(normalized_entry)

        return sorted(
            normalized,
            key=lambda item: (
                item["filter_type"],
                item["operator"],
                item["query"],
                tuple(
                    (col["name"], tuple(col["potential_tables"]))
                    for col in item["columns"]
                ),
            ),
        )

    def _normalize_select_columns(self, columns):
        normalized = []
        for entry in columns:
            column = entry["column"]
            normalized.append(
                {
                    "name": column.col_name,
                    "potential_tables": sorted(column.potential_tables),
                    "direct": entry["direct"],
                }
            )
        return sorted(
            normalized,
            key=lambda col: (
                col["name"],
                tuple(col["potential_tables"]),
                col["direct"],
            ),
        )

    def test_queries_against_expectations(self):
        for case in TEST_CASES:
            with self.subTest(case=case["name"]):
                parser = QueryParser(case["query"])
                raw_filters = parser.filters()
                expected_columns = self._normalize_columns(case["expected_columns"])
                actual_columns = self._normalize_columns(parser.feature_columns())
                self.assertEqual(
                    expected_columns,
                    actual_columns,
                    f"Column mismatch for case {case['name']}",
                )

                expected_joins = self._normalize_joins(case["expected_joins"])
                actual_joins = self._normalize_joins(parser.joins())
                self.assertEqual(
                    expected_joins,
                    actual_joins,
                    f"Join mismatch for case {case['name']}",
                )

                if "expected_filters" in case:
                    expected_filters = self._normalize_filters(case["expected_filters"])
                    actual_filters = self._normalize_filters(raw_filters)
                    self.assertEqual(
                        expected_filters,
                        actual_filters,
                        f"Filter mismatch for case {case['name']}",
                    )

                if "expected_filter_lineage" in case:
                    filter_lookup = {entry["query"]: entry for entry in raw_filters}
                    for expectation in case["expected_filter_lineage"]:
                        target_filter = filter_lookup.get(expectation["query"])
                        self.assertIsNotNone(
                            target_filter,
                            f"Missing filter for lineage assertion: {expectation['query']}",
                        )
                        column_spec = expectation["column"]
                        target_column = None
                        for column in target_filter["columns"]:
                            if column.col_name == column_spec["name"] and sorted(
                                column.potential_tables
                            ) == sorted(column_spec["potential_tables"]):
                                target_column = column
                                break
                        self.assertIsNotNone(
                            target_column,
                            f"Missing column {column_spec['name']} in filter {expectation['query']}",
                        )
                        actual_lineage = [
                            {
                                "name": entry["name"],
                                "potential_tables": entry["potential_tables"],
                            }
                            for entry in self._normalize_lineage_columns(
                                target_column.lineage
                            )
                        ]
                        expected_lineage = self._normalize_columns(
                            expectation["depends_on"]
                        )
                        self.assertEqual(
                            expected_lineage,
                            actual_lineage,
                            f"Filter lineage mismatch for column {column_spec['name']} in case {case['name']}",
                        )

    def test_source_tables(self):
        query = """
        SELECT *
        FROM CORE.ACCOUNTS a
        JOIN CORE.ACCOUNT_LOOKUP b ON a.ID = b.ID
        LEFT JOIN analytics.balance_lookup c ON b.ID = c.ID
        """
        parser = QueryParser(query)
        self.assertEqual(
            ["CORE.ACCOUNTS", "CORE.ACCOUNT_LOOKUP", "analytics.balance_lookup"],
            parser.source_tables(),
        )

    def test_subquery_star_lineage_and_source_columns(self):
        query = """
        SELECT outer_alias.*
        FROM (
            SELECT
                ID,
                AMOUNT,
                AMOUNT * 2 AS DOUBLED
            FROM SALES_DB.PUBLIC.TABLE_A
        ) outer_alias
        """
        parser = QueryParser(query)
        expected_columns = self._normalize_columns(
            [
                {"name": "ID", "potential_tables": ["SALES_DB.PUBLIC.TABLE_A"]},
                {"name": "AMOUNT", "potential_tables": ["SALES_DB.PUBLIC.TABLE_A"]},
                {"name": "DOUBLED", "potential_tables": ["SALES_DB.PUBLIC.TABLE_A"]},
            ]
        )
        actual_columns = self._normalize_columns(parser.feature_columns())
        self.assertEqual(expected_columns, actual_columns)

        lineage = parser.column_lineage()
        self.assertIn("outer_alias", lineage)
        self.assertEqual(
            {
                "ID": ["SALES_DB.PUBLIC.TABLE_A"],
                "AMOUNT": ["SALES_DB.PUBLIC.TABLE_A"],
                "DOUBLED": ["SALES_DB.PUBLIC.TABLE_A"],
            },
            lineage["outer_alias"],
        )

    def test_nested_filter_lineage_preserved(self):
        query = """
        WITH base AS (
            SELECT amount FROM sales.orders
        ),
        cte AS (
            SELECT amount AS inner_amount FROM base
        ),
        final AS (
            SELECT inner_amount AS outer_amount FROM cte
        )
        SELECT outer_amount
        FROM final
        HAVING outer_amount > 10
        """
        parser = QueryParser(query)
        target_filter = next(
            entry for entry in parser.filters() if entry["query"] == "outer_amount > 10"
        )
        target_column = next(
            column
            for column in target_filter["columns"]
            if column.col_name == "outer_amount"
        )
        normalized_lineage = self._normalize_lineage_columns(target_column.lineage)
        names = {entry["name"] for entry in normalized_lineage}
        self.assertIn("inner_amount", names)
        inner_entry = next(
            entry for entry in normalized_lineage if entry["name"] == "inner_amount"
        )
        inner_dependencies = inner_entry.get("lineage") or []
        self.assertTrue(
            any(child.get("name") == "amount" for child in inner_dependencies),
            "inner_amount should depend on base amount column",
        )
        outer_entry = next(
            (entry for entry in normalized_lineage if entry["name"] == "outer_amount"),
            None,
        )
        if outer_entry:
            outer_dependencies = outer_entry.get("lineage") or []
            self.assertTrue(
                any(
                    child.get("name") == "inner_amount" for child in outer_dependencies
                ),
                "outer_amount should depend on inner_amount",
            )

    def test_select_columns_direct_and_expression_lineage(self):
        direct_query = """
        SELECT
            A,
            B
        FROM TABLE_B
        WHERE C > 10
        """
        parser = QueryParser(direct_query)
        direct_columns = self._normalize_select_columns(parser.select_columns())
        self.assertEqual(
            [
                {"name": "A", "potential_tables": ["TABLE_B"], "direct": True},
                {"name": "B", "potential_tables": ["TABLE_B"], "direct": True},
            ],
            direct_columns,
        )

        derived_query = """
        SELECT
            A + B AS D
        FROM TABLE_B
        WHERE C > 10
        """
        parser = QueryParser(derived_query)
        derived_columns = self._normalize_select_columns(parser.select_columns())
        self.assertEqual(
            [
                {"name": "A", "potential_tables": ["TABLE_B"], "direct": False},
                {"name": "B", "potential_tables": ["TABLE_B"], "direct": False},
            ],
            derived_columns,
        )

    def test_select_columns_cte_lineage_is_flattened(self):
        query = """
        WITH base AS (
            SELECT
                a,
                b,
                a + b AS sum_ab
            FROM TABLE_C
        )
        SELECT sum_ab FROM base
        """
        parser = QueryParser(query)
        columns = self._normalize_select_columns(parser.select_columns())
        self.assertEqual(
            [
                {"name": "a", "potential_tables": ["TABLE_C"], "direct": False},
                {"name": "b", "potential_tables": ["TABLE_C"], "direct": False},
            ],
            columns,
        )

    def test_set_operation_lineage_preserved(self):
        query = """
        WITH union_cte AS (
            SELECT id, amount FROM T1
            UNION ALL
            SELECT id, amount FROM T2
        )
        SELECT id, amount FROM union_cte
        """
        parser = QueryParser(query)
        columns = self._normalize_select_columns(parser.select_columns())
        self.assertEqual(
            [
                {"name": "amount", "potential_tables": ["T1", "T2"], "direct": True},
                {"name": "id", "potential_tables": ["T1", "T2"], "direct": True},
            ],
            columns,
        )

    def test_star_expansion_includes_derived_outputs(self):
        query = """
        SELECT *
        FROM (
            SELECT
                a,
                b,
                a + b AS sum_ab
            FROM SOURCE_TABLE
        )
        """
        parser = QueryParser(query)
        columns = self._normalize_columns(parser.feature_columns())
        self.assertEqual(
            [
                {"name": "a", "potential_tables": ["SOURCE_TABLE"]},
                {"name": "b", "potential_tables": ["SOURCE_TABLE"]},
                {"name": "sum_ab", "potential_tables": ["SOURCE_TABLE"]},
            ],
            columns,
        )


if __name__ == "__main__":
    unittest.main()
