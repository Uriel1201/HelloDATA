import adbc_driver_sqlite.dbapi as dbapi
import polars as pol
import pyarrow as pa
import arrowkit

def main(table:str):

    conn = dbapi.connect("file:/content/my_SQLite.db?mode=ro")
    name = table.upper()
    if (name == "TRANSACTIONS_02"):

        try:

            arrow_transactions = arrowkit.get_ArrowTable(conn, table)
            transactions = pol.from_arrow(arrow_transactions).lazy()

            sample = transactions.head(5)
            print(f'TRANSACTIONS TABLE USING POLARS LAZYFRAMES -> SAMPLE:\n{sample.collect()}')
            _type = (transactions.unpivot(on = ['SENDER', 'RECEIVER'],
                                         index = 'AMOUNT',
                                         variable_name = 'TYPE',
                                         value_name = 'USER_ID'
                                 )
                                .with_columns(pol.when(pol.col('TYPE') == 'SENDER')
                                                 .then(pol.col('AMOUNT') * -1)
                                                 .otherwise(pol.col('AMOUNT'))
                                                 .alias('AMOUNT')
                                 )
                                .group_by(pol.col('USER_ID'))
                                .agg(pol.col('AMOUNT').sum())
                                .sort(by = 'AMOUNT', 
                                     descending = True
                                 )
            )
            print(":" * 40)
            print(f'NET CHANGES USING POLARS LAZYFRAMES:\n{_type.collect()}')

            query = """
                    WITH 
                        SENDERS AS (
                            SELECT
                                SENDER,
                                SUM(AMOUNT) AS SENDED
                            FROM 
                                TRANSACTIONS_02
                            GROUP BY
                                SENDER),
                        RECEIVERS AS (
                            SELECT
                                RECEIVER,
                                SUM(AMOUNT) AS RECEIVED
                            FROM
                                TRANSACTIONS_02
                            GROUP BY
                                RECEIVER) 
                    SELECT
                        COALESCE(S.SENDER, R.RECEIVER) AS USER_ID,
                        COALESCE(R.RECEIVED, 0) - COALESCE(S.SENDED, 0) AS NET_CHANGE
                    FROM
                        RECEIVERS R 
                    FULL JOIN 
                        SENDERS S 
                    ON 
                        (R.RECEIVER = S.SENDER)
                    ORDER BY 
                        2 DESC
            """
            result = arrowkit.get_ArrowTable(conn, query)
            df = result.to_pandas()
            print(":" * 40)
            print(f'NET CHANGES USING QUERIES:\n{result}')
            print(f'<*pandas visualization*>\n{df}')

        finally:

            conn.close()

    else:

        print(f'TABLE {table} NOT AVAILABLE')
