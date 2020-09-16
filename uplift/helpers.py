import pandas.io.sql as sqlio


def exec_query(conn, query):
    print(query)
    sqlio.execute(query, conn)


def make_dataframe(conn, query):
    print(query)
    df = sqlio.read_sql_query(
        query, con=conn)
    return df

