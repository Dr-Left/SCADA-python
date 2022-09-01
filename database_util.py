"""
run this script to create databases for testing & debugging
"""
from sqlalchemy import create_engine

ems_tables = [
    {
        "name": "ems_rtu_info",
        "columns": ["id", "name", "ip_addr", "port", "status", "refresh_time"],
        "attribution": ["int", "varchar2(64)", "varchar2(64)", "int", "int", "int"],
        "primary keys": [1, 0, 0, 0, 0, 0]
    },
    {
        "name": "ems_yc_info",
        "columns": ["rtu_id", "pnt_no", "name", "value", "status", "refresh_time"],
        "attribution": ["int", "int", "varchar2(64)", "float", "int", "int"],
        "primary keys": [1, 1, 0, 0, 0, 0]
    },
    {
        "name": "ems_yx_info",
        "columns": ["rtu_id", "pnt_no", "name", "value", "status", "refresh_time"],
        "attribution": ["int", "int", "varchar2(64)", "float", "int", "int"],
        "primary keys": [1, 1, 0, 0, 0, 0]
    },
    {
        "name": "ems_yt_info",
        "columns": ["rtu_id", "pnt_no", "name", "value", "refresh_time", "ret_code"],
        "attribution": ["int", "int", "varchar2(64)", "float", "int", "int"],
        "primary keys": [1, 1, 0, 0, 0, 0]
    },
    {
        "name": "ems_yt_info",
        "columns": ["rtu_id", "pnt_no", "name", "value", "refresh_time", "ret_code"],
        "attribution": ["int", "int", "varchar2(64)", "float", "int", "int"],
        "primary keys": [1, 1, 0, 0, 0, 0]
    },
]

rtu_tables = [
    {
        "name": "rtu_info",
        "columns": ["id", "name", "ip_addr", "port", "status", "refresh_time"],
        "attribution": ["int", "varchar2(64)", "varchar2(64)", "int", "int", "int"],
        "primary keys": [1, 0, 0, 0, 0, 0]
    },
    {
        "name": "rtu_yc_info",
        "columns": ["id", "name", "value", "status", "refresh_time"],
        "attribution": ["int", "varchar2(64)", "float", "int", "int"],
        "primary keys": [1, 0, 0, 0, 0]
    },
    {
        "name": "rtu_yx_info",
        "columns": ["id", "name", "value", "status", "refresh_time"],
        "attribution": ["int", "varchar2(64)", "float", "int", "int"],
        "primary keys": [1, 0, 0, 0, 0]
    },
    {
        "name": "rtu_yt_info",
        "columns": ["id", "name", "value", "refresh_time", "ctrl_code"],
        "attribution": ["int", "varchar2(64)", "float", "int", "int"],
        "primary keys": [1, 0, 0, 0, 0]
    },
    {
        "name": "rtu_yk_info",
        "columns": ["id", "name", "value", "refresh_time", "ctrl_code"],
        "attribution": ["int", "varchar2(64)", "float", "int", "int"],
        "primary keys": [1, 0, 0, 0, 0]
    },
]


def work():
    # create
    engine1 = create_engine("sqlite:///db/ems.db")
    with engine1.connect() as conn1:
        create_table(conn1, ems_tables)
        # insert mocking data
        for rtu_id in range(1, 7):
            conn1.execute(
                f'insert into ems_rtu_info values({rtu_id}, "rtu{rtu_id}", "127.0.0.1", 66{rtu_id + 65}, 0, 0)')

    for rtu_id in range(1, 7):
        engine2 = create_engine(f"sqlite:///db/rtu{rtu_id}.db")
        with engine2.connect() as conn2:
            create_table(conn2, rtu_tables)
            conn2.execute(f'insert into rtu_info values({rtu_id}, "rtu{rtu_id}", "127.0.0.1", 66{rtu_id + 65}, 0, 0)')
            for operation in range(1, 5):
                records = []
                table = rtu_tables[operation]
                name = table["name"][4:6]  # yc
                column_cnt = len(table["columns"])
                for i in range(1, 7):
                    record = {"id": i, "name": name + f".rtu{rtu_id}.{i}", "value": 0.0, "status": 0, "refresh_time": 0}
                    records.append(record)
                # print(records)
                conn2.execute(f"insert into rtu_{name}_info values(:id, :name, :value, :status, :refresh_time)",
                              records)


def create_table(connection, tables):
    """
    constructs::

        engine = create_engine("sqlite:///db/ems.db")
            with engine.connect() as conn:
                database_util.create_table(conn, ems_tables)


    :param connection: sqlalchemy's database's connection
    :param tables: dictionary
        "name" : a str, table's name
        "columns" : a list of column names
        "attribution" : a list of the attribution of each column (int, text, etc.)
        "primary keys" : a list of ints, 1 shows the corresponding column is a primary key,
                                         0 shows it's not.
    :return: None

    """

    drop_lang = "drop table if exists %s"
    create_lang = "create table {0}({1}, primary key({2}))"
    for table in tables:
        size = len(table["columns"])
        connection.execute(drop_lang % table["name"])
        column_st = ", ".join([" ".join(x) for x in zip(table["columns"], table["attribution"])])
        primary_keys_st = ",".join([table["columns"][x] for x in range(size) if table["primary keys"][x]])
        final_sql_st = create_lang.format(table["name"], column_st, primary_keys_st)
        connection.execute(final_sql_st)


def get_server_addr(rtu_id, database_name, table_name):
    # establishing a connection to database
    engine = create_engine("sqlite:///db/" + database_name)
    with engine.connect() as conn:
        results = conn.execute(f"select ip_addr, port from {table_name} where id = {rtu_id}")
        for ip_addr, port in results:
            return ip_addr, port
        return None


if __name__ == "__main__":
    work()
