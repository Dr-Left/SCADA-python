from sqlalchemy import create_engine

import database_util

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
        database_util.create_table(conn1, ems_tables)
        # insert mocking data
        for rtu_id in range(1, 7):
            conn1.execute(
                f'insert into ems_rtu_info values({rtu_id}, "rtu{rtu_id}", "127.0.0.1", 66{rtu_id + 65}, 0, 0)')

    for rtu_id in range(1, 7):
        engine2 = create_engine(f"sqlite:///db/rtu{rtu_id}.db")
        with engine2.connect() as conn2:
            database_util.create_table(conn2, rtu_tables)
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
