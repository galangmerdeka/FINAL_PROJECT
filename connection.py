import psycopg2 as pg
import constant as const_value

def Connection():
    conn = pg.connect(
        database = const_value.database,
        user = const_value.user,
        password = const_value.password,
        host = const_value.host_conn,
        port = const_value.port_conn
    )
    conn.autocommit = True

def ConnectionMysql():
    conn = pg.connect(
        database = const_value.mysql_database,
        user = const_value.mysql_user,
        password = const_value.mysql_password,
        host = const_value.mysql_host_conn,
        port = const_value.mysql_port_conn
    )
    conn.autocommit = True