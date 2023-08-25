import os

import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

load_dotenv()
database_host = os.getenv('DATASOURCE_URL')
database_port = os.getenv('DATASOURCE_PORT')
database_user = os.getenv('DATASOURCE_USERNAME')
database_password = os.getenv('DATASOURCE_PASSWORD')
database_application_name = "process-name"


def get_cursor_by_database_name(database_name):
    connection = psycopg2.connect(database=database_name,
                                  user=database_user,
                                  password=database_password,
                                  host=database_host,
                                  port=database_port,
                                  application_name=database_application_name)
    return connection, connection.cursor(cursor_factory=psycopg2.extras.DictCursor)


def get_info():
    database_connection, table_cursor = get_cursor_by_database_name("database-name")
    table_cursor.execute("select column from table")
    info = table_cursor.fetchall()
    table_cursor.close()
    database_connection.close()

    return info


def update(info):
    db_connection, table_cursor = get_cursor_by_database_name("database-name")
    for item in info:
        item_1 = item[0]
        table_cursor.execute(f"update table set "
                             f"field='{item_1}' "
                             f"where item_1='{item_1}'")

    db_connection.commit()
    table_cursor.close()
    db_connection.close()
    print("update finished")


def main():
    info = get_info()
    update(info)


if __name__ == '__main__':
    print("script started")
    main()
    print("script finished")
