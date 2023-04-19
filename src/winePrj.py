import mysql.connector
from urllib.parse import urlparse, unquote



class CreateConnection:
    def run(self):
        try:
            conn = mysql.connector.connect(host='localhost', database='wine_4all', user='alexlpaz', password='Paulista@123')
            return conn
        except mysql.connector.Error as e:
            print('Error while connecting to MySQL: %s', e)


class InsertInTable:
    def run(self, table_from_url, json_str, cursor, connection):
        values = [list(x.values()) for x in json_str]
        columns = [list(x.keys()) for x in json_str][0]
        values_str = ""
        for i, record in enumerate(values):
            val_list = []
            for v, val in enumerate(record):
                if type(val) == str:
                    val = "'{}'".format(val.replace("'", "''"))
                val_list += [str(val)]
            values_str += "(" + ', '.join(val_list) + "),\n"
        values_str = values_str[:-2] + ";"
        sql_string = "INSERT INTO %s (%s)\nVALUES\n%s" % (
            table_from_url,
            ', '.join(columns),
            values_str
        )
        cursor.execute(sql_string)
        connection.commit()
        rows = cursor.rowcount
        result = {'code': 201,
                  'name': 'Created',
                  'description': 'Success: ' + str(rows) + ' rows inserted in table ' + str(table_from_url)
                  }
        return result, 201


class GetInTable:
    def run(self, table_from_url, id, cursor):
        sql_string = "SELECT * FROM " + str(table_from_url) + " WHERE id = %s"
        cursor.execute(sql_string, (id,))
        record = cursor.fetchall()
        row_headers = [x[0] for x in cursor.description]
        json_data = []
        for result in record:
            json_data.append(dict(zip(row_headers, result)))
        return json_data

class GetInTableAddQuery:
    def run(self, table_from_url, add_query, cursor):
        sql_string = "SELECT * FROM " + str(table_from_url) + " WHERE " + add_query
        cursor.execute(sql_string)
        record = cursor.fetchall()
        row_headers = [x[0] for x in cursor.description]
        json_data = []
        for result in record:
            json_data.append(dict(zip(row_headers, result)))
        return json_data

class GetAllInTable:
    def run(self, table_from_url, cursor):
        sql_string = "SELECT * FROM " + str(table_from_url)
        cursor.execute(sql_string)
        record = cursor.fetchall()
        row_headers = [x[0] for x in cursor.description]
        json_data = []
        for result in record:
            json_data.append(dict(zip(row_headers, result)))
        return json_data


class UpdateInTable:
    def run(self, table_from_url, id, json_str, cursor, connection):
        values = [list(x.values()) for x in json_str]
        columns = [list(x.keys()) for x in json_str][0]
        values_str = ""
        for i, record in enumerate(values):
            val_list = []
            for v, val in enumerate(record):
                if type(val) == str:
                    val = columns[v] + "='" + val + "'"
                val_list += [str(val)]
            values_str += "" + ', '.join(val_list) + "),"
        values_str = values_str[:-2]
        sql_string = "UPDATE %s SET %s WHERE id = %s" % (
            table_from_url,
            values_str, id
        )
        cursor.execute(sql_string)
        connection.commit()
        rows = cursor.rowcount
        cursor.close()
        connection.close()
        return 'Success: ' + str(rows) + ' rows updated in table ' + str(table_from_url)


class DeleteInTable:
    def run(self, table_from_url, id, cursor, connection):
        sql_string = "DELETE FROM " + str(table_from_url) + " WHERE id = %s"
        cursor.execute(sql_string, (id,))
        connection.commit()
        rows = cursor.rowcount
        cursor.close()
        connection.close()
        return 'Success: ' + str(rows) + ' rows deleted in table ' + str(table_from_url)


class UrlParser:
    def run(self, base_url, cursor):
        parts = urlparse(base_url)
        directories = parts.path.strip('/').split('/')
        cursor.execute("SHOW TABLES")
        table_return = ""
        for table_name in cursor:
            if " | ".join(table_name) == directories[0]:
                table_return = directories[0]
        if table_return == "":
            return None
        else:
            return_to_func = [directories]
            add_query = unquote(parts.query)
            if len(add_query) > 0:
                return_to_func.append(add_query)
            return return_to_func
