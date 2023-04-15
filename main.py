import flask
from src.winePrj import GetAllInTable
from src.winePrj import InsertInTable
from src.winePrj import GetInTable
from src.winePrj import GetInTableAddQuery
from src.winePrj import UpdateInTable
from src.winePrj import DeleteInTable
from src.winePrj import UrlParser
from src.winePrj import CreateConnection

app = flask.Flask(__name__)


@app.route('/', defaults={'path': ''}, methods=['POST', 'GET', 'PUT', 'DELETE'])
@app.route('/<path:path>', methods=['POST', 'GET', 'PUT', 'DELETE'])
def process_request(path):
    connection = CreateConnection().run()
    cursor = connection.cursor()
    vars_return = UrlParser().run(flask.request.url, cursor)
    if vars_return is None:
        result = {'code': 501,
                  'name': 'Not Implemented',
                  'description': 'Not allowed - Invalid endpoint'
                  }
        return result, 501
    else:
        table_from_url = vars_return[0][0]
        if len(vars_return[0]) > 1:
            zid_from_url = vars_return[0][1]
        else:
            zid_from_url = None
        if len(vars_return) > 1:
            zquery_from_url = vars_return[1]
        else:
            zquery_from_url = None
        method = flask.request.method
        if method == 'POST':
            if zid_from_url is not None:
                result = {'code': 501,
                          'name': 'Not Implemented',
                          'description': 'POST method not allowed with parameters in URL'
                          }
                return result, 501
            else:
                new_item_json = flask.request.get_json()
                new_item = InsertInTable().run(table_from_url, new_item_json, cursor, connection)
                return new_item
        elif method == 'GET':
            if zquery_from_url is not None:
                item = GetInTableAddQuery().run(table_from_url, zquery_from_url, cursor)
                return flask.jsonify(item)
            elif zid_from_url is not None:
                item = GetInTable().run(table_from_url, zid_from_url, cursor)
                return flask.jsonify(item)
            else:
                items = GetAllInTable().run(table_from_url, cursor)
                return flask.jsonify(items)
        elif method == 'PUT':
            if zid_from_url is None:
                result = {'code': 501,
                          'name': 'Not Implemented',
                          'description': 'PUT method not allowed with no parameters in URL'
                          }
                return result, 501
            else:
                update_item_json = flask.request.get_json()
                item_update = UpdateInTable().run(table_from_url, zid_from_url, update_item_json, cursor, connection)
                return item_update
        else:
            if zid_from_url is None:
                result = {'code': 501,
                          'name': 'Not Implemented',
                          'description': 'DELETE method not allowed with no parameters in URL'
                          }
                return result, 501
            else:
                item_delete = DeleteInTable().run(table_from_url, zid_from_url, cursor, connection)
                return item_delete
    connection.close()


app.run(port=5000, host='localhost', debug=True)
