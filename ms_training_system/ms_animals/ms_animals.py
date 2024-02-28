import ms_training_system.common.lib_db_operations as pgdb
from flask import Flask, request
from flask_cors import CORS

TREE_TABLE = 'trees'
TREE_TYPE_TABLE = 'tree_types'

# Задаём параметры для flask
app = Flask(__name__)
CORS(app)
app.config['JSON_AS_ASCII'] = False
app.config['JSON_SORT_KEYS'] = False

# Создаем подключения к API и БД
connect = pgdb.Connector()
dboperation = pgdb.DbDataOperations(connect)
loader = pgdb.DbAdvancedOperations(connect)

@app.route('/trees', methods=['GET', 'POST'])
def all_trees():
    if request.method == "GET":
        res: dict = dboperation.data_select(TREE_TABLE, result='dict')
        return {
            'state': 'OK',
            'result': res
        }
    if request.method == "POST":
        res = dboperation.data_insert(TREE_TABLE, request.json, extensions='RETURNING id', result='fetchone')
        tree_id = res[0]
        return {
            'state': 'OK',
            'tree_id': tree_id
        }

@app.route('/trees/types', methods=['GET', 'POST'])
def all_tree_types():
    if request.method == "GET":
        res: dict = dboperation.data_select(TREE_TYPE_TABLE, result='dict')
        return {
            'state': 'OK',
            'result': res
        }

@app.route('/trees/<int:element_id>', methods=['GET', 'PUT', 'DELETE'])
def element(element_id):
    condition = f'id = {element_id}'
    # Проверяем что такой элемент существует
    res: dict = dboperation.data_select(TREE_TABLE, condition=condition, result='dict')
    if not res:
        return {
            'state': 'ERROR',
            'result': 'Такого дерева в лесу нет!'
        }

    if request.method == "GET":
        res: dict = dboperation.data_select(TREE_TABLE, condition=condition, result='dict')
        return {
            'state': 'OK',
            'result': res
        }
    if request.method == "PUT":
        dboperation.data_update(TREE_TABLE, request.json, condition)
        return {
            'state': 'OK',
            'tree_id': element_id
        }
    if request.method == "DELETE":
        dboperation.data_delete(TREE_TABLE, condition)
        return {
            'state': 'OK',
            'tree_id': element_id
        }


if __name__ == "__main__":
    app.run(debug=True)
