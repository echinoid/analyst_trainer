import common.lib_db_operations as pgdb
from flask import Flask, request

# Задаём параметры для flask
app = Flask(__name__)
app.config['JSON_AS_ASCII'] = False
app.config['JSON_SORT_KEYS'] = False

# Создаем подключения к API и БД
connect = pgdb.Connector()
loader = pgdb.DbAdvancedOperations(connect)


@app.route('/trees', methods=['GET', 'POST'])
def all_trees():
    if request.method == "GET":
        # res = connect.execute('SELECT id, ')
        return {
            'сообщение': 'Настоящий API запрос должен возвращать'
                         ' список сущностей',
            'метод': request.method
        }
    if request.method == "POST":
        return {
            'сообщение': 'Данный API запрос обеспечивает создание'
                         ' дополнительной сущности',
            'дополнительная_сущность': request.json,
            'метод': request.method
        }

@app.route('/trees/<int:element_id>', methods=['GET', 'PUT', 'DELETE'])
def element(element_id):
    if request.method == "GET":
        return {
            'id': element_id,
            'сообщение': 'Настоящий API запрос возвращает информацию об'
                         ' отдельной сущности {}'.format(element_id),
            'метод': request.method
        }
    if request.method == "PUT":
        return {
            'id': element_id,
            'сообщение': 'Данный API запрос обновляет отдельную сущность {}'
            .format(element_id),
            'метод': request.method,
            'body': request.json
        }
    if request.method == "DELETE":
        return {
            'id': element_id,
            'сообщение': 'Настоящий API запрос удаляет отдельную сущность {}'
            .format(element_id),
            'метод': request.method
        }


if __name__ == "__main__":
    app.run(debug=True)
