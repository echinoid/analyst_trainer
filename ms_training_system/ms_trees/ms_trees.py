import ms_training_system.common.lib_db_operations as pgdb
from flask import Flask, request
from flask_cors import CORS
from flask_graphql import GraphQLView
import graphene
from graphene import ObjectType, String, Int, Field, List

TREE_TABLE = 'trees'
TREE_TYPE_TABLE = 'tree_types'

# Задаём параметры для flask
app = Flask(__name__)
CORS(app)
app.config['JSON_AS_ASCII'] = False
app.config['JSON_SORT_KEYS'] = False

# Создаем подключение к БД
connect = pgdb.Connector()
dboperation = pgdb.DbDataOperations(connect)
loader = pgdb.DbAdvancedOperations(connect)


# Определение GraphQL типа для trees
class TreeType(graphene.ObjectType):
    id = Int()
    type = Int()
    age = Int()
    comment = String()


# Определение GraphQL типа для tree_types
class TreeTypeType(graphene.ObjectType):
    id = Int()
    name = String()
    comment = String()


@app.route('/trees', methods=['GET', 'POST', 'PUT'])
def all_trees():
    tree_type = request.args.get('type')
    tree_id = request.args.get('id')
    if request.method == "GET":
        if tree_type:
            condition = f"type = '{tree_type}'"
        else:
            condition = None
        res: dict = dboperation.data_select(TREE_TABLE, condition=condition, result='dict')
        return {
            'state': 'OK',
            'result': res
        }
    elif request.method == "POST":
        res = dboperation.data_insert(TREE_TABLE, request.json, extensions='RETURNING id', result='fetchone')
        tree_id = res[0]
        return {
            'state': 'OK',
            'tree_id': tree_id
        }
    elif request.method == "PUT":
        if tree_id:
            condition = f"type = '{tree_id}'"
        else:
            condition = None
        dboperation.data_update(TREE_TABLE, request.json, condition)
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

    elif request.method == "GET":
        res: dict = dboperation.data_select(TREE_TABLE, condition=condition, result='dict')
        return {
            'state': 'OK',
            'result': res
        }
    elif request.method == "PUT":
        dboperation.data_update(TREE_TABLE, request.json, condition)
        return {
            'state': 'OK',
            'tree_id': element_id
        }
    elif request.method == "DELETE":
        dboperation.data_delete(TREE_TABLE, condition)
        return {
            'state': 'OK',
            'tree_id': element_id
        }


# Определение операций для работы с trees
class TreeQuery(ObjectType):
    trees = List(TreeType, id=Int())
    tree = Field(TreeType, id=Int())

    def resolve_trees(self, info):
        res: dict = dboperation.data_select(TREE_TABLE, result='dict')
        return res


class TreeMutation(ObjectType):
    create_tree = Field(TreeType, tree_type=Int(), age=Int(), comment=String())

    def resolve_create_tree(self, info, type, age, comment):
        # Ваша логика для создания нового дерева
        return None

    # Поддержите другие мутации, такие как обновление и удаление деревьев


# Определение операций для работы с tree_types
class TreeTypeQuery(ObjectType):
    tree_types = List(TreeTypeType)

    def resolve_tree_types(self, info):
        # Ваша логика для получения всех типов деревьев
        return []


# Объединение всех операций в одну схему
class Query(TreeQuery, TreeTypeQuery, graphene.ObjectType):
    pass


class Mutation(TreeMutation, graphene.ObjectType):
    pass


schema = graphene.Schema(query=Query, mutation=Mutation)

app.add_url_rule('/graphql', view_func=GraphQLView.as_view('graphql', schema=schema, graphiql=True))

if __name__ == "__main__":
    app.run(debug=True)
