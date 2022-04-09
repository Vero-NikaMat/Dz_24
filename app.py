import os
import re
from typing import Any, Iterator, List

from flask import Flask, request, Response
from werkzeug.exceptions import BadRequest

app = Flask(__name__)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")

def build_query(fd: Iterator, query: str)-> Iterator:
    query_items = query.split("|")
    res = iter(map(lambda v: v.strip(), fd))
    print(res)
    print(query_items)
    for item in query_items:
        res=get_data(res, item)
    return res

def slice_limit(it: Iterator, limit: int) -> Iterator:
    i = 0
    for item in it:
        if i < limit:
            yield item
        else:
            break
        i += 1

def get_data(res: Iterator, item: str)-> Iterator:
    split_item = item.split(":")
    cmd = split_item[0]
    if cmd == "filter":
        return filter(lambda v: split_item[1] in v, res)
    if cmd == "map":
        return map(lambda v: v.split(" ")[int(split_item[1])], res)
    if cmd == "unique":
        return iter(set(res))
    if cmd == "sort":
        arg = split_item[1]
        if arg == "desc":
            reverse = True
        else:
            reverse = False
        return iter(sorted(res, reverse=reverse))
    if cmd == "limit":
        return slice_limit(res, int(split_item[1]))
    if cmd =='regex':
        regex = re.compile(split_item[1])
        return filter(lambda v: regex.search(v), res)
    return res

@app.post("/perform_query")
# добавить команду regex
# добавить типизацию в проект, чтобы проходила утилиту mypy app.py
def perform_query()-> Response:
    try:
        query = request.args["query"]
        file_name = request.args["file_name"]
    except KeyError:
        raise BadRequest

    file_path = os.path.join(DATA_DIR, file_name)
    if not os.path.exists(file_path):
        return Response(f"{file_name} was not found")

    with open(file_path) as fd:
        res = build_query(fd, query)
        content = '\n'.join(res)
        print(content)

    return app.response_class(content, content_type="text/plain")

if __name__ == '__main__':
    app.run(debug=True)
