import sys
import record
import record

def process(query):
    parse_query(query)


def parse_query(query):
    func_dict = {
        'select': record.select,
        'drop':  record.drop,
        'create': record.create,
        'insert': record.insert,
        # 'update': record.update,
        'show': record.show
    }
    query = query.strip()
    sql_list = query.split()
    func = sql_list[0]
    if (func == 'quit' or func == 'exit'):
        print('Bye ~')
        sys.exit(0)
    func_dict.get(func, lambda: 'Invalid')(query)
