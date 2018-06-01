import sys
from catalog import col, table, Database

database = Database()
database = database.load()


def show(query):
    print('The table name already exists in the database:')
    print(database.table_names)
    # print(database.tables)
    # new = Database()
    # new.load()
    # print('test load:\n ------------- ')
    # print(new.table_names)


show(' ')


def create(query):
    sql_list = query.split()
    if (sql_list[0] == 'create' and sql_list[1] == 'table'):
        table_name = sql_list[2]
        table_name = table_name.split('(')[0]
        print("table name:{}".format(table_name))

        start = query.find('(')
        end = query.rfind(')')
        table_col_info = query[start + 1:end]
        table_col_list = table_col_info.split(',')
        # create a table
        new_table = table(table_name=table_name)
        print(table_col_list)
        for i in range(len(table_col_list) - 1):
            col_list = table_col_list[i].split()
            # change the key word 'unique' to 1
            if col_list[-1] == 'unique':
                col_list[-1] = 1
            temp_col = col(*[value for value in col_list])
            new_table.add_col(temp_col)
        # check if the last col is primary key or not
        col_last_list = table_col_list[-1].split()
        if (col_last_list[0] != 'primary'):
            temp_col = col(*[value for value in col_last_list])
            new_table.add_col(temp_col)
        else:
            prim_index = query.find('primary key')
            if prim_index != -1:
                prim_index += 11
                end_index = query.find(',', prim_index)
                primary_key = query[prim_index:end_index].split()[0]
                primary_key = primary_key.split('(')[-1]
                primary_key = primary_key.split(')')[0]
                new_table.set_primary_key(primary_key)
            else:
                print('create SyntaxError')
                sys.exit(0)
        database.add_table(new_table)
        print(database)
        database.save()


def insert(query):
    '''
    example:
    insert into student values (‘12345678’,’wy’,22,’M’);
    TODO: 当前插入的时候，只能一次性插入所有为列
    '''
    into_index = query.find('into')
    values_index = query.find('values')
    if into_index == -1 or values_index == -1:
        print('insert SyntaxError')
        sys.exit(0)
    else:
        table_name = query[into_index + 4:values_index].strip()
        if table_name in database.table_names:
            # if table_name in database.keys():
            values_query = query[values_index + 6:-1]
            values_query.strip()
            values_query = values_query[1:-3]
            values_list = values_query.split(',')
            i = 0
            flag = False
            # TODO: 这里保存之后会比较慢，可能要改成局部保存
            database.save()
            for value in values_list:
                flag = database.tables[table_name].col_list[i].add_data(value)
                if (flag is True):
                    i = i + 1
                else:
                    database = database.load()
                    break
            if (flag is True):
                database.save()
            else:
                sys.exit(0)
        else:
            print('Cannot insert a nonexistent table')
            sys.exit(0)


def update(query):
    print('in the function update')
    pass


def drop(query):
    print('in the function drop')
    pass


def select(query):
    '''
    SELECT column_name,column_name
    FROM table_name
    [WHERE Clause]
    '''
    op_list = {'>=', '<=', '>', '<', '='}
    is_select_all = 0
    from_index = query.find('from')
    if from_index != -1:
        # parse the selected column
        select_index = query.find('select')
        select_index += 6
        select_col = query[select_index + 1:from_index]
        if (select_col.strip() == '*'):
            is_select_all = 1
            # print(is_select_all)
        else:
            select_obj = select_col.strip().split('(')[-1]
            select_obj = select_obj.split(')')[0]
            select_obj = select_obj.split(',')
            print(select_obj)
        # parse table_name
        remain_query = query[from_index + 4:]
        table_name = remain_query.strip().split()[0]
        # print(table_name)
        where_index = query.find('where')
        where_query = query[where_index + 5:-1].strip()
        # where_query.strip()
        # print(where_query)
        where_list = where_query.split('and')
        for caluse in where_list:
            find_op = 0
            if find_op == 0:
                for op in op_list:
                    op_index = caluse.find(op)
                    if (op_index != -1):
                        find_op = 1
                        front = query[where_index + 5:op_index]
                        behind = query[op_index + 1:-1]
                        # TODO: 增加 where语句的判断
                        judge()

    else:
        print('select SyntaxError')
        sys.exit(0)
