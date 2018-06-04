import sys
import prettytable as pt
from catalog import col, table, Database

database = Database()
database = database.load()


def show(query):
    # print('The table name already exists in the database:')
    global database
    print('tables:{}'.format(database.table_names))
    # for i in range()
    # print('data:{}'.format(database.tables[0].))
    # print(database.tables)
    # new = Database()
    # new.load()
    # print('test load:\n ------------- ')
    # print(new.table_names)


show(' ')


def create(query):
    global database
    sql_list = query.split()
    if (sql_list[0] == 'create' and sql_list[1] == 'table'):
        table_name = sql_list[2]
        table_name = table_name.split('(')[0]
        # print("table name:{}".format(table_name))

        start = query.find('(')
        end = query.rfind(')')
        table_col_info = query[start + 1:end]
        table_col_list = table_col_info.split(',')
        # create a table
        new_table = table(table_name=table_name)
        # print(table_col_list)
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
        # print(database)
        database.save()


def insert(query):
    '''
    example:
    insert into student values (‘12345678’,’wy’,22,’M’);
    TODO: 当前插入的时候，只能一次性插入所有为列
    '''
    global database
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
            values_query = values_query[1:-2]
            # values_list = values_query.replace('\'', '').replace('\"',
            #                                                      '').split(',')
            values_list = values_query.split(',')
            print("value list :{}".format(values_list))
            for value in values_list:
                value.strip()
                value = eval(value)
                print(value)
                # print(value)
                # value.replace('\'', '')
                # print(value)
                # value.replace('\"', '')
                # print(value)
            col_i = 0
            flag = False
            # TODO: 这里保存之后会比较慢，可能要改成局部保存
            database.save()
            # FIXME: 传入是空值的时候会报错,加一个空值的判断
            for value in values_list:
                flag = database.tables[table_name].col_list[col_i].add_data(
                    value)

                if (flag is True):
                    col_i = col_i + 1
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


def drop(query):
    global database
    key = query.strip().split()[1]
    if key == "table":
        table_name = query.strip().split()[2].rstrip(';')
        print(table_name)
        database.drop_table(table_name)
        database.save()
    elif key == "index":
        pass


def select(query):
    '''
    SELECT column_name,column_name
    FROM table_name
    [WHERE Clause]
    '''
    global database
    # op_list = {'>=', '<=', '>', '<', '='}
    res = []
    select_obj = []
    select_index = []
    # is_select_all = 0
    from_index = query.find('from')
    if from_index != -1:
        # parse the selected column
        remain_query = query[from_index + 4:]
        table_name = remain_query.strip().split()[0]
        try: 
            col_num = len(database.tables[table_name].col_list)
        except KeyError:
            print("No such table in database")
            sys.exit(0)
        # print(database.tables[table_name].col_list)
        # print(database.tables)
        select_index = query.find('select')
        select_index += 6
        select_col = query[select_index + 1:from_index]
        if (select_col.strip() == '*'):
            # is_select_all = 1
            select_index = range(col_num)
            for i in select_index:
                select_obj.append(
                    database.tables[table_name].col_list[i].col_name)
            # print(is_select_all)
        else:
            select_obj = select_col.strip().split('(')[-1]
            select_obj = select_obj.split(')')[0]
            select_obj = select_obj.split(',')
            for i in range(col_num):
                for v in range(select_obj):
                    if v == database.tables[table_name].col_list[i].col_name:
                        select_index.append(i)
                        break
        # print(select_obj)
        # print(select_index)
        # parse table_name
        # print(table_name)
        where_index = query.find('where')
        if where_index == -1:
            res = [
                database.tables[table_name].col_list[x].data
                for x in select_index
            ]
        else:
            where_query = query[where_index + 5:-1].strip()
            where_list = where_query.split('and')
            # TODO: 增加 where从语的判断
            for clause in where_list:
                op1_index = clause.find('<=')
                op2_index = clause.find('>=')
                op3_index = clause.find('<')
                op4_index = clause.find('>')
                op5_index = clause.find('=')
                if op1_index != -1:
                    attr = query[where_index + 5:op1_index].strip()
                    value = query[op1_index + 2:-1].strip()
                    index = (attr == database.tables[table_name].col_list[i]
                             .col_name for i in range(col_num))
                    res_i = 0
                    for v in database.tables[table_name].col_list[index].data:
                        if v <= value:
                            res.append(database.tables[table_name]
                                       .col_list[x].data[res_i]
                                       for x in select_index)
                    res_i = res_i + 1
                elif op2_index != -1:
                    attr = query[where_index + 5:op2_index].strip()
                    value = query[op2_index + 2:-1].strip()
                    index = (attr == database.tables[table_name].col_list[i]
                             .col_name for i in range(col_num))
                    res_i = 0
                    for v in database.tables[table_name].col_list[index].data:
                        if v >= value:
                            res.append(database.tables[table_name]
                                       .col_list[x].data[res_i]
                                       for x in select_index)
                    res_i = res_i + 1
                elif op3_index != -1:
                    attr = query[where_index + 5:op3_index].strip()
                    value = query[op3_index + 1:-1].strip()
                    index = (attr == database.tables[table_name].col_list[i]
                             .col_name for i in range(col_num))
                    res_i = 0
                    for v in database.tables[table_name].col_list[index].data:
                        if v < value:
                            res.append(database.tables[table_name]
                                       .col_list[x].data[res_i]
                                       for x in select_index)
                    res_i = res_i + 1
                elif op4_index != -1:
                    attr = query[where_index + 5:op4_index].strip()
                    value = query[op4_index + 1:-1].strip()
                    index = (attr == database.tables[table_name].col_list[i]
                             .col_name for i in range(col_num))
                    res_i = 0
                    for v in database.tables[table_name].col_list[index].data:
                        if v > value:
                            res.append(database.tables[table_name]
                                       .col_list[x].data[res_i]
                                       for x in select_index)
                    res_i = res_i + 1
                elif op5_index != -1:
                    attr = query[where_index + 5:op5_index].strip()
                    value = query[op5_index + 1:-1].strip()
                    index = (attr == database.tables[table_name].col_list[i]
                             .col_name for i in range(col_num))
                    res_i = 0
                    for v in database.tables[table_name].col_list[index].data:
                        if v == value:
                            res.append(database.tables[table_name]
                                       .col_list[x].data[res_i]
                                       for x in select_index)
                    res_i = res_i + 1
        # print(('-').join(select_obj))
        # print(value for value in res)

        tb = pt.PrettyTable()
        tb.field_names = select_obj
        # print(res)
        # print(select_index)
        # print(select_obj)
        num = len(res[0])
        # num = int(len(res[0]) / len(select_index))
        # print(num)
        # print(num)
        # print(res)
        # row  = []
        for i in range(num):
            row = []
            for x in range(len(select_obj)):
                row.append(res[x][i])
            tb.add_row(row)
        # for i in range(num):
        #     print(len(select_index))
        #     print( res[i * len(select_index):(i + 1) * len(select_index) - 1])
        # for i in range(num):
        #     tb.add_row(
        #         res[i * len(select_index):(i + 1) * len(select_index) - 1])
        print(tb)
    else:
        print('select SyntaxError')
        sys.exit(0)
