import six
import sys
from collections import OrderedDict
import pickle
import os


class col(dict):
    r'''
    class for col information

    @param:
    col_name: the name of the col
    attr: attribution, default is int
            Generally, we have 'int', 'char(n)' and 'float'
    is_unique: the data is unique or not
    '''

    def __init__(self, col_name=None, attr='int', is_unique=0, data=[]):
        super(col).__init__()
        self['col_name'] = col_name
        self['attr'] = attr
        self['is_unique'] = is_unique
        self.data = data

    def __getattr__(self, key):
        return super(col, self).get(key, None)

    def __setattr__(self, key, value):
        self[key] = value

    def set_attr(self, attr):
        self['attr'] = attr

    def set_is_unique(self, is_unique):
        self['is_unique'] = is_unique

    def set_col_name(self, col_name):
        self['col_name'] = col_name

    def add_data(self, data):
        if data in self.data and self.is_unique == 1:
            print(
                'Cannot insert a duplicate data when {} is setted \'unique\''.
                format(self['col_name']))
            return False
        else:
            self.data.append(data)
            return True


class table(dict):
    r'''
    class for tabel information

    @param:
    table_name: the name of table
    primary_key: primary key, if not exist, None
    col_list: a list containing col class (implemented above)
                    which covers the information of the col
    '''

    def __init__(self,
                 table_name=None,
                 primary_key=None,
                 col_list=[],
                 col_index=[],
                 data=[]):
        super(table).__init__()
        self['table_name'] = table_name
        self['primary_key'] = primary_key
        self['col_index'] = col_index
        self['col_list'] = col_list

    def __getattr__(self, key):
        return super(table, self).get(key, None)

    def __setattr__(self, key, value):
        self[key] = value

    # Rebuild __str__(), for checking the contents of the table
    def __str__(self):
        table_str = ''
        for key, val in six.iteritems(self):
            if table_str:
                table_str += '\n'
            table_str += key + '=' + str(val)
        return self.__class__.__name__ + '\n' + table_str

    def set_table_name(self, table_name):
        self['table_name'] = table_name

    def set_primary_key(self, key):
        self['primary_key'] = key

    def add_col(self, _col):
        if _col['col_name'] not in self['col_index']:
            self['col_index'].append(_col['col_name'])
            self['col_list'].append(_col)
        else:
            print('Column Redundant')
            sys.exit(0)

    def drop_col(self, _col):
        if _col['col_name'] in self['col_index']:
            del self['col_index'][_col['col_name']]
            del self['col_list'][_col]
        else:
            print('cannot drop a col which does not exist')
            sys.exit(0)


# class Database(dict):
#     def __init__(self):
#         super(Database).__init__()
#         # self.table_names = table_names

#     def add_table(self, _table):
#         # if _table.table_name in self.table_names:
#         if _table['table_name'] in self.keys():
#             print(
#                 "Cannot have table_names with the same names. RedundancyError")
#             sys.exit(0)
#         else:
#             # self.table_names.append(_table.table_name)
#             self[_table['table_name']] = _table

#     def drop_table(self, _table):
#         # if _table.table_name not in self.table_names:
#         if _table['table_name'] not in self.keys():
#             print("Cannot find table: {} in database".format(
#                 _table['table_name']))
#             sys.exit(0)
#         else:
#             # def self.table_names[_table.table_name]
#             del self[_table['table_name']]

#     def save(self):
#         with open('database/data.pickle', 'wb') as file:
#             pickle.dump(self.__dict__, file, 1)

#     def load(self):
#         os.makedirs('./database', exist_ok=True)
#         try:
#             with open('database/data.pickle', 'rb') as file:
#                 temp_dict = pickle.load(file)
#                 # self = pickle.load(file)
#                 self.update(temp_dict)
#         except EOFError:
#             print("EOFERROR")
#             return None
#         except FileNotFoundError:
#             with open('database/data.pickle', 'wb') as file:
#                 pickle.dump(self.__dict__, file, 1)


class Database():
    def __init__(self, table_names=[], tables={}):
        # super(Database).__init__()
        self.table_names = table_names
        self.tables = tables

    def add_table(self, _table):
        # if _table.table_name in self.table_names:
        if _table['table_name'] in self.table_names:
            print(
                "Cannot have table_names with the same names. RedundancyError")
            sys.exit(0)
        else:
            # self.table_names.append(_table.table_name)
            self.tables[_table['table_name']] = _table

    def drop_table(self, _table):
        # if _table.table_name not in self.table_names:
        if _table['table_name'] not in self.table_names:
            print("Cannot find table: {} in database".format(
                _table['table_name']))
            sys.exit(0)
        else:
            # def self.table_names[_table.table_name]
            del self.table_names[_table['table_name']]

    def save(self):
        with open('database/data.pickle', 'wb') as file:
            pickle.dump(self.__dict__, file, 1)

    def load(self):
        os.makedirs('./database', exist_ok=True)
        try:
            with open('database/data.pickle', 'rb') as file:
                temp_dict = pickle.load(file)
                # self = pickle.load(file)
                self.__dict__.update(temp_dict)
        except EOFError:
            print("EOFERROR")
            return None
        except FileNotFoundError:
            with open('database/data.pickle', 'wb') as file:
                pickle.dump(self.__dict__, file, 1)

    def __getstate__(self):
        return self.__dict__

    def __setstate__(self, d):
        self.__dict__.update(d)
