import os
from API import process

# from catalog import Database


def main():
    while True:
        try:
            query = input('MiniSQL>  ')
            # print(query[-1])
            query.rstrip()
            while (query == '' or query[-1] != ';'):
                query += input(' ')
                query.rstrip()
        except EOFError:
            break
        process(query)


if __name__ == '__main__':
    main()

# create table student(
#         stu_id char(2) unique,
#         stu_name char(12),
#         primary key (stu_id));

# insert into student values ('12', '123456789012');
# select * from student where sage > 20 and sgender = ‘F’;
