
# logical operator
def select(table_name,conditions):
    '''
    # TODO
    '''
    # TODO
    return


def insert(table_name: str, data: dict):
    '''
    Example: 
    INSERT INTO my_table (id, name) VALUES (1, 'John');
    
    Args:
    - table_name (str): the name of the table to insert into
    - data (dict): a dictionary representing the data to insert, with keys matching column names
    '''
    # TODO
    return

def drop_table(table_name: str):
    '''
    Example:
    DROP TABLE my_table;

    Args:
    - table_name: string
    '''
    # TODO
    return

def update(table_name: str, set: dict, where: dict):
    '''
    Example:
    UPDATE students 
    SET grade = 'A' 
    WHERE id = 1234;
    
    Args:
    - table_name: string
    - set: dict {"grade": "A"}
    - where: dict {"id": "1234"}
    '''
    # TODO
    return



def delete(table_name: str, where: dict):
    '''
    DELETE FROM my_table
    WHERE column1 = 'value1';

    Args:
    - table_name: string
    - where: dict {"id": "1234"}
    '''
    # TODO
    return


def create_index(index_name: str, table_name: str, column_name: str):
    '''
    CREATE INDEX my_index ON my_table (id);

    Args:
    - index_name: string "my_index"
    - table_name: string "my_table"
    - column_name: string "id"

    '''
    return


