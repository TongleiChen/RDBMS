# Input: line or visual editor
# Parser: 
#    Obtain SQL grammer
#    (Recursive descent) parser
from collections import namedtuple
from lark import Lark

# SELECT GRAMMAR
SELECT_SQL_Grammar = """
	%import common.CNAME
	%import common.SIGNED_NUMBER
	%import common.WS
	%ignore WS

	EQUAL: "="
	LT: "<"
	GT: ">"
	LTE: "<="
	GTE: ">="
	comparison: EQUAL | LT | GT | LTE | GTE
	COMMA: ","
	PERIOD: "."
	LEFT_PAREN: "("
	RIGHT_PAREN: ")"
	SEMICOLON: ";"
	QUOTE: "'"

	NAME: CNAME+
	NUMBER: SIGNED_NUMBER+

	ALL: "*"
	AND: "AND"i
	ASC: "ASC"i
	BETWEEN: "BETWEEN"i
	BY: "BY"i
	DESC: "DESC"i
	FROM: "FROM"i
	MAX: "MAX"i
	MIN: "MIN"i
	ORDER: "ORDER"i
	SELECT: "SELECT"i
	WHERE: "WHERE"i


	char_num: (NAME | NUMBER)*

	start: [select_statement end]

	end: SEMICOLON

	select_statement: SELECT selection from_clause options*

	selection: ALL | num_word_commalist

	num_word_commalist: [max_min] num_word
		| num_word_commalist COMMA [max_min] num_word

	num_word: column_table
		| LEFT_PAREN num_word RIGHT_PAREN
		| item

	options: where_clause | order_by_clause

	from_clause: FROM column_table_commalist

	column_table_commalist: column_table
		| column_table_commalist COMMA column_table

	column_table: char_num
		| char_num PERIOD char_num

	where_clause: WHERE search_condition

	search_condition: search_condition AND search_condition
		| LEFT_PAREN search_condition RIGHT_PAREN
		| predicate

	predicate: between_predicate | comparison_predicate

	between_predicate: num_word BETWEEN num_word AND num_word

	comparison_predicate: num_word comparison num_word

	item_commalist: item | item_commalist COMMA item

	item: QUOTE char_num QUOTE

	order_by_clause: ORDER BY order_by_commalist

	order_by_commalist: order_by | order_by_commalist COMMA order_by

	order_by: column_table asc_desc

	asc_desc: ASC | DESC

	max_min: MAX | MIN
"""
# CREATE GRAMMAR
CREATE_SQL_Grammar = """
	%import common.CNAME
	%import common.SIGNED_NUMBER
	%import common.WS
	%ignore WS
    
    start: [create_statement end]
    
    create_statement: "CREATE TABLE" CNAME "(" field_list ")" 

    field_list: field_definition ("," field_definition)*

    field_definition: CNAME data_type field_constraints?

    data_type: "INT" | "FLOAT" | "VARCHAR" "(" /[0-9]+/ ")"

    field_constraints: "NOT NULL" | "PRIMARY KEY"
    
    end:";"

"""
# DROP GRAMMAR
DROP_SQL_Grammar = """
	%import common.CNAME
	%import common.SIGNED_NUMBER
	%import common.WS
	%ignore WS
    start: [drop_statement end]

    drop_statement: "DROP" "TABLE" table_name

    table_name: CNAME
    
    end: ";"

"""
# UPDATE GRAMMAR
UPDATE_SQL_Grammar = """
	%import common.CNAME
	%import common.SIGNED_NUMBER
	%import common.WS
	%ignore WS
    
    UPDATE: "UPDATE"i
    SET: "SET"i

    start: [update_statement end]
	end: SEMICOLON

    update_statement: UPDATE table_name SET update_list where_clause?

    update_list: update_clause ("," update_clause)*

    update_clause: column_name "=" new_value

    new_value: CNAME | NUMBER

    where_clause: "WHERE" search_condition

    search_condition: (column_name | new_value) comparison_operator (column_name | new_value)

    comparison_operator: "==" | "!=" | "<" | ">" | "<=" | ">=" 

    table_name: CNAME
    column_name: CNAME
    NUMBER: SIGNED_NUMBER+
    SEMICOLON: ";"


"""
# INSERT GRAMMAR
INSERT_SQL_Grammar = """
	%import common.CNAME
	%import common.SIGNED_NUMBER
	%import common.WS
	%ignore WS
    %import common.ESCAPED_STRING -> STRING

    
    start: [insert_statement end]
    
    insert_statement: "INSERT INTO" CNAME "(" column_name_commalist ")" "VALUES" "(" value_commalist ")"

    column_name_commalist: column_name | column_name_commalist "," column_name

    column_name: CNAME

    value_commalist: value | value_commalist "," value

    value: "'" CNAME "'" | NUMBER | "'" STRING "'"
    
    NUMBER: SIGNED_NUMBER+
    
    end: ";"


"""
# DELETE GRAMMAR
DELETE_SQL_Grammar = """
	%import common.CNAME
	%import common.SIGNED_NUMBER
	%import common.WS
	%ignore WS
    start: [delete_statement end]
    
    delete_statement: "DELETE" "FROM" table_name [where_clause]
    
    end:";"

    where_clause: "WHERE" condition

    condition: column_name comparison_operator value

    comparison_operator: "=" | "<" | ">" | "<=" | ">=" | "!="

    column_name: CNAME
    
    value: CNAME | NUMBER
    
    NUMBER: SIGNED_NUMBER+

    table_name: CNAME

"""

if __name__=='__main__':
    # 1. select grammer
    SELECT_Parser = Lark(SELECT_SQL_Grammar)
    select_query = "SELECT name FROM users;"
    select_tree = SELECT_Parser.parse(select_query)
    # parser output: tree
    print(select_tree.pretty())
    
	# 2. create grammer
    CREATE_Parser = Lark(CREATE_SQL_Grammar)
    create_query = """
    CREATE TABLE customers (
    id INT PRIMARY KEY,
    name VARCHAR(50) NOT NULL,
    age INT,
    email VARCHAR(100) NOT NULL
	);
    """
    create_tree = CREATE_Parser.parse(create_query)
    print(create_tree.pretty())	

	# 3. drop grammer
    DROP_Parser = Lark(DROP_SQL_Grammar)
    drop_query="DROP TABLE mybook;"
    update_tree = DROP_Parser.parse(drop_query)
    drop_tree=DROP_Parser.parse(drop_query)
    print(drop_tree.pretty())

	# 4. update grammer
    UPDATE_Parser = Lark(UPDATE_SQL_Grammar)
    update_query="UPDATE my_table SET column1 = 5, column2 = 3 WHERE column3 > 10;"
    update_tree = UPDATE_Parser.parse(update_query)
    print(update_tree.pretty())
    
	# INSERT grammer
    INSERT_Parser = Lark(INSERT_SQL_Grammar)
    insert_query="INSERT INTO customers (name, age) VALUES ('John', 30);"
    insert_tree = INSERT_Parser.parse(insert_query)
    print(insert_tree.pretty())

	# DELETE grammer
    DELETE_Parser = Lark(DELETE_SQL_Grammar)
    delete_query="DELETE FROM customers WHERE age < 18;"
    delete_tree = DELETE_Parser.parse(delete_query)
    print(delete_tree.pretty())	
    
	# mySystem.get_data(relation_name):
	#  


