# Input: line or visual editor
# Parser: 
#    Obtain SQL grammer
#    (Recursive descent) parser
from collections import namedtuple
from lark import Lark

# 定义语法规则
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

if __name__=='__main__':
    # create parser
    SELECT_Parser = Lark(SELECT_SQL_Grammar)
    # parse string
    query = "SELECT name,age FROM users;"
    tree = SELECT_Parser.parse(query)
    # parser output: tree
    print(tree.pretty())
    print(tree)
