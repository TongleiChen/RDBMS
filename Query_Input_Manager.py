# Input: line or visual editor
# Parser: 
#    Obtain SQL grammer
#    (Recursive descent) parser
from collections import namedtuple
from lark import Lark
from beautifultable import BeautifulTable
from Data_Definition_Language import System
import time
import os
import pickle
import warnings
warnings.filterwarnings("ignore")

# start: database for testing
# datatables = {
# 	"people": {
# 		"first_name": ["Elvis", "Elton", "Ariana", "Katy", "Blake"],
# 		"last_name": ["Presley", "John", "Grande", "Perry", "Lively"],
# 		"age": [42, 75, 36, 37, 34],
# 		"city": ["Memphis", "Pinner", "Boca Raton", "Santa Barbara", "Los Angeles"],
# 		"day": [8, 25, 6, 25, 25],
# 		"month": [1, 3, 6, 10, 8],
# 		"year": [1935, 1947, 1993, 1984, 1987],
# 		"alive": ["no", "yes", "yes", "yes", "yes"]
# 	},
# 	"sports": {
# 		"team": ["Arsenal", "Manchester United", "Brentford", "Liverpool"],
# 		"city": ["London", "Manchester", "Brentford", "Liverpool"],
# 		"standing": [4, 6, 12, 2],
# 		"year_founded": [1886, 1878, 1889, 1892]
# 	}
# }
# GLOBAL FOR
comparisons = ["=", ">", "<", ">=", "<="]
select_cols = []
tables = []
where_items = []
max_min_cols = []
sum_cols = []
order_by_cols = []
asc_desc = ""
max_min = ""
sum_ = ""
end_flag = False
all_flag = False
from_flag = False
column_flag = False
where_flag = False
between_flag = False
max_min_flag = False
order_by_flag = False
sum_flag = False
and_flag = False
# result = BeautifulTable()
# end: database for testing

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
    OR: "OR"i
    ASC: "ASC"i
    BETWEEN: "BETWEEN"i
    BY: "BY"i
    DESC: "DESC"i
    FROM: "FROM"i
    MAX: "MAX"i
    MIN: "MIN"i
    SUM: "SUM"i
    ORDER: "ORDER"i
    SELECT: "SELECT"i
    WHERE: "WHERE"i
    AVG: "AVG"i
    COUNT: "COUNT"i


    char_num: (NAME | NUMBER)

    start: [select_statement end]

    end: SEMICOLON

    select_statement: SELECT selection from_clause options*

    selection: ALL
        | num_word_commalist

    num_word_commalist: [max_min] num_word
        | num_word_commalist COMMA [max_min] num_word
        | [sum] num_word
        | num_word_commalist COMMA [sum] num_word
        | [avg] num_word
        | num_word_commalist COMMA [avg] num_word
        | [count] num_word
        | num_word_commalist COMMA [count] num_word
        

    num_word: column_table
        | LEFT_PAREN num_word RIGHT_PAREN
        | item
    
    item: QUOTE char_num QUOTE

    options: where_clause* groupby_clause* order_by_clause*
            | theta_join_clause* where_clause* order_by_clause*

    from_clause: FROM column_table_commalist

    column_table_commalist: column_table
        | column_table_commalist COMMA column_table

    column_table: char_num
        | char_num PERIOD char_num

    where_clause: WHERE search_condition

    search_condition: search_condition AND search_condition
        | search_condition OR search_condition
        | LEFT_PAREN search_condition RIGHT_PAREN
        | predicate

    predicate: between_predicate | comparison_predicate

    between_predicate: num_word BETWEEN num_word AND num_word

    comparison_predicate: num_word comparison num_word

    item_commalist: item | item_commalist COMMA item

    order_by_clause: ORDER BY order_by_commalist

    order_by_commalist: column_table asc_desc

    groupby_clause: "GROUP BY" column_table having_clause?

    having_clause: "HAVING" agg_func "(" column_table ")" comparison num_word

    agg_func: AVG | SUM | COUNT | MIN | MAX

    theta_join_clause: "INNER JOIN" column_table "ON" comparison_predicate

    asc_desc: ASC | DESC

    max_min: MAX | MIN 
    
    sum: SUM

    avg: AVG

    count: COUNT
"""
# CREATE GRAMMAR
CREATE_TABLE_SQL_Grammar = """
    %import common.CNAME
    %import common.SIGNED_NUMBER
    %import common.WS
    %ignore WS
    
    start: [create_statement end]
    
    create_statement: "CREATE TABLE" table_name "(" field_list ")" 
    
    table_name: CNAME

    field_list: field_definition ("," field_definition)* ("," foreign_key)*

    field_definition: attribute_name data_type field_constraints?

    foreign_key: "FOREIGN KEY" "(" attribute_name ")" "REFERENCES" table_name "(" attribute_name ")"
    
    attribute_name: CNAME

    INT: "INT"
    FT: "FLOAT"
    VCHR: "VARCHAR" "(" /[0-9]+/ ")"
    data_type: INT | FT | VCHR

    field_constraints: NOT_NULL | PRIMARY_KEY 
    
    NOT_NULL: "NOT NULL"i
    PRIMARY_KEY: "PRIMARY KEY"i

    end:";"

"""
# DROP GRAMMAR
DROP_TABLE_SQL_Grammar = """
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

    start: [update_statement end]
    end: ";"

    update_statement: "UPDATE" table_name "SET" update_list where_clause?

    update_list: update_clause ("," update_clause)*

    update_clause: column_name "=" new_value

    new_value: "'" CNAME "'" | NUMBER

    where_clause: "WHERE" search_condition

    search_condition: (column_name | new_value) comparison_operator (column_name | new_value)
    
    EQUAL: "="
    LT: "<"
    GT: ">"
    LTE: "<="
    GTE: ">="
    QUOTE: "'"
    comparison_operator: EQUAL | LT | GT | LTE | GTE

    table_name: CNAME
    column_name: CNAME
    NUMBER: SIGNED_NUMBER+

"""
# INSERT GRAMMAR
INSERT_SQL_Grammar = """
    %import common.CNAME
    %import common.SIGNED_NUMBER
    %import common.WS
    %ignore WS
    %import common.ESCAPED_STRING -> STRING

    
    start: [insert_statement end]
    
    insert_statement: "INSERT INTO" table_name "(" column_name_commalist ")" "VALUES" "(" value_commalist ")"
    
    table_name: CNAME

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
    
    delete_statement: "DELETE" "FROM" table_name [where_clause] cascade?

    cascade: "CASCADE"
    
    end:";"

    where_clause: "WHERE" condition

    condition: column_name comparison_operator value

    EQUAL: "="
    LT: "<"
    GT: ">"
    LTE: "<="
    GTE: ">="
    comparison_operator: EQUAL | LT | GT | LTE | GTE

    column_name: CNAME
    
    value: "'" CNAME "'" | NUMBER
    
    NUMBER: SIGNED_NUMBER+

    table_name: CNAME

"""

CREATE_INDEX_SQL_Grammar = """
    %import common.CNAME
    %import common.SIGNED_NUMBER
    %import common.WS
    %ignore WS
    start: [create_index_statement end]

    create_index_statement: "CREATE" "INDEX" index_name "ON" table_name "(" column_name ")"

    index_name: CNAME
    table_name: CNAME
    column_name: CNAME
    
    end: ";"
"""

DROP_INDEX_SQL_Grammar = """
    %import common.CNAME
    %import common.SIGNED_NUMBER
    %import common.WS
    %ignore WS
    start: [drop_index_statement end]

    drop_index_statement: "DROP" "INDEX" index_name

    index_name: CNAME
    
    end: ";"
"""

class DROP_INDEX_tree_Evaluator:
    def __init__(self,grammar,query) -> None:
        self.parser = Lark(grammar)
        self.query=query
        self.result=BeautifulTable()
        self.index_name=None # Name of index being dropped
        
    def get_result(self):
        tree=self.parser.parse(self.query)
        self.eval_tree(tree)
        # TODO to get results
        print("drop index name: ",self.index_name)
        ...
        return self.result
    
    def eval_tree(self,tree):
        if tree.data=="start":
            self.eval_tree(tree.children[0])
        elif tree.data=="drop_index_statement":
            self.index_name=tree.children[0].children[0].value
        else:
            raise ValueError(f"Invalid syntax query")

class CREATE_INDEX_tree_Evaluator:
    def __init__(self,grammar,query) -> None:
        self.parser = Lark(grammar)
        self.query=query
        self.result=BeautifulTable()
        self.index_name=None # Name of index being create
        self.table_name=None # Name of index being create
        self.column_name=None # Name of index being create
        
    def get_result(self):
        tree=self.parser.parse(self.query)
        self.eval_tree(tree)
        # TODO to get results
        # print("index name: ",self.index_name)
        # print("table name: ",self.table_name)
        # print("column name: ",self.column_name)
        ...
        return self.result
    def eval_tree(self,tree):
        if tree.data=="start":
            self.eval_tree(tree.children[0])
        elif tree.data=="create_index_statement":
            # create_index_statement: "CREATE" "INDEX" index_name "ON" table_name "(" column_name ")"
            self.index_name=tree.children[0].children[0].value
            self.table_name=tree.children[1].children[0].value
            self.column_name=tree.children[2].children[0].value
        else:
            raise ValueError(f"Invalid syntax query")
        
class SELECT_tree_Evaluator:
    def __init__(self,grammar,query) -> None:
        self.parser = Lark(grammar)
        self.query=query
        self.result=BeautifulTable()
        self.datatable = {}

    def get_result(self,datatable):
        self.datatable = datatable
        tree=self.parser.parse(self.query)
        # print(tree.pretty())
        self.eval_tree(tree)
        return self.result

    def apply(self,token):

        global asc_desc, max_min, end_flag, all_flag, column_flag, from_flag, where_flag, between_flag, max_min_flag, order_by_flag, sum_flag,sum_,and_flag
        
        if token == "SELECT":
            column_flag = True
        elif token == ";":
            end_flag = True
        elif token == "ORDER":
            order_by_flag = True
            from_flag = False
        elif token == "BY":
            pass
        elif token == "FROM":
            from_flag = True
        elif token == "WHERE":
            where_flag = True
        elif token == "BETWEEN":
            between_flag = True
        elif token == "AND":
            and_flag = True
        
        elif token == "ASC":
            asc_desc = "ASC"
        elif token == "DESC":
            asc_desc = "DESC"
        elif order_by_flag:
            if asc_desc != "ASC" and asc_desc != "DESC":
                order_by_cols.append(token)
        elif token == "MAX":
            max_min = "MAX"
            max_min_flag = True
        elif token == "MIN":
            max_min = "MIN"
            max_min_flag = True
        elif token == "SUM":
            sum_="SUM"
            sum_flag == True
        elif where_flag:
            where_items.append(token) 
        elif from_flag:
            if token.type == "NAME":
                tables.append(token)
        elif max_min_flag:
            max_min_cols.append(token)
            max_min_flag = False
            select_cols.append(token)
        elif sum_flag:
            sum_cols.append(token)
            sum_flag=False
            select_cols.append(token)
        elif column_flag:
            if token == "*":
                all_flag = True
            else:
                select_cols.append(token)

        print(where_items)
        if end_flag:
            if where_flag:
                if between_flag:
                    matches = {}
                    for name in tables:
                        for key, values in self.datatable.items():
                            between_vals = []
                            if key == where_items[0]:
                                for v in values:
                                    for item in where_items[1:]:
                                        if item.type == "NUMBER":
                                            between_vals.append(int(item))
                                        elif item.type == "NAME":
                                            between_vals.append(str(item))
                                    if between_vals[0] < v < between_vals[1]:
                                        if key in matches:
                                            matches[key].append(v)
                                        else:
                                            matches[key] = [v]

                                indices = []
                                for v in values:
                                    for match in matches[key]:
                                        if v in matches[key]:
                                            indices.append(values.index(match))						
                        self.datatable = {k:[elt for ind, elt in enumerate(v) if ind in indices] for k,v in self.datatable.items()}
                else:
                    # no between and 
                    temp_cols = []
                    for name in tables:
                        for key, value in self.datatable.items():
                            # print(key,value)
                            temp_comps = []
                            temp_vals = []
                            quote_count = 0
                            concat_val = ""
                            for item in where_items:
                                if key == item:
                                    temp_cols.append(key)
                                elif item in comparisons:
                                    temp_comps.append(item)
                                if item not in temp_cols and item not in temp_comps:
                                    if item == "'":
                                        quote_count += 1
                                    if item.type == "NUMBER":
                                        temp_vals.append(int(item))
                                    elif item.type == "NAME":
                                        temp_vals.append(str(item))
                                        if quote_count % 2 != 0:
                                            concat_val = " ".join([concat_val, item])
                                            temp_vals.remove(item)
                                    if quote_count % 2 == 0:
                                        temp_vals.append(concat_val)
                                        concat_val = ""

                        indices = []
                        valid_data = []
                        print(temp_cols)
                        print(temp_comps)
                        print(temp_vals)
                        if len(temp_cols)==1: 
                            # WHERE condition
                            for col in temp_cols:
                                for sign in temp_comps:
                                    for val in temp_vals:
                                        if val == "":
                                            break
                                        elif isinstance(val, str):
                                            val = val[1:]
                                        for data in self.datatable[col]:
                                            print(data,val)
                                            if sign == "=":
                                                if data == val:
                                                    valid_data.append(data)
                                            if sign == ">":
                                                if data > val:
                                                    valid_data.append(data)
                                            if sign == "<":
                                                if data < val:
                                                    valid_data.append(data)
                                            if sign == ">=":
                                                if data >= val:
                                                    valid_data.append(data)
                                            if sign == "<=":
                                                if data <= val:
                                                    valid_data.append(data)
                                
                                    for key, values in self.datatable.items():
                                        for v in values:
                                            for d in valid_data:
                                                if v in valid_data:
                                                    indices.append(values.index(d))
                        elif len(temp_cols)>1 and and_flag:
                            # WHERE condition1 AND condition2
                            temp_cols1=temp_cols[:len(temp_cols)//2]
                            temp_cols2=[x for x in temp_cols if x not in temp_cols1]
                            print(temp_cols1,temp_cols2)

                            temp_comps1=temp_comps[:len(temp_comps)//2]
                            temp_comps2=[x for x in temp_comps if x not in temp_comps1]
                            print(temp_comps1,temp_comps2)

                            temp_vals1=temp_vals[:len(temp_vals)//2]
                            temp_vals2=[x for x in temp_vals if x not in temp_vals1]
                            print(temp_vals1,temp_vals2)

                            for col in temp_cols1:
                                for sign in temp_comps1:
                                    for val in temp_vals1:
                                        if val == "":
                                            break
                                        elif isinstance(val, str):
                                            val = val[1:]
                                        for data in self.datatable[col]:
                                            print(data,val)
                                            if sign == "=":
                                                if data == val:
                                                    valid_data.append(data)
                                            if sign == ">":
                                                if data > val:
                                                    valid_data.append(data)
                                            if sign == "<":
                                                if data < val:
                                                    valid_data.append(data)
                                            if sign == ">=":
                                                if data >= val:
                                                    valid_data.append(data)
                                            if sign == "<=":
                                                if data <= val:
                                                    valid_data.append(data)
                                
                                    for key, values in self.datatable.items():
                                        for v in values:
                                            for d in valid_data:
                                                if v in valid_data:
                                                    indices.append(values.index(d))
                        
                            for col in temp_cols2:
                                for sign in temp_comps2:
                                    for val in temp_vals2:
                                        if val == "":
                                            break
                                        elif isinstance(val, str):
                                            val = val[1:]
                                        for data in self.datatable[col]:
                                            print(data,val)
                                            if sign == "=":
                                                if data == val:
                                                    valid_data.append(data)
                                            if sign == ">":
                                                if data > val:
                                                    valid_data.append(data)
                                            if sign == "<":
                                                if data < val:
                                                    valid_data.append(data)
                                            if sign == ">=":
                                                if data >= val:
                                                    valid_data.append(data)
                                            if sign == "<=":
                                                if data <= val:
                                                    valid_data.append(data)
                                
                                    for key, values in self.datatable.items():
                                        for v in values:
                                            for d in valid_data:
                                                if v in valid_data:
                                                    indices.append(values.index(d))
                        
                        self.datatable = {k:[elt for ind, elt in enumerate(v) if ind in indices] for k,v in self.datatable.items()}

            if order_by_flag:
                for name in tables:
                    for key in self.datatable.keys():
                        for col in order_by_cols:
                            if key == col:
                                if asc_desc == "ASC":
                                    self.datatable[key].sort()
                                elif asc_desc == "DESC":
                                    self.datatable[key].sort(reverse=True)

            if all_flag:
                for name in tables:
                    for value in self.datatable.values():
                        self.result.columns.append(value)
                    self.result.columns.header = self.datatable.keys()
                # print(result)
            else:
                for name in tables:
                    self.datatable = {k : self.datatable[k] for k in select_cols}
                    index = []
                    if max_min == "MAX":
                        for col in max_min_cols:
                            index.append(self.datatable[col].index(max(self.datatable[col])))
                        self.datatable = {k:[elt for ind, elt in enumerate(v) if ind in index] for k,v in self.datatable.items()}
                    elif max_min == "MIN":
                        for col in max_min_cols:
                            index.append(self.datatable[col].index(min(self.datatable[col])))
                        self.datatable = {k:[elt for ind, elt in enumerate(v) if ind in index] for k,v in self.datatable.items()}
                    elif sum_=="SUM":
                        for key, value in self.datatable.items():
                            new_value=[sum(value)]
                            self.datatable[key]=new_value
                            break
                    for key, value in self.datatable.items():
                        if key in select_cols:
                            self.result.columns.append(value)
                
                self.result.columns.header = select_cols
                # print(result)

    def eval_tree(self,tree):
        # TODO 识别sum的分支
        if tree.data == "start":
            self.eval_tree(tree.children[0]) #select_statement
            self.eval_tree(tree.children[1]) #end
        elif tree.data == "end":
            self.apply(tree.children[0]) #SEMICOLON
        elif tree.data == "select_statement":
            self.apply(tree.children[0]) #SELECT
            self.eval_tree(tree.children[1]) #selection
            self.eval_tree(tree.children[2]) #from clause
            if len(tree.children) > 3:
                self.eval_tree(tree.children[3])
        elif tree.data == "selection":
            if tree.children[0] == "*":
                self.apply(tree.children[0]) # (all) *
            else:
                self.eval_tree(tree.children[0])
        elif tree.data == "num_word_commalist":
            #tree.children[1] 	comma
            if len(tree.children) == 2:
                if tree.children[0] != None: #optional max/min
                    self.eval_tree(tree.children[0])
                self.eval_tree(tree.children[1])
            else:
                self.eval_tree(tree.children[0])
                if tree.children[2] != None: #optional max/min
                    self.eval_tree(tree.children[2])
                self.eval_tree(tree.children[3])
        elif tree.data == "num_word":
            if len(tree.children) == 3:
                #eval_tree(tree.children[0]) left parenthesis
                self.eval_tree(tree.children[1])
                #eval_tree(tree.children[2]) right parenthesis
            elif len(tree.children) == 1:
                self.eval_tree(tree.children[0])
        elif tree.data == "from_clause":
            self.apply(tree.children[0])
            self.eval_tree(tree.children[1])
        elif tree.data == "column_table_commalist":
            self.eval_tree(tree.children[0])
        elif tree.data == "column_table":
            self.eval_tree(tree.children[0])
        elif tree.data == "char_num":
            for x in range(len(tree.children)):
                self.apply(tree.children[x])
        elif tree.data == "options":
            self.eval_tree(tree.children[0])
        elif tree.data == "where_clause":
            self.apply(tree.children[0]) #WHERE
            self.eval_tree(tree.children[1]) # search condition
        elif tree.data == "order_by_clause":
            self.apply(tree.children[0]) #ORDER
            self.apply(tree.children[1]) #BY
            self.eval_tree(tree.children[2]) # order_by_commalist
        elif tree.data == "search_condition": 
            if len(tree.children)==1:
                self.eval_tree(tree.children[0]) 
            else:
                for child in tree.children:
                    if child=="AND":
                        self.apply(child) # AND
                    else:
                        self.eval_tree(child)

        elif tree.data == "predicate":
            self.eval_tree(tree.children[0])
        elif tree.data == "comparison_predicate":
            self.eval_tree(tree.children[0]) #num_word
            self.eval_tree(tree.children[1]) #comparison
            self.eval_tree(tree.children[2])
        elif tree.data == "comparison":
            self.apply(tree.children[0])
        elif tree.data == "between_predicate":
            self.eval_tree(tree.children[0])
            self.apply(tree.children[1])
            self.eval_tree(tree.children[2])
            self.apply(tree.children[3])
            self.eval_tree(tree.children[4])
        elif tree.data == "order_by_commalist":
            #tree.children[1] 	comma
            if len(tree.children) == 1:
                self.eval_tree(tree.children[0])
            else:
                self.eval_tree(tree.children[0])
                self.eval_tree(tree.children[2])
        elif tree.data == "order_by":
            self.eval_tree(tree.children[0]) #column_table
            self.eval_tree(tree.children[1]) #asc_desc
        elif tree.data == "asc_desc":
            self.apply(tree.children[0])
        elif tree.data == "max_min":
            self.apply(tree.children[0])
        elif tree.data == "sum":
            self.apply(tree.children[0])
        elif tree.data == "item":
            self.apply(tree.children[0]) # quote
            self.eval_tree(tree.children[1])
            self.apply(tree.children[2]) # quote
        else:
            raise SyntaxError('unrecognized tree')

class new_SELECT_tree_Evaluator:
    def __init__(self,grammar,query) -> None:
        self.parser = Lark(grammar)
        self.query=query
        self.result=BeautifulTable()
        self.selection_clause={"tables":[],"cols":[],"agg_fun":[],"all_flag":[False]}
        self.from_clause=[] # table name
        self.option={"where_clause":[],
                     "order_by_clause":[],
                     "group_having_clause":[],
                     "theta_join_clause":[],
                     "group_having_clause":[]}
    def get_result(self):
        tree=self.parser.parse(self.query)
        # print(tree.pretty())
        self.eval_tree(tree)
        # TODO to get results
        # print("selection clause: ",self.selection_clause)
        # print("from clause: ",self.from_clause)
        # print("options: ",self.option)
        ...
        return self.result    
    def eval_tree(self,tree):
        if tree.data=="start":
            self.eval_tree(tree.children[0]) # select statement
        elif tree.data=="select_statement":
            self.eval_tree(tree.children[1]) # selection
            self.eval_tree(tree.children[2]) # from_clause
            if len(tree.children)>3:
                self.eval_tree(tree.children[3])
        elif tree.data=="selection":
            self.eval_tree(tree.children[0])
        elif tree.data=="ALL":
            self.selection_clause["all_flag"][0]=True
        elif tree.data=="num_word_commalist": 
            if len(tree.children)>=3: # num_word_commalist COMMA [max_min] num_word
                self.eval_tree(tree.children[0])
                for i in range(1,len(tree.children)):
                    if tree.children[i]==",":
                        pass
                    elif tree.children[i]==None:
                        self.selection_clause["agg_fun"].append(None)
                    elif tree.children[i].data=="max_min" or tree.children[i].data=="sum" or tree.children[i].data=="avg" or tree.children[i].data=="count" :
                        self.selection_clause["agg_fun"].append(tree.children[i].children[0].value)
                    elif tree.children[i].data=="num_word":
                        if len(tree.children[i].children)==1:
                            if len(tree.children[i].children[0].children)==1: # column_table-char_num
                                if self.from_clause==[]:self.selection_clause["tables"].append(None)
                                self.selection_clause["cols"].append(tree.children[i].children[0].children[0].children[0].value)
                            elif len(tree.children[i].children[0].children)==3: # column_table-"."
                                self.selection_clause["tables"].append(tree.children[i].children[0].children[0].children[0].value)
                                self.selection_clause["cols"].append(tree.children[i].children[0].children[2].children[0].value)

                        elif len(tree.children[i].children)>1:
                            if self.from_clause==[]:self.selection_clause["tables"].append(None)
                            self.selection_clause["cols"].append(tree.children[i].children[1].children[0].children[0].children[0].value)
            else: # [max_min/sum] num_word
                for i in range(len(tree.children)):
                    if tree.children[i]==",":
                        pass
                    elif tree.children[i]==None:
                        self.selection_clause["agg_fun"].append(None)
                    elif tree.children[i].data=="max_min" or tree.children[i].data=="sum" or tree.children[i].data=="avg" or tree.children[i].data=="count":
                        self.selection_clause["agg_fun"].append(tree.children[i].children[0].value)
                    elif tree.children[i].data=="num_word":
                        if len(tree.children[i].children)==1: # column_table
                            if len(tree.children[i].children[0].children)==1: # column_table-char_num
                                if self.from_clause==[]:self.selection_clause["tables"].append(None)
                                self.selection_clause["cols"].append(tree.children[i].children[0].children[0].children[0].value)
                            elif len(tree.children[i].children[0].children)==3: # column_table-"."
                                self.selection_clause["tables"].append(tree.children[i].children[0].children[0].children[0].value)
                                self.selection_clause["cols"].append(tree.children[i].children[0].children[2].children[0].value)
                        elif len(tree.children[i].children)>1: # LEFT_PAREN num_word RIGHT_PAREN
                            if self.from_clause==[]:self.selection_clause["tables"].append(None)
                            self.selection_clause["cols"].append(tree.children[i].children[1].children[0].children[0].children[0].value)
                    
        elif tree.data=="from_clause":
            self.eval_tree(tree.children[1])
        elif tree.data=="column_table_commalist":
            self.from_clause.append(tree.children[0].children[0].children[0].value)
        elif tree.data=="options":
            for child in tree.children:
                self.eval_tree(child)
        elif tree.data=="where_clause":
            self.eval_tree(tree.children[1]) # search condition
        elif tree.data=="search_condition": # 只在where clause中出现
            if len(tree.children)==1:
                self.eval_tree(tree.children[0]) # predict
            elif len(tree.children)==3: # AND/OR
                self.eval_tree(tree.children[0]) 
                self.option["where_clause"].append(tree.children[1].value) # 先把AND OR append 进去
                self.eval_tree(tree.children[2])
        elif tree.data=="predicate":
            self.eval_tree(tree.children[0]) # between_predict / comparison_predict
        elif tree.data=="between_predicate":
            temp=[]
            for child in tree.children:
                if child=="BETWEEN" or child=="AND":
                    temp.append(child.value)
                elif child.data=="num_word":
                    if len(child.children)==1: # column_table/item
                        if len(child.children[0].children)==1:
                            temp.append(child.children[0].children[0].children[0].value)
                        elif len(child.children[0].children)==3:
                            temp.append(child.children[0].children[1].children[0].value)
            self.option["where_clause"].append(temp)

        elif tree.data=="comparison_predicate": # 只针对于where clause的
            temp=[]
            for child in tree.children: 
                if child.data=="comparison": 
                    temp.append(child.children[0].value)
                elif child.data=="num_word":
                    if len(child.children)==1: # column_table/item
                        if len(child.children[0].children)==1:
                            temp.append(child.children[0].children[0].children[0].value)
                        elif len(child.children[0].children)==3:
                            temp.append(child.children[0].children[1].children[0].value)
            self.option["where_clause"].append(temp)     

        elif tree.data=="order_by_clause":
            self.eval_tree(tree.children[2]) # order_by_commalist

        elif tree.data=="order_by_commalist":
            # print(tree.children[1].children[0].value)
            self.option["order_by_clause"].append(tree.children[0].children[0].children[0].value)
            self.option["order_by_clause"].append(tree.children[1].children[0].value)

        elif tree.data=="groupby_clause":
            self.option["group_having_clause"].append(tree.children[0].children[0].children[0].value) # column_table
            if len(tree.children)>=2: # "GROUP BY" column_table having_clause 
                self.eval_tree(tree.children[1])

        elif tree.data=="having_clause":
            for child in tree.children:
                if child.data=="agg_func":
                    self.option["group_having_clause"].append(child.children[0].value)
                elif child.data=="column_table":
                    self.option["group_having_clause"].append(child.children[0].children[0].value) # column_table
                elif child.data=="comparison":
                    self.option["group_having_clause"].append(child.children[0].value)
                elif child.data=="num_word":
                    if len(child.children)==1: # column_table/item
                        if len(child.children[0].children)==1:
                            self.option["group_having_clause"].append(child.children[0].children[0].children[0].value)
                        elif len(child.children[0].children)==3:
                            self.option["group_having_clause"].append(child.children[0].children[1].children[0].value)
                            
        elif tree.data=="theta_join_clause":
            for child in tree.children:
                if child.data=="column_table":
                    self.option["theta_join_clause"].append(child.children[0].children[0].value)
                elif child.data=="comparison_predicate":
                    # print("comparison_predicate")
                    # name_age.name = name_address.name
                    for child in child.children: 
                        if child.data=="comparison": 
                            # print(child.children[0].value)
                            self.option["theta_join_clause"].append(child.children[0].value)
                        elif child.data=="num_word":
                            self.option["theta_join_clause"].append(child.children[0].children[0].children[0].value)
                            self.option["theta_join_clause"].append(child.children[0].children[2].children[0].value)

class UPDATE_tree_Evaluator:
    def __init__(self,grammar,query) -> None:
        self.parser = Lark(grammar)
        self.query=query
        self.result=BeautifulTable()
        self.table_name=None # Name of Table being changed
        self.update_clause={"cols":[],"vals":[]} # Update clause list
        self.where_clause={"cols":[],"ops":[],"vals":[]} # Where clause list

    def get_result(self):
        tree=self.parser.parse(self.query)
        self.eval_tree(tree)
        # TODO to get results
        # print("table to be updated: ",self.table_name)
        # print("update clause: ",self.update_clause)
        # print("where clause: ",self.where_clause)
        ...
        return self.result
    
    def eval_tree(self,tree):
        if tree.data == "start":
            self.eval_tree(tree.children[0]) # "update_statement"
            # "END"
        elif tree.data == "update_statement":
            self.table_name=tree.children[0].children[0]
            self.eval_tree(tree.children[1])
            if len(tree.children) == 3:
                self.eval_tree(tree.children[2])
        
        # self.update_clause={"cols":[],"vals":[]} 
        # self.where_clause={"cols":[],"ops":[],"vals":[]} 
        elif tree.data == "update_list":
            for child in tree.children:
                self.eval_tree(child)

        elif tree.data == "update_clause":
            self.update_clause["cols"].append(tree.children[0].children[0].value)
            self.update_clause["vals"].append(tree.children[1].children[0].value)
        elif tree.data=="where_clause":
            self.eval_tree(tree.children[0])

        elif tree.data == "search_condition":
            self.where_clause["cols"].append(tree.children[0].children[0].value)
            self.where_clause["ops"].append(tree.children[1].children[0].value)
            self.where_clause["vals"].append(tree.children[2].children[0].value)
        
        else:
            raise ValueError(f"Invalid syntax query")

class DROP_TABLE_tree_Evaluator:
    def __init__(self,grammar,query) -> None:
        self.parser = Lark(grammar)
        self.query=query
        self.result=BeautifulTable()
        self.table_name=None # Name of Table being dropped
        
    def get_result(self):
        tree=self.parser.parse(self.query)
        self.eval_tree(tree)
        # TODO to get results
        # print("table to be dropped: ",self.table_name)
        ...
        return self.result
    
    def eval_tree(self,tree):
        if tree.data == "start":
            self.eval_tree(tree.children[0])
        elif tree.data == "drop_statement":
            self.eval_tree(tree.children[0])
        elif tree.data == "table_name":
            self.table_name=tree.children[0].value
        else:
            raise ValueError(f"Invalid syntax query")

class INSERT_tree_Evaluator:
    def __init__(self,grammar,query) -> None:
        self.parser = Lark(grammar)
        self.query=query
        self.result=BeautifulTable()
        self.table_name=None # Name of Table being inserted
        self.insert_cols=[]
        self.insert_vals=[]
    
    def get_result(self):
        tree=self.parser.parse(self.query)
        self.eval_tree(tree)
        # TODO to get results
        # print("table to be inserted: ",self.table_name)
        # print("insert columns: ",self.insert_cols)
        # print("insert valuse: ",self.insert_vals)
        
        return self.result
    
    def eval_tree(self,tree):
        if tree.data == "start":
            self.eval_tree(tree.children[0])
        elif tree.data == "insert_statement":
            self.table_name=tree.children[0].children[0].value # YUNI: 加了.value
            self.eval_tree(tree.children[1]) # column_name_commalist
            self.eval_tree(tree.children[2]) # value_commalist
        elif tree.data == "column_name_commalist":
            if len(tree.children)==1:
                self.insert_cols.append(tree.children[0].children[0].value)
            else:
                self.eval_tree(tree.children[0])
                for child in tree.children[1:]:
                    self.insert_cols.append(child.children[0].value)
        
        elif tree.data == "value_commalist":
            if len(tree.children)==1:
                self.insert_vals.append(tree.children[0].children[0].value)
            else:
                self.eval_tree(tree.children[0])
                for child in tree.children[1:]:
                    self.insert_vals.append(child.children[0].value)
        else:
            raise ValueError(f"Invalid syntax query")
        
class DELETE_tree_Evaluator:
    def __init__(self,grammar,query) -> None:
        self.parser = Lark(grammar)
        self.query=query
        self.result=BeautifulTable()
        self.table_name=None # Name of Table being delete
        self.where_clause={"cols":[],"ops":[],"vals":[]} # Where clause list
        self.cascade_flag=False
    
    def get_result(self):
        tree=self.parser.parse(self.query)
        self.eval_tree(tree)
        # TODO to get results
        # print("table to be delete: ",self.table_name)
        # print("where clause: ",self.where_clause)
        # print("cascade flag: ",self.cascade_flag)
        

        ...
        return self.result
    
    def eval_tree(self,tree):
        if tree.data == "start":
            self.eval_tree(tree.children[0])
        elif tree.data == "delete_statement":
            self.eval_tree(tree.children[0])
            self.eval_tree(tree.children[1])
            if len(tree.children)>2:
                self.eval_tree(tree.children[2]) # cascade
        elif tree.data == "cascade":
            self.cascade_flag=True
        elif tree.data == "table_name":
            self.table_name=tree.children[0].value
        elif tree.data == "where_clause":
            self.eval_tree(tree.children[0]) # condition
        elif tree.data == "condition":
            self.where_clause["cols"].append(tree.children[0].children[0].value)
            self.where_clause["ops"].append(tree.children[1].children[0].value)
            self.where_clause["vals"].append(tree.children[2].children[0].value)
        
        else:
            raise ValueError(f"Invalid syntax query")
        
class CREATE_TABLE_tree_Evaluator:
    def __init__(self,grammar,query) -> None:
        self.parser = Lark(grammar)
        self.query=query
        self.result=BeautifulTable()
        self.table_name=None # Name of Table being create
        self.attributes_clause={"names":[],"data_type":[],"constraints":[],"foreign_keys_for_table":[]} # Where clause list
    def get_result(self):
        tree=self.parser.parse(self.query)
        self.eval_tree(tree)
        # TODO to get results
        # print("table to be delete: ",self.table_name)
        # print("attributes clause: ",self.attributes_clause)
        ...
        return self.result
    def eval_tree(self,tree):
        if tree.data == "start":
            self.eval_tree(tree.children[0]) # create_statement
        elif tree.data == "create_statement":
            self.table_name=tree.children[0].children[0].value # table_name
            self.eval_tree(tree.children[1]) # field_list
        elif tree.data == "field_list":
            for child in tree.children:
                self.eval_tree(child) # field definition
    # field_list: field_definition ("," field_definition)* (foreign_key)*
    # field_definition: attribute_name data_type field_constraints?
    # field_constraints: not_null | primary_key 
    
    # not_null: "NOT NULL"
    # primary_key: "PRIMARY KEY" 
    # foreign_key: "FOREIGN KEY" "(" attribute_name ")" "REFERENCES" table_name "(" attribute_name ")"
        elif tree.data == "field_definition":
            print("field_definition")
            self.attributes_clause["names"].append(tree.children[0].children[0].value)
            self.attributes_clause["data_type"].append(tree.children[1].children[0].value)
            if len(tree.children)==2:
                self.attributes_clause["constraints"].append("None")
            elif len(tree.children)==3: # field_constraints
                self.eval_tree(tree.children[2])
        elif tree.data == "foreign_key":
            print(tree)
            temp=[]
            temp.append(tree.children[0].children[0].value)
            temp.append(tree.children[1].children[0].value)
            temp.append(tree.children[2].children[0].value)
            self.attributes_clause["foreign_keys_for_table"].append(temp)
        elif tree.data=="field_constraints":
            self.attributes_clause["constraints"].append(tree.children[0].value)
        else:
            raise ValueError(f"Invalid syntax query")

def GET_EVALUATOR_from_Query(query):
        EVALUATOR=None
        query_list=query.split(" ")
        option=query_list[0]
        if option=="CREATE" or option=="DROP":
            option+=query_list[1]
        
        if option=="SELECT":
            EVALUATOR=new_SELECT_tree_Evaluator(SELECT_SQL_Grammar,query)
        elif option=="CREATETABLE":
            EVALUATOR=CREATE_TABLE_tree_Evaluator(CREATE_TABLE_SQL_Grammar,query)
        elif option=="DROPTABLE":
            EVALUATOR=DROP_TABLE_tree_Evaluator(DROP_TABLE_SQL_Grammar,query)
        elif option=="UPDATE":
            EVALUATOR=UPDATE_tree_Evaluator(UPDATE_SQL_Grammar,query)
        elif option=="INSERT":
            EVALUATOR=INSERT_tree_Evaluator(INSERT_SQL_Grammar,query)
        elif option=="CREATEINDEX":
            EVALUATOR=CREATE_INDEX_tree_Evaluator(CREATE_INDEX_SQL_Grammar,query)
        elif option=="DROPINDEX":
            EVALUATOR=DROP_INDEX_tree_Evaluator(DROP_INDEX_SQL_Grammar,query)
        elif option=="DELETE":
            EVALUATOR=DELETE_tree_Evaluator(DELETE_SQL_Grammar,query)
        else:
            raise ValueError(f"Invalid syntax query")
        
        return option, EVALUATOR

def EXECUTE(db_system:System,query:str):
    start_time = time.time()
    option,parser = GET_EVALUATOR_from_Query(query)
    if option=="SELECT":
        return SELECT(db_system,parser)
    elif option=="CREATETABLE":
        CREATE(db_system,parser)
    elif option=="DROPTABLE":
        DROP(db_system,parser)
    elif option=="UPDATE":
        UPDATE(db_system,parser)
    elif option=="INSERT":
        INSERT(db_system,parser)
    elif option=="DELETE":
        DELETE(db_system,parser)
    end_time = time.time()
    print("Execution time:", end_time - start_time, "seconds")
    return

def UPDATE(db_system:System,update_parser:UPDATE_tree_Evaluator):
    update_parser.get_result()
    db_system.update_data(update_parser.table_name,update_parser.update_clause,update_parser.where_clause)
    # update_answer = "UPDATE {} SUCCESSFULLY. ".format(update_parser.table_name)
    return


def INSERT(db_system:System,insert_parser:INSERT_tree_Evaluator):
    insert_parser.get_result()
    db_system.insert_data(insert_parser.table_name,insert_parser.insert_cols,insert_parser.insert_vals)

    return


def DELETE(db_system:System,delete_parser:DELETE_tree_Evaluator):
    delete_parser.get_result()
    db_system.delete_data_dict(delete_parser.table_name,delete_parser.where_clause)

    return

def DROP(db_system:System,drop_parser:DROP_TABLE_tree_Evaluator):
    drop_parser.get_result()
    db_system.drop_table_dict(drop_parser.table_name)

    return

def CREATE(db_system:System,create_parser:CREATE_TABLE_tree_Evaluator):
    create_parser.get_result()
    db_system.create_table_dict(create_parser.table_name,create_parser.attributes_clause)

def SELECT(db_system:System,select_parser:new_SELECT_tree_Evaluator):
    select_parser.get_result()
    selection_clause = select_parser.selection_clause
    from_clause = select_parser.from_clause[0] # one table 
    option = select_parser.option
    select_all_flag = selection_clause['all_flag']

    if select_all_flag == True:
        select_columns = list(db_system.database_tables[from_clause].keys())
    else:
        select_columns = selection_clause['cols']
    select_tables = selection_clause['tables']
    select_agg = selection_clause['agg_fun']
    where_clause = option['where_clause']
    order_by_clause = option['order_by_clause']
    group_by_clause = option['group_having_clause']
    theta_join_clause = option['theta_join_clause']

    if len(theta_join_clause) != 0:
        # SIMPLE INNER JOIN
        projection_cols_1 = []
        projection_cols_2 = []
        for idx,col in enumerate(select_columns):
            if select_tables[idx] == from_clause:
                projection_cols_1.append(col)
            else:
                projection_cols_2.append(col)
        print("HERE")
        
        join_table = db_system.nested_loop_join(table_1=from_clause,
                                table_1_col=theta_join_clause[2],
                                table_2=theta_join_clause[0],
                                table_2_col=theta_join_clause[5],
                                projection_cols_1=projection_cols_1,
                                projection_cols_2=projection_cols_2
                                )
        return join_table
    temp_data_table = db_system.database_tables[from_clause]
    if len(where_clause) != 0:
        # apply where
        temp_data_table,_ = db_system.select_where(relation_name = from_clause,
                                                conditions = where_clause)
    if len(group_by_clause) != 0:
        temp_data_table = db_system.group_by(data_table = temp_data_table,
                                        group_columns = [group_by_clause[0]],
                                        having_condition = group_by_clause[1:],
                                        table_cols = select_columns,
                                        agg_func = select_agg
                                        )
        
        if len(order_by_clause) != 0:
            temp_data_table = db_system.order_by(table_data = temp_data_table,
                                            order_cols = [order_by_clause[0]],
                                            sort = order_by_clause[1])
        return temp_data_table
    if len(order_by_clause) != 0:
        temp_data_table = db_system.order_by(table_data = temp_data_table,
                                            order_cols = [order_by_clause[0]],
                                            sort = order_by_clause[1])
    
    temp_data_table = db_system.projection(data_table = temp_data_table,
                    cols = select_columns,
                    agg_fun = select_agg
                    )

    return temp_data_table

def load_database(database_name) -> System:
    database_file_path = os.path.join("data",database_name)
    with open(database_file_path, 'rb') as f:
        db_system = pickle.load(f)
    return db_system
def save_query(db_system:System,query:str):
    database_file_path = os.path.join("query",db_system.database_name+".txt")
    with open(database_file_path, "r+") as f:
        f.seek(0, 2)
        f.write(query+"\n")

def checkpoint(db_system:System):
    database_file_path = os.path.join("query",db_system.database_name+".txt")
    with open(database_file_path, "r+") as f:
        f.seek(0, 2)
        f.write("<CHECKPOINT>"+"\n")
    db_system.save_database()

def read_query(db_system:System):
    database_file_path = os.path.join("query",db_system.database_name+".txt")
    with open(database_file_path, 'rb') as f:
        try:  # catch OSError in case of a one line file 
            f.seek(-2, os.SEEK_END)
            while f.read(1) != b'\n':
                f.seek(-2, os.SEEK_CUR)
        except OSError:
            return None
        last_line = f.readline().decode()
        if last_line == "<CHECKPOINT>"+"\n":
            return None
    with open(database_file_path, 'r') as f:
        query_list = f.readlines()
        position = -1
        for line_n,line in enumerate(query_list):
            if line == "<CHECKPOINT>"+"\n":
                position = line_n
        # for 

        return query_list[position+1:]
def recover(db_system:System):
    res = read_query(db_system)
    if res != None:
        for query in res:
            EXECUTE(db_system,query)
        checkpoint(db_system)

        
    




def DISPLAY_SQL_RESULTS(res):
    table = BeautifulTable()
    table.column_headers = list(res.keys())
    for i in range(len(res[list(res.keys())[0]])):
        row = []
        for key in res.keys():
            row.append(res[key][i])
        table.append_row(row)
    
    print(table)
    return table
# mydict={'id': [0, 1, 2, 3, 4], 'customer_name': ['yuni', 'suzy', 'John', 'Lesley', 'selina']}
# print(DISPLAY_SQL_RESULTS(mydict))

def examples(option):
	sql_statement = ""

	# CREATE
	if option == 1:
		sql_statement = """
            CREATE TABLE customers (
            id INT NOT NULL,
            name VARCHAR(50),
            age INT PRIMARY KEY,
            email VARCHAR(100) NOT NULL,
            FOREIGN KEY (customer_id) REFERENCES Customers(customer_id)
            );
    """
    # SELECT
	elif option == 2:
		sql_statement = """
            SELECT name_age.age,name_address.address 
            FROM name_age 
            INNER JOIN name_address 
            ON name_age.name = name_address.name 
            WHERE name='suzy' AND age BETWEEN 12 AND 30 
            ORDER BY age ASC;
		"""
	# UPDATE
	elif option == 3:
		sql_statement = "UPDATE my_table SET column1 = 'suzy', column2 = 3 WHERE column3 >= 10;"
                
	# INSERT
	elif option == 4:
		sql_statement = "INSERT INTO name_age (name, age) VALUES ('John', 30);"
                
	# DELETE
	elif option == 5:
		sql_statement = "DELETE FROM name_age WHERE age < 14 CASCADE;"
                
	# DROP TABLE
	elif option == 6:
		sql_statement = "DROP TABLE customers;"


if __name__=='__main__':
    #test_query="SELECT age FROM name_age INNER JOIN name_age ON name_age1.name=name_age2.name;"
    #test_query="CREATE TABLE customers (id INT PRIMARY KEY,name VARCHAR(50) NOT NULL,age INT PRIMARY KEY,email VARCHAR(100) NOT NULL);"
    #test_query="DROP TABLE customers;"
    #test_query="UPDATE my_table SET column1 = 'suzy', column2 = 3 WHERE column3 >= 10;"
    #test_query="INSERT INTO name_age (name, age) VALUES ('John', 30);"
    #test_query="CREATE INDEX index_nameON table_name (column_name);"
    #test_query="DROP INDEX index_name ON table_name;"
    #test_query="CREATE INDEX index_name ON name_age (name);"
    
    # test_query="DROP INDEX index_name;"
    # EVALUATOR=GET_EVALUATOR_from_Query(test_query)
    # print(EVALUATOR.get_result())
    test_system = load_database("TEST")
    recover(test_system)
    query_num = 0
    while True:
        print('Tell Me Your Option: \n create your own query \n Type \'EXAMPLE\' to use some given example queries \n Type \'EXIT\' to quit: \n')
        sql = input('Please Input >>> ')

            # execution: get result dict

    
        if sql == "example" or sql=="EXAMPLE":
            example = input('Choose from given example by typing a name below: \n \t CREATE TABLE \n \t SELECT \n \t UPDATE \n \t INSERT \n \t DELETE \n \t DROP TABLE \n >')
            if example == "CREATE TABLE":
                examples(1)
            elif example == "SELECT":
                examples(2)
            elif example == "UPDATE":
                examples(3)
            elif example == "INSERT":
                examples(4)
            elif example == "DELETE":
                examples(5)
            elif example == "DROP TABLE":
                examples(6)
            else:
                raise ValueError(f"Invalid syntax query")
        elif sql=="exit" or sql=="EXIT":
            test_system.save_database()
            checkpoint(test_system)
            break
        else:
            
            if sql=="exit" or sql=="EXIT":
                break
            # EVALUATOR=GET_EVALUATOR_from_Query(sql)
            res = EXECUTE(db_system=test_system,query=sql)
            if res != None:
                DISPLAY_SQL_RESULTS(res)
            else:
                save_query(test_system,sql)
                query_num += 1
                if query_num == 10:
                    query_num = 0 
                    test_system.save_database()
                    checkpoint(test_system)
                    

    # # 1. select grammar
    # # 0410 tested
    # mySystem=System()
    # mySystem.open_database('CLASS')
    # #select_query = "SELECT age FROM name_age INNER JOIN name_age ON name_age1.name=name_age2.name;"
    # #select_query = "SELECT age,MAX(name),name FROM name_age WHERE name='suzy' AND age BETWEEN 12 AND 30;" #where的顺序只能跟列表的顺序一致：
    # select_query = "SELECT age,name,MAX(name) FROM name_age WHERE name='suzy' AND age BETWEEN 12 AND 30;"
    # """
    # select_query = 
    # SELECT name_age.age,name_address.address 
    # FROM name_age 
    # INNER JOIN name_address 
    # ON name_age.name = name_address.name 
    # WHERE name='suzy' AND age BETWEEN 12 AND 30 
    # ORDER BY age ASC;
    # """
    # #select_query ="SELECT height FROM name_height ORDER BY height DESC;"
    # #select_query = "SELECT MAX(age) FROM name_age WHERE name = suzy AND age < 18;"
    # SELECT_SQL_EVALUATOR=new_SELECT_tree_Evaluator(SELECT_SQL_Grammar,select_query)
    # print(SELECT_SQL_EVALUATOR.get_result())


    
    # # 2. create grammar
    # create_query = """
    # CREATE TABLE customers (
    # id INT NOT NULL,
    # name VARCHAR(50),
    # age INT PRIMARY KEY,
    # email VARCHAR(100) NOT NULL,
    # FOREIGN KEY (customer_id) REFERENCES Customers(customer_id)
    # );
    # """
    # CREATE_SQL_EVALUATOR=CREATE_TABLE_tree_Evaluator(CREATE_TABLE_SQL_Grammar,create_query)
    # print(CREATE_SQL_EVALUATOR.get_result())
    # # mySystem.create_table_dict(CREATE_SQL_EVALUATOR.table_name.value,CREATE_SQL_EVALUATOR.attributes_clause)
    # print(mySystem.database_tables)
    # print(mySystem.table_attributes)
    # print(mySystem.table_path)
    
    # print("==============================")
    # # create_foreign_key = '''
    # # CREATE TABLE orders (
    # # order_id INT PRIMARY KEY,
    # # customer_id INT,
    # # FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
    # # );
    # # '''
    # # CREATE_SQL_EVALUATOR=CREATE_TABLE_tree_Evaluator(CREATE_TABLE_SQL_Grammar,create_foreign_key)
    # # print(CREATE_SQL_EVALUATOR.get_result())



    # # 3. drop grammar
    # # 0410 tested
    # drop_query="DROP TABLE customers;"
    # DROP_SQL_EVALUATOR=DROP_TABLE_tree_Evaluator(DROP_TABLE_SQL_Grammar,drop_query)
    # print(DROP_SQL_EVALUATOR.get_result())
    # print(DROP_SQL_EVALUATOR.table_name)
    # mySystem.drop_table_dict(DROP_SQL_EVALUATOR.table_name)
    # print(mySystem.database_tables)
    # print(mySystem.table_attributes)
    # print(mySystem.table_path)

    # # 4. update grammar
    # # 0410 tested
    # update_query="UPDATE my_table SET column1 = 'suzy', column2 = 3 WHERE column3 >= 10;"
    # UPDATE_SQL_EVALUATOR=UPDATE_tree_Evaluator(UPDATE_SQL_Grammar,update_query)
    # print(UPDATE_SQL_EVALUATOR.get_result())

    
    # # INSERT grammar
    # # 0410 tested
    # insert_query="INSERT INTO name_age (name, age) VALUES ('John', 30);"
    # INSERT_SQL_EVALUATOR=INSERT_tree_Evaluator(INSERT_SQL_Grammar,insert_query)
    # print(INSERT_SQL_EVALUATOR.get_result())
    
    # data = {}
    # for i,column in enumerate(INSERT_SQL_EVALUATOR.insert_cols):
    #     data[column] = INSERT_SQL_EVALUATOR.insert_vals[i]
    # print(data)
    # # mySystem.insert_data(INSERT_SQL_EVALUATOR.table_name,data)


    # # DELETE grammar
    # # 0410 tested
    # delete_query="DELETE FROM name_age WHERE age < 14 CASCADE;"
    # DELETE_SQL_EVALUATOR=DELETE_tree_Evaluator(DELETE_SQL_Grammar,delete_query)
    # print(DELETE_SQL_EVALUATOR.get_result())
    # mySystem.delete_data_dict(DELETE_SQL_EVALUATOR.table_name,DELETE_SQL_EVALUATOR.where_clause)
    # print(mySystem.database_tables)

    # # update_query_2 = "UPDATE name_height SET height = 160 WHERE name = 'suzy';"
    # # UPDATE_SQL_EVALUATOR_2=UPDATE_tree_Evaluator(UPDATE_SQL_Grammar,update_query_2)
    # # DELETE_SQL_EVALUATOR.get_result()
    # # mySystem.update_data(UPDATE_SQL_EVALUATOR_2.table_name,UPDATE_SQL_EVALUATOR_2.update_clause,UPDATE_SQL_EVALUATOR_2.where_clause)
    # # print(mySystem.database_tables)

    # # update_query="UPDATE name_height SET name = 'suzy' WHERE column3 = 10;"
    # # UPDATE_SQL_EVALUATOR=UPDATE_tree_Evaluator(UPDATE_SQL_Grammar,update_query)
    # # print(UPDATE_SQL_EVALUATOR.get_result())

    # insert_query="INSERT INTO name_age (id,name, age) VALUES (3,'Chelsea', 18);"
    # INSERT_SQL_EVALUATOR=INSERT_tree_Evaluator(INSERT_SQL_Grammar,insert_query)
    # print(INSERT_SQL_EVALUATOR.get_result())

    # mySystem.insert_data(INSERT_SQL_EVALUATOR.table_name,INSERT_SQL_EVALUATOR.insert_cols,INSERT_SQL_EVALUATOR.insert_vals)
    # print(mySystem.database_tables)


    # update_query="UPDATE name_age SET name = 'YUNI' WHERE age = 13;"
    # UPDATE_SQL_EVALUATOR=UPDATE_tree_Evaluator(UPDATE_SQL_Grammar,update_query)
    # print(UPDATE_SQL_EVALUATOR.get_result())
    # mySystem.update_data(UPDATE_SQL_EVALUATOR.table_name,UPDATE_SQL_EVALUATOR.update_clause,UPDATE_SQL_EVALUATOR.where_clause)
    # print(mySystem.database_tables)



    # select_query = """
    # SELECT name_age.age,name_height.height 
    # FROM name_age 
    # INNER JOIN name_height 
    # ON name_age.name = name_height.name;
    # """
    # #select_query = "SELECT MAX(age) FROM name_age WHERE name = suzy AND age < 18;"
    # SELECT_SQL_EVALUATOR=new_SELECT_tree_Evaluator(SELECT_SQL_Grammar,select_query)
    # print(SELECT_SQL_EVALUATOR.get_result())
    # print("table_1",SELECT_SQL_EVALUATOR.from_clause[0])
    # print("projection_cols_1",[SELECT_SQL_EVALUATOR.selection_clause['cols'][0]])
    # print(mySystem.nested_loop_join(SELECT_SQL_EVALUATOR.from_clause[0],
    #                           table_1_col=SELECT_SQL_EVALUATOR.option['theta_join_clause'][2],
    #                         table_2 = SELECT_SQL_EVALUATOR.option['theta_join_clause'][0],
    #                         table_2_col=SELECT_SQL_EVALUATOR.option['theta_join_clause'][5],
    #                         projection_cols_1=[SELECT_SQL_EVALUATOR.selection_clause['cols'][0]],
    #                         projection_cols_2=[SELECT_SQL_EVALUATOR.selection_clause['cols'][1]]
    #                         ))
    
    # select_query = """
    # SELECT name,height
    # FROM name_height
    # WHERE height < 165 OR height = 170;
    # """
    # #select_query = "SELECT MAX(age) FROM name_age WHERE name = suzy AND age < 18;"
    # SELECT_SQL_EVALUATOR_new=new_SELECT_tree_Evaluator(SELECT_SQL_Grammar,select_query)
    # print(SELECT_SQL_EVALUATOR_new.get_result())

    # # output = mySystem.order_by(mySystem.database_tables[SELECT_SQL_EVALUATOR_new.from_clause[0]],order_cols=[SELECT_SQL_EVALUATOR_new.option['order_by_clause'][0]],sort=SELECT_SQL_EVALUATOR_new.option['order_by_clause'][1])

    # # print(mySystem.projection(output,SELECT_SQL_EVALUATOR_new.selection_clause['cols']))
    # # print(mySystem.select_where(SELECT_SQL_EVALUATOR_new.from_clause[0],SELECT_SQL_EVALUATOR_new.option['where_clause']))

    # select_query = """
    # SELECT MIN(height),age
    # FROM name_height
    # GROUP BY age HAVING COUNT(age) >1;
    # """
    # #select_query = "SELECT MAX(age) FROM name_age WHERE name = suzy AND age < 18;"
    # SELECT_SQL_EVALUATOR_new=new_SELECT_tree_Evaluator(SELECT_SQL_Grammar,select_query)
    # print(SELECT_SQL_EVALUATOR_new.get_result())
    # simple_data = {
    #     'name' : ['suzy','yuni','john','chelsea','selina','ella'],
    #     'age': [20,20,20,10,14,23],
    #     'height': [120,140,150,150,180,190]
    # }
    # # result = 410,150,180,190
    # #          20 ,10, 14, 23
    # print(SELECT_SQL_EVALUATOR_new.option['group_having_clause'][0])
    # print(SELECT_SQL_EVALUATOR_new.option['group_having_clause'][1:])
    # print(SELECT_SQL_EVALUATOR_new.selection_clause['cols'])
    # print(SELECT_SQL_EVALUATOR_new.selection_clause['agg_fun'])
    # print(mySystem.group_by(simple_data,
    #                         group_columns=[SELECT_SQL_EVALUATOR_new.option['group_having_clause'][0]],
    #                         having_condition=[SELECT_SQL_EVALUATOR_new.option['group_having_clause'][1:]],
    #                         table_cols=SELECT_SQL_EVALUATOR_new.selection_clause['cols'],
    #                         agg_func=SELECT_SQL_EVALUATOR_new.selection_clause['agg_fun']))
    



    # select_query = """
    # SELECT AVG(height),SUM(height)
    # FROM name_height;
    # """
    # #select_query = "SELECT MAX(age) FROM name_age WHERE name = suzy AND age < 18;"
    # SELECT_SQL_EVALUATOR_new=new_SELECT_tree_Evaluator(SELECT_SQL_Grammar,select_query)
    # print(SELECT_SQL_EVALUATOR_new.get_result())
    # print(mySystem.projection(# mySystem.database_tables[SELECT_SQL_EVALUATOR_new.from_clause[0]],
    #                           simple_data,
    #                           SELECT_SQL_EVALUATOR_new.selection_clause['cols'],
    #                           SELECT_SQL_EVALUATOR_new.selection_clause['agg_fun']
    #                           ))
    
    # new_system = System()
    # new_system.open_database("CUSTOMERS")
    # # drop_query="DROP TABLE customer_name;"
    # # DROP_SQL_EVALUATOR=DROP_TABLE_tree_Evaluator(DROP_TABLE_SQL_Grammar,drop_query)
    # # print(DROP_SQL_EVALUATOR.get_result())
    # # print(DROP_SQL_EVALUATOR.table_name)
    # # new_system.drop_table_dict(DROP_SQL_EVALUATOR.table_name)


    # # drop_query="DROP TABLE orders;"
    # # DROP_SQL_EVALUATOR=DROP_TABLE_tree_Evaluator(DROP_TABLE_SQL_Grammar,drop_query)
    # # print(DROP_SQL_EVALUATOR.get_result())
    # # print(DROP_SQL_EVALUATOR.table_name)
    # # new_system.drop_table_dict(DROP_SQL_EVALUATOR.table_name)
    # # print(new_system.database_tables)


    # # update_query="UPDATE name_age SET name = 'YUNI' WHERE age = 13;"

    # # UPDATE_SQL_EVALUATOR=UPDATE_tree_Evaluator(UPDATE_SQL_Grammar,update_query)
    # # print(UPDATE_SQL_EVALUATOR.get_result())
    # # new_system.update_data(UPDATE_SQL_EVALUATOR.table_name,UPDATE_SQL_EVALUATOR.update_clause,UPDATE_SQL_EVALUATOR.where_clause)
    # # print(new_system.database_tables)

    # print(new_system.database_tables)
    # # delete_query="DELETE FROM orders WHERE id < 14;"
    # delete_query="DELETE FROM customer_name WHERE id > 1;"

    # DELETE_SQL_EVALUATOR=DELETE_tree_Evaluator(DELETE_SQL_Grammar,delete_query)
    # print(DELETE_SQL_EVALUATOR.get_result())
    # new_system.delete_data_dict(DELETE_SQL_EVALUATOR.table_name,DELETE_SQL_EVALUATOR.where_clause)
    # # print(new_system.database_tables)

    # insert_query="INSERT INTO customer_name (id,customer_name) VALUES (4,'selina');"
    # INSERT_SQL_EVALUATOR=INSERT_tree_Evaluator(INSERT_SQL_Grammar,insert_query)
    # print(INSERT_SQL_EVALUATOR.get_result())
    # new_system.insert_data(INSERT_SQL_EVALUATOR.table_name,INSERT_SQL_EVALUATOR.insert_cols,INSERT_SQL_EVALUATOR.insert_vals)
    # # print(new_system.database_tables)
    # # print("$$$$$$$$",new_system.foreign_key['foreign_key_1'])
    # # insert_query="INSERT INTO orders (id,customer_id) VALUES (40,3);"

    # insert_query="INSERT INTO orders (id,customer_id) VALUES (40,5);"
    # INSERT_SQL_EVALUATOR=INSERT_tree_Evaluator(INSERT_SQL_Grammar,insert_query)
    # print(INSERT_SQL_EVALUATOR.get_result())
    # new_system.insert_data(INSERT_SQL_EVALUATOR.table_name,INSERT_SQL_EVALUATOR.insert_cols,INSERT_SQL_EVALUATOR.insert_vals)
    # # print(new_system.database_tables)

    # # update_query="UPDATE orders SET customer_id = 4 WHERE id = 10;"
    # update_query="UPDATE orders SET customer_id = 5 WHERE id = 10;"

    # UPDATE_SQL_EVALUATOR=UPDATE_tree_Evaluator(UPDATE_SQL_Grammar,update_query)
    # print(UPDATE_SQL_EVALUATOR.get_result())
    # new_system.update_data(UPDATE_SQL_EVALUATOR.table_name,UPDATE_SQL_EVALUATOR.update_clause,UPDATE_SQL_EVALUATOR.where_clause)
    # # print(new_system.database_tables)
    # # update_query="UPDATE customer_name SET id = 5 WHERE id = 0;"
    # update_query="UPDATE customer_name SET id = 5 WHERE id = 1;"


    # UPDATE_SQL_EVALUATOR=UPDATE_tree_Evaluator(UPDATE_SQL_Grammar,update_query)
    # print(UPDATE_SQL_EVALUATOR.get_result())
    # new_system.update_data(UPDATE_SQL_EVALUATOR.table_name,UPDATE_SQL_EVALUATOR.update_clause,UPDATE_SQL_EVALUATOR.where_clause)
    # print(new_system.database_tables)


    # # select_query = "SELECT age,name,MAX(name) FROM name_age WHERE name='suzy' AND age BETWEEN 12 AND 30;"

    # select_query = '''
    # SELECT name_age.age,name_address.address 
    # FROM name_age 
    # INNER JOIN name_address 
    # ON name_age.name = name_address.name 
    # WHERE name='suzy' AND age BETWEEN 12 AND 30 
    # ORDER BY age ASC;
    # '''

    # # select_query = """
    # # SELECT MIN(height),age
    # # FROM name_height
    # # GROUP BY age HAVING COUNT(age) >1;
    # # """
    # #select_query ="SELECT height FROM name_height ORDER BY height DESC;"
    # #select_query = "SELECT MAX(age) FROM name_age WHERE name = suzy AND age < 18;"
    # SELECT_SQL_EVALUATOR=new_SELECT_tree_Evaluator(SELECT_SQL_Grammar,select_query)
    # print(SELECT_SQL_EVALUATOR.get_result())








    # print("+++++++++++++++++++++++++++")
    # test_system = System()
    # print(test_system.open_database('CUSTOMERS'))
    # delete_query="DELETE FROM customer_name WHERE id < 1;"
    # EXECUTE(test_system,delete_query)
    # print(test_system.database_tables)
    # print("+++++++++++++++++++++++++++")
    # update_query="UPDATE customer_name SET id = 5 WHERE id = 1;"
    # EXECUTE(test_system,update_query)
    # print(test_system.database_tables)
    # drop_query="DROP TABLE customer_name;"
    # EXECUTE(test_system,drop_query)
    # print(test_system.database_tables)
    # insert_query="INSERT INTO customer_name (id,customer_name) VALUES (4,'selina');"
    # EXECUTE(test_system,insert_query)
    # print(test_system.database_tables)
    # select_query_1 = 'SELECT customer_name.customer_name, orders.id FROM orders INNER JOIN customer_name ON orders.customer_id = customer_name.id;'
    # print(EXECUTE(test_system,select_query_1))
    # print(test_system.database_tables)
    # select_query_2 = 'SELECT customer_id, id FROM orders WHERE customer_id > 1 ORDER BY id DESC;'
    # print(EXECUTE(test_system,select_query_2))
    # select_query_3 = 'SELECT COUNT(customer_id) FROM orders WHERE customer_id > 1;'
    # print(EXECUTE(test_system,select_query_3))
    # select_query_4 = 'SELECT MAX(customer_id),id FROM orders WHERE customer_id > 1 GROUP BY id ORDER BY id DESC;'
    # print(EXECUTE(test_system,select_query_4))
    # drop_query="DROP TABLE orders;"
    # EXECUTE(test_system,drop_query)
    # # create_query = "CREATE TABLE orders (id INT PRIMARY KEY, customer_id INT);"

    # # EXECUTE(test_system,create_query)
    # print(test_system.database_tables)
    # create_query = 'CREATE TABLE orders (id INT PRIMARY KEY, customer_id INT, FOREIGN KEY (customer_id) REFERENCES customer_name(id));'
    # EXECUTE(test_system,create_query)
    # print(test_system.database_tables)
    # insert_query="INSERT INTO orders (id,customer_id) VALUES (4,1);"
    # EXECUTE(test_system,insert_query)
    # print(test_system.database_tables)
    # insert_query="INSERT INTO orders (id,customer_id) VALUES (4,5);"
    # EXECUTE(test_system,insert_query)
    # print(test_system.database_tables)
    
    # test_system = System()
    # test_system.init_database("TEST")
    # create_query  = "CREATE TABLE customer_name (id INT PRIMARY KEY, customer_name VARCHAR(10));"
    
    # EXECUTE(test_system,create_query)
    # create_query = 'CREATE TABLE orders (id INT PRIMARY KEY, customer_id INT, FOREIGN KEY (customer_id) REFERENCES customer_name(id));'

    # EXECUTE(test_system,create_query)
    # print(test_system.database_tables)
    # test_system.save_database()
    
    # print(test_system.database_tables)

    # insert_query = "INSERT INTO customer_name (id, customer_name) VALUES (1, 'suzy');"
    # insert_query_1 = "INSERT INTO customer_name (id, customer_name) VALUES (0, 'yuni');"
    # insert_query_2 = "INSERT INTO customer_name (id, customer_name) VALUES (2, 'Selina');"
    # insert_query_3 = "INSERT INTO customer_name (id, customer_name) VALUES (3, 'Chelsea');"


    # insert_query = "INSERT INTO orders (id, customer_id) VALUES (100, 2);"
    # insert_query_1 = "INSERT INTO orders (id, customer_id) VALUES (200, 2);"
    # insert_query_2 = "INSERT INTO orders (id, customer_id) VALUES (300, 2);"
    # insert_query_3 = "INSERT INTO orders (id, customer_id) VALUES (310, 3);"
    # insert_query_4 = "INSERT INTO orders (id, customer_id) VALUES (210, 0);"
    # insert_query_5 = "INSERT INTO orders (id, customer_id) VALUES (290, 1);"






    # EXECUTE(test_system,insert_query)
    # EXECUTE(test_system,insert_query_1)
    # EXECUTE(test_system,insert_query_2)
    # EXECUTE(test_system,insert_query_3)
    # EXECUTE(test_system,insert_query_5)


    # print(test_system.database_tables)

    # select_query_1 = 'SELECT customer_name.customer_name, orders.id FROM orders INNER JOIN customer_name ON orders.customer_id = customer_name.id;'
    # print(EXECUTE(test_system,select_query_1))
    # select_query_2 = 'SELECT customer_id, id FROM orders WHERE customer_id > 1 ORDER BY id DESC;'
    # print(EXECUTE(test_system,select_query_2))
    # select_query_3 = 'SELECT COUNT(customer_id) FROM orders WHERE customer_id > 1;'
    # print(EXECUTE(test_system,select_query_3))
    # select_query_4 = 'SELECT MAX(customer_id),id FROM orders WHERE customer_id > 1 GROUP BY id ORDER BY id DESC;'
    # print(EXECUTE(test_system,select_query_4))

    # test_system.save_database()










    
    