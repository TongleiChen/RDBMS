# Input: line or visual editor
# Parser: 
#    Obtain SQL grammer
#    (Recursive descent) parser
from collections import namedtuple
from lark import Lark
from beautifultable import BeautifulTable
from Data_Definition_Language import System

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
# TODO 语法缺少HAVING join
# TODO 目前只支持where后面只有一个condition
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


    char_num: (NAME | NUMBER)*

    start: [select_statement end]

    end: SEMICOLON

    select_statement: SELECT selection from_clause options*

    selection: ALL
        | num_word_commalist

    num_word_commalist: [max_min] num_word
        | num_word_commalist COMMA [max_min] num_word
        | [sum] num_word
        

    num_word: column_table
        | LEFT_PAREN num_word RIGHT_PAREN
        | item

    options: where_clause
        | order_by_clause

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

    item: QUOTE char_num QUOTE

    order_by_clause: ORDER BY order_by_commalist

    order_by_commalist: order_by | order_by_commalist COMMA order_by

    order_by: column_table asc_desc

    asc_desc: ASC | DESC

    max_min: MAX | MIN 
    
    sum: SUM
"""
# CREATE GRAMMAR
CREATE_SQL_Grammar = """
    %import common.CNAME
    %import common.SIGNED_NUMBER
    %import common.WS
    %ignore WS
    
    start: [create_statement end]
    
    create_statement: "CREATE TABLE" table_name "(" field_list ")" 
    
    table_name: CNAME

    field_list: field_definition ("," field_definition)*

    field_definition: attribute_name data_type field_constraints?
    
    attribute_name: CNAME

    INT: "INT"
    FT: "FLOAT"
    VCHR: "VARCHAR" "(" /[0-9]+/ ")"
    data_type: INT | FT | VCHR

    NOTNULL: "NOT NULL"
    KEY: "PRIMARY KEY" | "FOREIGN KEY"
    field_constraints: NOTNULL | KEY
    
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

    start: [update_statement end]
    end: ";"

    update_statement: "UPDATE" table_name "SET" update_list where_clause?

    update_list: update_clause ("," update_clause)*

    update_clause: column_name "=" new_value

    new_value: CNAME | NUMBER

    where_clause: "WHERE" search_condition

    search_condition: (column_name | new_value) comparison_operator (column_name | new_value)
    
    EQUAL: "="
    LT: "<"
    GT: ">"
    LTE: "<="
    GTE: ">="
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
    
    value: CNAME | NUMBER
    
    NUMBER: SIGNED_NUMBER+

    table_name: CNAME

"""

class SELECT_tree_Evaluator:
    def __init__(self,grammar,query) -> None:
        self.parser = Lark(grammar)
        self.query=query
        self.result=BeautifulTable()
        self.datatable = {}

    def get_result(self,datatable):
        self.datatable = datatable
        tree=self.parser.parse(self.query)
        self.eval_tree(tree)
        print(tree.pretty())
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
        print("table to be updated: ",self.table_name)
        print("update clause: ",self.update_clause)
        print("where clause: ",self.where_clause)
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

class DROP_tree_Evaluator:
    def __init__(self,grammar,query) -> None:
        self.parser = Lark(grammar)
        self.query=query
        self.result=BeautifulTable()
        self.table_name=None # Name of Table being dropped
        
    def get_result(self):
        tree=self.parser.parse(self.query)
        self.eval_tree(tree)
        # TODO to get results
        print("table to be dropped: ",self.table_name)
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
        print("table to be inserted: ",self.table_name)
        print("insert columns: ",self.insert_cols)
        print("insert valuse: ",self.insert_vals)
        
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
        print("table to be delete: ",self.table_name)
        print("where clause: ",self.where_clause)
        print("cascade flag: ",self.cascade_flag)
        

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
        
class CREATE_tree_Evaluator:
    def __init__(self,grammar,query) -> None:
        self.parser = Lark(grammar)
        self.query=query
        self.result=BeautifulTable()
        self.table_name=None # Name of Table being create
        self.attributes_clause={"names":[],"data_type":[],"constraints":[]} # Where clause list
    def get_result(self):
        tree=self.parser.parse(self.query)
        self.eval_tree(tree)
        # TODO to get results
        print("table to be delete: ",self.table_name)
        print("attributes clause: ",self.attributes_clause)
        ...
        return self.result
    def eval_tree(self,tree):
        if tree.data == "start":
            self.eval_tree(tree.children[0]) # create_statement
        elif tree.data == "create_statement":
            self.table_name=tree.children[0].children[0] # table_name
            self.eval_tree(tree.children[1]) # field_list
        elif tree.data == "field_list":
            for child in tree.children:
                self.eval_tree(child) # field definition
        elif tree.data == "field_definition":
            self.attributes_clause["names"].append(tree.children[0].children[0].value)
            self.attributes_clause["data_type"].append(tree.children[1].children[0].value)
            if len(tree.children)==2:
                self.attributes_clause["constraints"].append("None")
            elif len(tree.children)==3:
                self.attributes_clause["constraints"].append(tree.children[2].children[0].value)

        else:
            raise ValueError(f"Invalid syntax query")
        
if __name__=='__main__':
    # 1. select grammar
    # 0410 tested

    mySystem=System()
    mySystem.open_database('CLASS')
    select_query = "SELECT age FROM name_age WHERE age < 18;" #where的顺序只能跟列表的顺序一致：
    #select_query = "SELECT MAX(age) FROM name_age WHERE name = suzy AND age < 18;"
    SELECT_SQL_EVALUATOR=SELECT_tree_Evaluator(SELECT_SQL_Grammar,select_query)
    print(SELECT_SQL_EVALUATOR.get_result(datatable=mySystem.get_data("name_age")))

    
    # 2. create grammar
    create_query = """
    CREATE TABLE customers (
    id INT PRIMARY KEY,
    name VARCHAR(50) NOT NULL,
    age INT PRIMARY KEY,
    email VARCHAR(100) NOT NULL
    );
    """
    CREATE_SQL_EVALUATOR=CREATE_tree_Evaluator(CREATE_SQL_Grammar,create_query)
    print(CREATE_SQL_EVALUATOR.get_result())

    # 3. drop grammar
    # 0410 tested
    drop_query="DROP TABLE mybook;"
    DROP_SQL_EVALUATOR=DROP_tree_Evaluator(DROP_SQL_Grammar,drop_query)
    print(DROP_SQL_EVALUATOR.get_result())

    # 4. update grammar
    # 0410 tested
    update_query="UPDATE my_table SET column1 = suzy, column2 = 3 WHERE column3 >= 10;"
    UPDATE_SQL_EVALUATOR=UPDATE_tree_Evaluator(UPDATE_SQL_Grammar,update_query)
    print(UPDATE_SQL_EVALUATOR.get_result())

    
    # INSERT grammar
    # 0410 tested
    insert_query="INSERT INTO name_age (name, age) VALUES ('John', 30);"
    INSERT_SQL_EVALUATOR=INSERT_tree_Evaluator(INSERT_SQL_Grammar,insert_query)
    print(INSERT_SQL_EVALUATOR.get_result())
    
    data = {}
    for i,column in enumerate(INSERT_SQL_EVALUATOR.insert_cols):
        data[column] = INSERT_SQL_EVALUATOR.insert_vals[i]
    print(data)
    # mySystem.insert_data(INSERT_SQL_EVALUATOR.table_name,data)


    # DELETE grammar
    # 0410 tested
    delete_query="DELETE FROM customers WHERE age < 18 CASCADE;"
    DELETE_SQL_EVALUATOR=DELETE_tree_Evaluator(DELETE_SQL_Grammar,delete_query)
    print(DELETE_SQL_EVALUATOR.get_result())



    # update_query_2 = "UPDATE name_height SET height = 160 WHERE name = 'suzy';"
    # UPDATE_SQL_EVALUATOR_2=UPDATE_tree_Evaluator(UPDATE_SQL_Grammar,update_query_2)
    # DELETE_SQL_EVALUATOR.get_result()
    # mySystem.update_data(UPDATE_SQL_EVALUATOR_2.table_name,UPDATE_SQL_EVALUATOR_2.update_clause,UPDATE_SQL_EVALUATOR_2.where_clause)
    # print(mySystem.database_tables)

    # update_query="UPDATE name_height SET name = 'suzy' WHERE column3 = 10;"
    # UPDATE_SQL_EVALUATOR=UPDATE_tree_Evaluator(UPDATE_SQL_Grammar,update_query)
    # print(UPDATE_SQL_EVALUATOR.get_result())




    insert_query="INSERT INTO name_age (name, age) VALUES ('Chelsea', 18);"
    INSERT_SQL_EVALUATOR=INSERT_tree_Evaluator(INSERT_SQL_Grammar,insert_query)
    print(INSERT_SQL_EVALUATOR.get_result())

    mySystem.insert_data(INSERT_SQL_EVALUATOR.table_name,INSERT_SQL_EVALUATOR.insert_cols,INSERT_SQL_EVALUATOR.insert_vals)
    print(mySystem.database_tables)


    update_query="UPDATE name_age SET age = 150 WHERE age = 13;"
    UPDATE_SQL_EVALUATOR=UPDATE_tree_Evaluator(UPDATE_SQL_Grammar,update_query)
    print(UPDATE_SQL_EVALUATOR.get_result())
    mySystem.update_data(UPDATE_SQL_EVALUATOR.table_name,UPDATE_SQL_EVALUATOR.update_clause,UPDATE_SQL_EVALUATOR.where_clause)
    print(mySystem.database_tables)


