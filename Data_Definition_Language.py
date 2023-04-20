from collections import namedtuple
import os
import shutil
import copy
from BTrees.OOBTree import OOBTree



# YUNI: 一边骂别人不写注释一边自己不写注释的我本人

class System:
    # Maintain two “tables”: TABLES & COLUMNS
    # the system can only contain one database
    def __init__(self) -> None:
        self.database_name = None
        self.tables_filepath = None
        self.columns_filepath = None
        self.table_path = {}
        self.table_attributes = {} # dict: "table_1" = {'column_1':['INT','True'],'column_2':['STRING','False']}
        self.database_tables = {}
        self.table_index = {}
        self.index_table_name = {} #useless right now


    
    def Create_Database(self,database_name):
        # YUNI: 0408 Tested
        self.database_name = database_name

        if os.path.isdir('./' + self.database_name):
            print(self.database_name," IS ALREADY EXIST. ")


        else:
            os.mkdir(self.database_name)
            self.tables_filepath = os.path.join(self.database_name,'TABLES.csv')
            # YUNI: 从来没有这么一天这么爱csv
            with open(self.tables_filepath,'w') as f:
                f.write('table_name\n')

            # YUNI: 所以说 所有table的columns都放在同一个文件夹对吧！
            self.columns_filepath = os.path.join(self.database_name,'COLUMNS.csv')

            with open(self.columns_filepath,'w') as f:

                # TODO:
                # Foreign key?
                f.write('table_name, attribute_name, data_type, primary_key, foreign_key\n')
            
            print(self.database_name, "CREATED SUCCESSFULLY. ")


    def open_database(self,database_name):
        # YUNI: 0408 Tested
        self.database_name = database_name
        self.tables_filepath = os.path.join(self.database_name,'TABLES.csv')
        self.columns_filepath = os.path.join(self.database_name,'COLUMNS.csv')
        with open(self.tables_filepath,'r') as f:
            f.readline()
            for line in f.readlines():
                line = line.strip("\n")
                self.table_path[line] = os.path.join(self.database_name,line+'.csv')
        with open(self.columns_filepath,'r') as f:
            f.readline()
            for line_2 in f.readlines():
                line_2 = line_2.strip("\n")
                line_2 = line_2.split(",") 
                
                if line_2[0] in self.table_attributes:

                    self.table_attributes[line_2[0]][line_2[1]] = line_2[2:]
                else:
                    self.table_attributes[line_2[0]] = {line_2[1]:line_2[2:]}

        print(self.table_attributes)
        # print(self.table_path)
        for table in self.table_path.keys():
            print(table)
            self.database_tables[table] = self.get_data(table)
        return self.database_tables
    
    def Drop_Database(self):
        # YUNI: 0408 Tested
        if os.path.isdir('./' + self.database_name):
            # remove table

            shutil.rmtree('./' + self.database_name)



            print(self.database_name," DROPPED SUCCESSFULLY. ")

        else: 
            print(self.database_name," HAS ALREADY BEEN DROPPED. ")



    def Create_Table(self,relation_name:str,attribute_list:list):
        # YUNI: 0408 Tested
        # YUNI: 0414 TODO: NEED TO BE REWRITE
        current_table_path = os.path.join(self.database_name,relation_name+'.csv')
        if not os.path.exists(current_table_path):
            self.table_path[relation_name] = current_table_path



            # write relation name data to TABLES.csv
            with open(self.tables_filepath,'r+') as f:
                f.seek(0, 2)
                f.write(relation_name+'\n')

            # write attribute data to COLUMNS.csv
            with open(self.columns_filepath,'r+') as f_c:
                f_c.seek(0,2)
                for attr in attribute_list:
                    formatted_attribute = relation_name + ","
                    formatted_attribute += ','.join(map(str, attr))
                    f_c.write(formatted_attribute+'\n')
            



            # create csv to store data in table
            with open(current_table_path,'w') as f_t:
                attr_name_list = []
                for attr in attribute_list:
                    attr_name_list.append(attr[0])

                f_t.write(",".join(attr_name_list)+'\n')

            self.table_attributes[relation_name] = attribute_list
            print(relation_name, "CREATED SUCCESSFULLY ")


        else:
            print(relation_name, "ALREADY EXISTS. ")

        return
    
    
    def create_table_dict(self,relation_name:str,attribute:dict):

        # YUNI: 0415 TESTED
        if relation_name in self.table_path:
            print("CREATE ERROR: Table already exists.")
            return
            
        current_table_path = os.path.join(self.database_name,relation_name+'.csv')
        col_names = attribute['names']
        col_data_type = attribute['data_type']
        col_constraints = attribute['constraints']
        # add to path
        self.table_path[relation_name] = current_table_path

        # add attributes
        self.table_attributes[relation_name] = {}
        self.database_tables[relation_name] = {}
        for col_idx,col_name_i in enumerate(col_names):
            self.database_tables[relation_name][col_name_i] = []
            if col_data_type[col_idx] == 'INT':
                self.table_attributes[relation_name][col_name_i] = [col_data_type[col_idx]]
            else:
                self.table_attributes[relation_name][col_name_i] = ['STR']
            if col_constraints[col_idx] == 'PRIMARY KEY':
                self.table_attributes[relation_name][col_name_i].append("True")
            else:
                self.table_attributes[relation_name][col_name_i].append("False")
        # add to database
        

        return
    
    def drop_table_dict(self,relation_name:str):

        # YUNI: 0415 TESTED
        # delete path
        # delete attributes
        # delete database
        if relation_name not in self.table_path:
            print("DROP ERROR: Table does not exist.")
            return
        del self.table_path[relation_name]
        del self.table_attributes[relation_name]
        del self.database_tables[relation_name]
        
        return


    def Drop_Table(self,relation_name):
        # YUNI: 0408 Tested
        # YUNI: 0414 TODO: NEED TO BE REWRITE
        if relation_name in self.table_path:

            os.remove(self.table_path[relation_name])

            print(relation_name, "DROPPED SUCCESSFULLY. ")
            del self.table_path[relation_name]
            del self.table_attributes[relation_name]
            write_lines = []


            with open(self.columns_filepath,'r') as f:
                for table_line in f.readlines():
                    if table_line.split(",")[0] == relation_name:
                        continue
                    else:
                        write_lines.append(table_line)
            with open(self.columns_filepath,'w') as f:
                f.write("".join(write_lines))

            write_lines = []


            with open(self.tables_filepath,'r') as f:
                for tables in f.readlines():
                    if tables[:-1] == relation_name:
                        continue
                    else:
                        write_lines.append(tables)
            with open(self.tables_filepath,'w') as f:
                f.write("".join(write_lines))



            


        else:
            print(relation_name, "DOES NOT EXIST. ")
        

        return
    


    # def insert_data(self,relation_name,data:dict):
    #     # TODO: 
    #     # Check duplicates 
    #     # YUNI: 是在这里check还是在到这里之前check比较好？？
    #     current_table_path = self.table_path[relation_name]
    #     attribute_list = self.table_attributes[relation_name]
    #     data_list = []
    #     for attri in attribute_list:
    #         print(attri)
    #         data_list.append(data[attri[0]])

        
    #     with open(current_table_path,'r+') as f_t:
    #         f_t.seek(0,2)
    #         f_t.write(",".join(map(str, data_list))+"\n")

    #     print("INSERT SUCCESSFULLY. ")
    #     return
    def insert_data(self,relation_name,insert_cols,insert_vals):


        # check duplicates

        primary_key_list = self.find_primary_key(relation_name)
        inserted_primary_key = []
        for i,column in enumerate(insert_cols):
            if column in primary_key_list:
                try_insert_list = copy.deepcopy(self.database_tables[relation_name][column])
                if self.table_attributes[relation_name][column][0] == 'INT':
                    try_insert_list.append(int(insert_vals[i]))
                else:
                    try_insert_list.append(insert_vals[i])
                inserted_primary_key.append(try_insert_list)
        if self.check_duplicates(inserted_primary_key) == True:
            print("Insertion ERROR: There exists DUPLICATES. ")
            return
        # TODO: insert_col not null check?
        for i,column in enumerate(insert_cols):
            if self.table_attributes[relation_name][column][0] == 'INT':

                self.database_tables[relation_name][column].append(int(insert_vals[i]))
            else:
                self.database_tables[relation_name][column].append(insert_vals[i])


        
        # TODO: NEED TO BE TESTED 0417
        if relation_name in self.table_index:
            # add index for the inserted data
            inserted_index_value = len(self.database_tables[relation_name][column])
            primary_key = primary_key_list[0]
            inseted_index_key = self.database_tables[relation_name][primary_key][inserted_index_value]
            self.table_index[relation_name].setdefault(inseted_index_key,inserted_index_value)
            

        
        return

    def find_primary_key(self,relation_name):
        primary_key = []

        for attr in self.table_attributes[relation_name].keys():#name,type,primary key
            if self.table_attributes[relation_name][attr][1] == 'True':
                primary_key.append(attr)
        return primary_key
    
    def check_duplicates(self,primary_column_list):
        # return False -> no duplicates
        # return True -> has duplicates
        if len(primary_column_list) == 1:
            if len(primary_column_list[0]) == len(set(primary_column_list[0])):
                return False
        else:
            primary_key_pairs = list(zip(*primary_column_list))
            if len(primary_key_pairs) == len(set(primary_key_pairs)):
                return False
        return True
        


    def delete_data_dict(self,relation_name:str, where_dict:dict):
        # YUNI: 0415 TESTED
        table_attri = self.get_column_list(relation_name)

        total_number_row = len(self.database_tables[relation_name][table_attri[0]])
        
        where_col = where_dict['cols'][0]

        if self.table_attributes[relation_name][where_col][0] == 'INT':
            where_val = int(where_dict['vals'][0])
        else:
            where_val = where_dict['vals'][0]


        where_op = where_dict['ops'][0]
        # print("&&&",where_op)


        delete_row_list = []
        for i in range(total_number_row):
            match_flag = False
            if where_op == "=":

                if self.database_tables[relation_name][where_col][i] == where_val:
                    # print("((((",self.database_tables[relation_name][where_col][i],where_val)
                    match_flag = True

            if where_op == ">":
                if self.database_tables[relation_name][where_col][i] > where_val:
                    match_flag = True


            if where_op == ">=":
                if self.database_tables[relation_name][where_col][i] >= where_val:
                    match_flag = True


            if where_op == "<":
                if self.database_tables[relation_name][where_col][i] < where_val:
                    match_flag = True

            if where_op == "<=":
                if self.database_tables[relation_name][where_col][i] <= where_val:
                    match_flag = True
            
            if match_flag == True:
                delete_row_list.append(i)
        # print("###",delete_row_list)
        
        delete_row_list_reverse = delete_row_list[::-1]
        for delete_idx in delete_row_list_reverse:
            for column in self.table_attributes[relation_name].keys():
                del self.database_tables[relation_name][column][delete_idx]
        
        # update index key after the first delete row!!
        # TODO: NEED TO BE TESTED 0417
        if relation_name in self.table_index:
            delete_row_list_first = delete_row_list[0]
            for update_idx_value in range(delete_row_list_first,total_number_row):
                update_idx_key = self.database_tables[relation_name][column][update_idx_value]
                self.table_index[relation_name][update_idx_key] = update_idx_value
            
        return
        
    def delete_data(self,relation_name,data_pos):
        # YUNI: 0408 Tested
        
        current_table_path = self.table_path[relation_name]
        new_data = []
        with open(current_table_path,"r") as f:
            for pos,data_line in enumerate(f.readlines()):
                # YUNI: 这里data_pos 是不包括标题那一行的第几行
                if pos == data_pos + 1:
                    continue
                else:
                    new_data.append(data_line)
        with open(current_table_path,"w") as f:
            f.writelines(new_data)
        

        return
    
    def get_column_list(self,relation_name):
        return list(self.table_attributes[relation_name].keys())
    

    def overwrite_data(self,relation_name,data_dict:dict):
        current_table_path = self.table_path[relation_name]
        new_data = []
        attribute_list = self.table_attributes[relation_name]
        table_attr = []
        for attri in attribute_list:
            table_attr.append(attri[0])
        new_data.append(",".join(table_attr)+'\n')

        total_number_row = len(data_dict[attri[0]])
        for i in range(total_number_row):
            new_line = []
            for attri in attribute_list:
                new_line.append(data_dict[attri[0]][i])
            new_data.append(",".join(map(str,new_line))+'\n')
        
        with open(current_table_path,"w") as f:
            f.writelines(new_data)
        
        return
    

    def update_data(self,relation_name,update_dict:dict,where_dict:dict):

        # YUNI: unknown data TESTED
        # Where only 1 condition

        # database_table = self.get_data(relation_name)
        table_attri = self.get_column_list(relation_name)

        total_number_row = len(self.database_tables[relation_name][table_attri[0]])
        
        where_col = where_dict['cols'][0]

        if self.table_attributes[relation_name][where_col][0] == 'INT':
            where_val = int(where_dict['vals'][0])
        else:
            where_val = where_dict['vals'][0]

        for col_idx,col in enumerate(update_dict['cols']):
            if self.table_attributes[relation_name][col][0] == 'INT':
                update_dict['vals'][col_idx] = int(update_dict['vals'][col_idx])
        
        where_op = where_dict['ops'][0]
        # print("&&&",where_op)
        update_row_list = []
        for i in range(total_number_row):
            match_flag = False
            if where_op == "=":

                if self.database_tables[relation_name][where_col][i] == where_val:
                    # print("((((",self.database_tables[relation_name][where_col][i],where_val)
                    match_flag = True

            if where_op == ">":
                if self.database_tables[relation_name][where_col][i] > where_val:
                    match_flag = True


            if where_op == ">=":
                if self.database_tables[relation_name][where_col][i] >= where_val:
                    match_flag = True


            if where_op == "<":
                if self.database_tables[relation_name][where_col][i] < where_val:
                    match_flag = True

            if where_op == "<=":
                if self.database_tables[relation_name][where_col][i] <= where_val:
                    match_flag = True
            
            if match_flag == True:
                update_row_list.append(i)
        print("###",update_row_list)
        
        primary_key_list = self.find_primary_key(relation_name)
        primary_update_list = []
        for check_i,column in enumerate(update_dict['cols']):
            if column in primary_key_list:
                try_update_list = copy.deepcopy(self.database_tables[relation_name][column])
                for row_idx in update_row_list:
                    try_update_list[row_idx] = update_dict['vals'][check_i]
                primary_update_list.append(try_update_list)
        if self.check_duplicates(primary_update_list) == True:
            print("Insertion ERROR: There exists DUPLICATES. ")
            return
        for update_idx in update_row_list:
            for j,column in enumerate(update_dict['cols']):
                print(self.database_tables[relation_name][column])
                print(update_dict['vals'])
                self.database_tables[relation_name][column][update_idx] = update_dict['vals'][j]


                # TODO: NEED TO BE TESTED 0417
                if (relation_name in self.table_index) and (column in primary_key_list):
                    updated_row_num = self.table_index[relation_name].pop(column) # should equal to j
                    self.table_index[relation_name].setdefault(update_dict['vals'][j],updated_row_num)

                
        return
        



            
        




        # current_table_path = self.table_path[relation_name]
        
        # attribute_list = self.table_attributes[relation_name]
        # data_list = []
        # for attri in attribute_list:
        #     data_list.append(data[attri[0]])
        # updated_data = ",".join(map(str,data_list)) + '\n'
        # new_data = []
        # with open(current_table_path,"r") as f:
        #     for pos,data_line in enumerate(f.readlines()):
        #         # YUNI: 这里data_pos 是不包括标题那一行的第几行
        #         if pos == data_pos + 1:
        #             new_data.append(updated_data)
        #             continue
        #         else:
        #             new_data.append(data_line)
        # with open(current_table_path,"w") as f:
        #     f.writelines(new_data)
        # return

        return
    
    def get_data(self,relation_name):
        # YUNI: 0408 Tested
        # YUNI: 不知道return 应该是list还是dict还是什么 -0408
        # YUNI: 看了Suzy的code觉得应该是return一个dict -0411
        # YUNI: 0411 Edited
        current_table_path = self.table_path[relation_name]
        data_in_table = []
        data_attributes = []
        for attr in self.table_attributes[relation_name]:
            data_attributes.append(attr)
        with open(current_table_path,"r") as f:
            f.readline()
            for data_line in f.readlines():
                data_line = data_line.strip('\n')
                data_in_table.append(data_line.split(","))
        # print(data_in_table)
        # print(data_attributes)
        data_dict = {}
        for i,column in enumerate(data_attributes):
            data_dict[column] = []
            for data_row in data_in_table:
                if self.table_attributes[relation_name][column][0] == "INT":
                    data_dict[column].append(int(data_row[i]))
                else:
                    data_dict[column].append(data_row[i])

            
        return data_dict
    

    def create_index(self,relation_name,index_name):
        # only one primary key
        primary_key = self.find_primary_key(relation_name)[0]
        index_tree = OOBTree()
        for col_idx,primary_key_num in enumerate(self.database_tables[relation_name][primary_key]):
            index_tree.setdefault(primary_key_num,col_idx)
        self.table_index[relation_name] = index_tree
        self.index_table_name[index_name] = relation_name

        return 
    
    def drop_index(self,relation_name,index_name):
        del self.index_table_name[index_name]
        del self.table_index[relation_name]
        return
    
    def get_row_num(self,relation_name):
        first_attri = self.get_column_list(relation_name)[0]

        total_number_row = len(self.database_tables[relation_name][first_attri])

        return total_number_row
    #####################################################
    ############    虔诚地给select开辟一块地    ############
    #####################################################

    def select_data(self,selection_clause,from_clause,option):
        select_columns = selection_clause['cols']
        col_functions = selection_clause['agg_fun']
        # theta_join = option['theta_join_clause']

    def nested_loop_join(self,table_1:str, table_1_col:str, table_2:str, table_2_col:str,projection_cols_1:list,projection_cols_2:list):
        # only support join grammar like "SELECT table_1.column, table_2.column from table_1 INNER JOIN table_2 ON table_1.column2 = table_2.column2"


        # YUNI 0419 TESTED!
        row_num_1 = self.get_row_num(table_1)
        row_num_2 = self.get_row_num(table_2)
        new_table = {}  # structure similar to self.database_tables[relation_name] {'column_1':[],'column_2':[]}
        for c_1 in projection_cols_1:
            new_table[c_1] = []
        for c_2 in projection_cols_2:
            new_table[c_2] = []
        
        for r_1 in range(row_num_1):
            value_1 = self.database_tables[table_1][table_1_col][r_1]
            for r_2 in range(row_num_2):
                value_2 = self.database_tables[table_2][table_2_col][r_2]
                if value_1 == value_2:
                    pass
                else:
                    continue
                 # when come to this line, join two tables
                for c_1 in projection_cols_1:
                    new_table[c_1].append(self.database_tables[table_1][c_1][r_1])
                for c_2 in projection_cols_2:
                    new_table[c_2].append(self.database_tables[table_2][c_2][r_2])
                
        return new_table
    


    def order_by(self,table_data:dict,order_cols:list,sort:str):
        
        # there should be only one key in order_col right now
        # YUNI 0419 Tested
        new_table = {}
        for column in table_data.keys():
            new_table[column] = []
        order_list = []
        order_col = order_cols[0]
        row_number = len(table_data[order_col]) # get the number of rows(how many lines of data)
        for i in range(row_number):
            # for column in order_cols:
            order_list.append((table_data[order_col][i],i))
        if sort == "ASC":
            sorted_order_list = sorted(order_list,key = lambda x:x[0])
        else:# DESC
            sorted_order_list = sorted(order_list,key = lambda x:x[0], reverse=True)
        for val,idx in sorted_order_list:
            for column in table_data.keys():
                new_table[column].append(table_data[column][idx])
        return new_table

    def select_where_from_output(self,data_table:dict,conditions:list):
        # YUNI 0419 TESTED
        # single table, condition at most 2.
        selected_table = {}
        selected_row_num = []
        columns_list = list(data_table.keys())
        row_num = len(data_table[columns_list[0]])
        if len(conditions) == 1:
            condition = conditions[0]
            if len(condition) == 3: # ['name', '=', 'suzy']
                col = condition[0]
                op = condition[1]
                try:
                    val = int(condition[2])
                except:
                    val = condition[2]
                for r in range(row_num):
                    value_in_row = data_table[col][r]
                    print(value_in_row)
                    if op == ">":
                        if value_in_row > val:
                            pass
                        else:
                            continue
                    elif op == ">=":
                        if value_in_row >= val:
                            pass
                        else:
                            continue
                    elif op == "=":
                        if value_in_row == val:
                            pass
                        else:
                            continue
                    elif op == "<=":
                        if value_in_row <= val:
                            pass
                        else:
                            continue
                    elif op == "<":
                        if value_in_row < val:
                            pass
                        else:
                            continue
                    # when come to this line: meet the requirement and select
                    selected_row_num.append(r)
            else: # ['age', 'BETWEEN', '12', 'AND', '30']
                col = condition[0]
                # the sql grammar said that former should be smaller than later
                try:
                    lower_bound = int(condition[2])
                except:
                    lower_bound = condition[2]
                try:
                    upper_bound = int(condition[4])
                except:
                    upper_bound = condition[4]
                
                for r in range(row_num):
                    value_in_row = data_table[col][r]
                    if (lower_bound <= value_in_row) and (value_in_row <= upper_bound):
                        selected_row_num.append(r)
                    else:
                        continue
            for column in columns_list:
                selected_table[column] = []
            for selected_r in selected_row_num:
                # no projection
                columns_list = list(data_table.keys())
                
                for column in columns_list:
                    selected_table[column].append(data_table[column][selected_r])
            return selected_table,selected_row_num
        else: # len(conditions) == 3 # [['name', '=', 'suzy'],'AND',['age', 'BETWEEN', '12', 'AND', '30']]
            condition_1 = conditions[0]
            operation = conditions[1]
            condition_2 = conditions[2]

            # TODO: optimization here!!!
            # TODO: primary key with index
            if operation == "AND":

                data_table_output,_ = self.select_where_from_output(data_table,[condition_1]) # meet with condition1 and meet with condition2
                data_table_output_2,_ = self.select_where_from_output(data_table_output,[condition_2])
                return data_table_output_2
            else: # operation == "OR": condition_1 = True
                 # IN condition_1 = False find condition_2 = True
                 # 好像要加一个参数要不要输出data_table_output
                 # 明天再确认一下 现在脑子不太清醒 又感觉好像不用
                _,selected_row_list = self.select_where_from_output(data_table,[condition_1])
                columns_list_ = list(data_table.keys())
                row_num_ = len(data_table[columns_list_[0]])
                not_selected_list = [i for i in range(row_num_)] - selected_row_list
                data_remain = {}
                for column in columns_list:
                    data_remain[column] = []
                for r_not_s in not_selected_list:
                    for column in columns_list:
                        data_remain[column].append(data_table[column][r_not_s])
                    
                _,selected_row_list_2 = self.select_where_from_output(data_remain,[condition_2])
                selected_row_total = set(selected_row_list + selected_row_list_2)
                for column in columns_list:
                    selected_table[column] = []
                for s_t in selected_row_total:
                    for column in columns_list:
                        selected_table[column].append(data_table[column][s_t])
                return selected_table
    


                    





            


                

    def select_where(self,relation_name:str,conditions:list):
        # YUNI 0419 TESTED

        # single table(no join), condition at most 2.
        selected_table = {} # similar to self.database_tables[relation_name]
        # [['name', '=', 'suzy'],'AND',['age', 'BETWEEN', '12', 'AND', '30']]
        selected_row_num = []
        # not using index
        if len(conditions) == 1:
            # one condition
            
            condition = conditions[0]
            # print(relation_name)
            if len(condition) == 3: # ['name', '=', 'suzy']
                
                col = condition[0]
                op = condition[1]
                # TODO: need to think twice
                try:
                    val = int(condition[2])
                except:
                    val = condition[2]
                row_num = self.get_row_num(relation_name)
                for r in range(row_num):
                    value_in_row = self.database_tables[relation_name][col][r]
                    if op == ">":
                        # print("here")
                        # print(value_in_row)
                        if value_in_row > val:
                            
                            pass
                        else:
                            continue
                    elif op == ">=":
                        if value_in_row >= val:
                            pass
                        else:
                            continue
                    elif op == "=":
                        if value_in_row == val:
                            pass
                        else:
                            continue
                    elif op == "<=":
                        if value_in_row <= val:
                            pass
                        else:
                            continue
                    elif op == "<":
                        if value_in_row < val:
                            pass
                        else:
                            continue
                    # when come to this line: meet the requirement and select
                    selected_row_num.append(r)
            else: # ['age', 'BETWEEN', '12', 'AND', '30']
                col = condition[0]
                # the sql grammar said that former should be smaller than later
                try:
                    lower_bound = int(condition[2])
                except:
                    lower_bound = condition[2]
                try:
                    upper_bound = int(condition[4])
                except:
                    upper_bound = condition[4]
                
                row_num = self.get_row_num(relation_name)
                for r in range(row_num):
                    value_in_row = self.database_tables[relation_name][col][r]
                    if (lower_bound <= value_in_row) and (value_in_row <= upper_bound):
                        selected_row_num.append(r)
                    else:
                        continue
            columns_list = self.get_column_list(relation_name)
            for column in columns_list:
                selected_table[column] = []
            for selected_r in selected_row_num:
                # no projection
                
                
                for column in columns_list:
                    selected_table[column].append(self.database_tables[relation_name][column][selected_r])
            return selected_table,selected_row_num

        else: # two conditions 
            condition_1 = conditions[0]
            operation = conditions[1]
            condition_2 = conditions[2]

            # TODO: optimization here!!!
            # TODO: primary key with index
            if operation == "AND":

                data_table_output,_ = self.select_where(relation_name,[condition_1]) # meet with condition1 and meet with condition2
                data_table_output_2,_ = self.select_where_from_output(data_table_output,[condition_2])
                return data_table_output_2
            else: # operation == "OR": condition_1 = True
                 # IN condition_1 = False find condition_2 = True
                 # TODO: 
                 # 好像要加一个参数要不要输出data_table_output
                 # 明天再确认一下 现在脑子不太清醒 又感觉好像不用
                _,selected_row_list = self.select_where(relation_name,[condition_1])
                columns_list = self.get_column_list(relation_name)
                row_num_ = self.get_row_num(relation_name)
                not_selected_list = [x for x in range(row_num_) if x not in selected_row_list]
                data_remain = {}
                for column in columns_list:
                    data_remain[column] = []
                for r_not_s in not_selected_list:
                    for column in columns_list:
                        data_remain[column].append(self.database_tables[relation_name][column][r_not_s])
                    
                _,selected_row_list_2 = self.select_where_from_output(data_remain,[condition_2])
                selected_row_total = set(selected_row_list + selected_row_list_2)
                for column in columns_list:
                    selected_table[column] = []
                for s_t in selected_row_total:
                    for column in columns_list:
                        selected_table[column].append(self.database_tables[relation_name][column][s_t])
                return selected_table

            


            
        # TODO: using index
            

    def projection(self,data_table:dict,cols:list,agg_fun:list):
        # YUNI 0419 TESTED
        # YUNI 0420 EDITED
        #  
        pro_data_table = {}
        agg_flag = None
        # 1. agg_fun all has value
        # OR
        # 2. agg_fun all not have value
        for c_agg in agg_fun:
            if c_agg != None: # have agg_fun
                if agg_flag == False:
                    # TODO: raise error
                    print("PROJECTION ERROR: In aggregated query without GROUP BY, SELECT list contains nonaggregated column. ")
                    return
                agg_flag = True
            else: # dont have agg_fun (if c_agg == None)
                if agg_flag == True:
                    # TODO: raise error
                    print("PROJECTION ERROR: In aggregated query without GROUP BY, SELECT list contains nonaggregated column. ")
                    return
                agg_flag = False
        if agg_flag == False:
            for col in cols:
                pro_data_table[col] = data_table[col]
        else: #agg_flag == True:
            for col_idx,col in enumerate(cols):
                current_agg = agg_fun[col_idx]
                col_name = "{}({})".format(current_agg,col)
                if current_agg == "SUM":
                    pro_data_table[col_name] = [sum(data_table[col])]
                elif current_agg == "MAX":
                    pro_data_table[col_name] = [max(data_table[col])]
                elif current_agg == "MIN":
                    pro_data_table[col_name] = [min(data_table[col])]
                elif current_agg == "COUNT":
                    pro_data_table[col_name] = [len(data_table[col])]
                elif current_agg == "AVG":
                    pro_data_table[col_name] = [sum(data_table[col])/len(data_table[col])]
        
        return pro_data_table
            
    def group_by(self,data_table:dict,group_columns:list,having_condition:list,table_cols:list,agg_func:list):
        # 改了十遍！！！！！！！我真的不想再改了！！！！！！
        # 啊啊啊啊啊啊啊啊

        # group by only one item
        # having only one condition
        
        columns_list = list(data_table.keys())
        row_num = len(data_table[columns_list[0]])
        group_column = group_columns[0]
        # having = having_condition[0]
        group_value = set(data_table[group_column])
        group_dict = {} # store all the value and the sub-table
        empty_table = {}
        for column in columns_list:
            empty_table[column] = []
        for value in group_value:
            group_dict[value] = copy.deepcopy(empty_table)
        for r in range(row_num):
            current_value = data_table[group_column][r]
            for column in columns_list:
                group_dict[current_value][column].append(data_table[column][r])


        # having conditions

        if len(having_condition) != 0: # have having then do filter
            # filtered_table = copy.deepcopy(empty_table)
            # having condition can be 
            # 1. about the group by value 
            # OR 
            # 2. about the aggregate column in select clause
            if len(having_condition[0]) == 4:
                aggregation = having_condition[0][0]
                agg_col = having_condition[0][1]
                op = having_condition[0][2]
                # agg_col should be 
                try:
                    agg_val = int(having_condition[0][3])
                except:
                    agg_val = having_condition[0][3]
                
                
                for val in group_value:
                    if aggregation == "SUM":
                        sub_value = sum(group_dict[val][agg_col])
                    elif aggregation == "MAX":
                        sub_value = max(group_dict[val][agg_col])
                    elif aggregation == "MIN":
                        sub_value = min(group_dict[val][agg_col])
                    elif aggregation == "AVG": 
                        # why python dont have average function !!!
                        sub_value = sum(group_dict[val][agg_col])/len(group_dict[val][agg_col])
                    elif aggregation == "COUNT":
                        sub_value = len(group_dict[val][agg_col])
                    if op == ">":
                        if sub_value > agg_val:
                            continue
                        else:
                            del group_dict[val]
                    elif op == ">=":
                        if sub_value >= agg_val:
                            continue
                        else:
                            del group_dict[val]
                    elif op == "=":
                        if sub_value == agg_val:
                            continue
                        else:
                            del group_dict[val]
                    elif op == "<=":
                        if sub_value <= agg_val:
                            continue
                        else:
                            del group_dict[val]
                    elif op == "<":
                        if sub_value < agg_val:
                            continue
                        else:
                            del group_dict[val]
            else:
                # about the group by value
                group_by_col = having_condition[0][0]
                op = having_condition[0][1]
                try:
                    group_by_val = int(having_condition[0][2])
                except:
                    group_by_val = having_condition[0][2]
                if group_by_col != group_column:
                    # TODO: raise error
                    print(group_by_col,group_column)
                    print("GROUP BY ERROR: Unknown column in having clause. ")
                    return
                for val in group_value:
                    if op == ">":
                        if val > group_by_val:
                            continue
                        else:
                            del group_dict[val]
                    elif op == ">=":
                        if val >= group_by_val:
                            continue
                        else:
                            del group_dict[val]
                    elif op == "=":
                        if val == group_by_val:
                            continue
                        else:
                            del group_dict[val]
                    elif op == "<=":
                        if val <= group_by_val:
                            continue
                        else:
                            del group_dict[val]
                    elif op == "<":
                        if val < group_by_val:
                            continue
                        else:
                            del group_dict[val]

                # meet the requirement
                
        # projections
        # select clause(table_col here) should 
        # 1. have aggregation
        # OR 
        # 2. is in group by list 
        # SELECT list is not in GROUP BY clause and contains nonaggregated column 'disease_data.case_death.total_case' which is not functionally dependent on columns in GROUP BY clause; this is incompatible with sql_mode=only_full_group_by
        proj_table = {}
        for col_idx,proj_col in enumerate(table_cols):
            if (proj_col not in group_columns) and (agg_func[col_idx] == None):
                # TODO: raise error
                print("GROUP BY ERROR: SELECT list is not in GROUP BY clause and contains nonaggregated column.")
                return
            else:
                proj_table[proj_col] = []
        for val in group_dict.keys():

            for col_idx,proj_col in enumerate(table_cols):
                current_agg = agg_func[col_idx]
                if current_agg == None:
                    sub_value = val
                    
                else:
                    if current_agg == "SUM":
                        sub_value = sum(group_dict[val][proj_col])
                    elif current_agg == "MAX":
                        sub_value = max(group_dict[val][proj_col])
                    elif current_agg == "MIN":
                        sub_value = min(group_dict[val][proj_col])
                    elif current_agg == "AVG": 
                        # why python dont have average function !!!
                        sub_value = sum(group_dict[val][proj_col])/len(group_dict[val][proj_col])
                    elif current_agg == "COUNT":
                        sub_value = len(group_dict[val][proj_col])
                proj_table[proj_col].append(sub_value)
        # change column name
        for col_idx,proj_col in enumerate(table_cols):
            current_agg = agg_func[col_idx]
            if current_agg != None:
                col_name = "{}({})".format(current_agg,proj_col)
                proj_table[col_name] = proj_table.pop(proj_col)
            
                
        
        



        return proj_table
    
    




























    #####################################################
    #################    select 结束    ##################
    #####################################################

if __name__=='__main__':
    mySystem=System()
    # mySystem.Create_Database('CLASS')
    # # open database
    # # ATTRIBUTE=namedtuple('attribute',['name','data_type','offset','p_key'])
    # # A1=ATTRIBUTE(*['animal_name','STR',0,True])
    # # A2=ATTRIBUTE(*['animal_age','INT',1,False])
    
    mySystem.open_database('CLASS')
    # print(mySystem.find_primary_key('name_height'))
    # mySystem.delete_data('name_age',0)
    # mySystem.Create_Table('name_age',[['name','String',True],['age','INT',False]])
    # mySystem.Create_Table('name_height',[['name','String',True],['height','INT',False]])
    # data = {"name": "suzy","age":13}

    # mySystem.insert_data('name_age',data)
    # data['age'] = 14
    # mySystem.update_data('name_age',1,data)
    # data = {"name": "suzy","height":170}
    # mySystem.insert_data('name_height',data)
    # table_name_age = mySystem.get_data('name_age')
    # print(table_name_age)
    # table_name_age['name'].append('Lesley')
    # table_name_age['age'].append(10)
    # print(table_name_age)
    # mySystem.overwrite_data("name_age",table_name_age)

    # print(mySystem.get_data('name_height'))
    # mySystem.Drop_Table('name_age') 
    # mySystem.Drop_Database()
    # mySystem.open_databa
    # print(mySystem.check_duplicates([[1,2,1,4],[2,3,2,5]]))
    mySystem.create_index('name_age')
    for key, value in mySystem.table_index['name_age'].items():
        print(key,value)
    








