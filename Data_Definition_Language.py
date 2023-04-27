from collections import namedtuple
import os
import shutil
import copy
from BTrees.OOBTree import OOBTree
import pickle
from tqdm import tqdm
from AVLTree import avlTree
import math


# YUNI: 一边骂别人不写注释一边自己不写注释的我本人


class SystemError(Exception):
    pass

class System:
    # Maintain two “tables”: TABLES & COLUMNS
    # the system can only contain one database
    def __init__(self) -> None:
        self.database_name = None
        self.tables_filepath = None
        self.columns_filepath = None
        self.constraint_filepath = None
        self.table_path = {}
        self.table_attributes = {} # dict: "table_1" = {'column_1':['INT','True'],'column_2':['STRING','False']}
        self.database_tables = {}
        self.table_index = {}
        self.index_table_name = {} #useless right now
        # 0 --> 1
        self.foreign_key = {'foreign_key_0':{},
                            'foreign_key_1':{}
                            }
        # table_0:{"table_1":[table_1_name],"col_0":[col_name_0],"col_1":[col_name_1]}
        # table_1:{"table_0":[table_0_name],"col_0":[col_name_0],"col_1":[col_name_1]}
        self.avlTree_dict = {}


        self.TREE_OPTIMIZER = False
        self.JOIN_OPTIMIZER = True
        self.INDEX = True

    def save_database(self):
        database_file_path = os.path.join("data",self.database_name)
        with open(database_file_path, 'wb') as f:
            pickle.dump(self, f)
    
    def init_database(self,database_name):
        database_file_path = os.path.join("data",database_name)
        if not os.path.exists(database_file_path):
            self.database_name = database_name
        else:
            print(database_name,"already exists.")
        return


    

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
                f.write('table_name, attribute_name, data_type, primary_key\n')
            
            print(self.database_name, "CREATED SUCCESSFULLY. ")
    def save_constraint(self):
        # self.constraint_filepath = os.path.join(self.database_name,'CONSTRAINT.const')
        with open(self.constraint_filepath,'wb') as f:
            pickle.dump(self.foreign_key,f)

    def load_constraint(self):
        with open(self.constraint_filepath,'rb') as f:
            self.foreign_key = pickle.load(f)

    def create_fake_constraint(self):

        self.foreign_key = {'foreign_key_0':{'orders':{"table_1":['customer_name'],
                                    "col_0":["customer_id"],
                                    "col_1":['id']
                                    }},
                    'foreign_key_1':{'customer_name':{"table_0":["orders"],
                                    "col_0":["customer_id"],
                                    "col_1":["id"]
                                    }}
                    }
    def open_database(self,database_name):
        # YUNI: 0408 Tested
        self.database_name = database_name
        self.tables_filepath = os.path.join(self.database_name,'TABLES.csv')
        self.columns_filepath = os.path.join(self.database_name,'COLUMNS.csv')
        self.constraint_filepath = os.path.join(self.database_name,'CONSTRAINT.const')

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

        # print(self.table_path)
        for table in self.table_path.keys():
            self.database_tables[table] = self.get_data(table)
        # read index
        self.read_index()
        self.load_constraint()
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
            raise SystemError("CREATE ERROR: Table already exists.")
            
        current_table_path = os.path.join(self.database_name,relation_name+'.csv')
        col_names = attribute['names']
        col_data_type = attribute['data_type']
        col_constraints = attribute['constraints']
        foreign_key_col = attribute['foreign_keys_for_table']
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
        self.create_index(relation_name,"default_name")
        self.create_avlTree(relation_name)
        if len(foreign_key_col) != 0:
            # TODO: check references
            # 0 --> 1
            # relation_name -> references
            # add to relation_0
            for i in range(len(foreign_key_col)):
                references = foreign_key_col[i][1]
                col_0_name = foreign_key_col[i][0]
                reference_col = foreign_key_col[i][2]
                col_1_name = self.find_primary_key(references)[0]
                if col_1_name != reference_col:
                    # TODO: raise error
                    raise SystemError("FOREIGN KEY ERROR: Reference column is not a primary key. ")
                if relation_name not in self.foreign_key['foreign_key_0']:
                    self.foreign_key['foreign_key_0'][relation_name] = {"table_1":[references],
                                                        "col_0":[col_0_name],
                                                        "col_1":[col_1_name]
                                                        }
                

                    # add to relation_1
                    self.foreign_key['foreign_key_1'][references] = {"table_0":[relation_name],
                                                        "col_0":[col_0_name],
                                                        "col_1":[col_1_name]
                                                        }
                else:
                    self.foreign_key['foreign_key_0'][relation_name]['table_1'].append(references)
                    self.foreign_key['foreign_key_0'][relation_name]['col_0'].append(col_0_name)
                    self.foreign_key['foreign_key_0'][relation_name]['col_1'].append(col_1_name)

                    self.foreign_key['foreign_key_1'][references]["table_0"].append(relation_name)
                    self.foreign_key['foreign_key_1'][references]["col_0"].append(col_0_name)
                    self.foreign_key['foreign_key_1'][references]["col_1"].append(col_1_name)




        # add to database

        

        return
    
    def create_fake_index(self):
        for data_table in self.database_tables.keys():
            self.create_index(data_table,"simple_name")



    def store_index(self):
        for data_table in self.table_index.keys():
            btree_file_name = os.path.join(self.database_name,"{}.tree".format(data_table))
            with open(btree_file_name,'wb') as f:
                pickle.dump(self.table_index[data_table],f)

    def read_index(self):
        for data_table in self.database_tables.keys():
            btree_file_name = os.path.join(self.database_name,"{}.tree".format(data_table))
            with open(btree_file_name,'rb') as f:
                read_tree = pickle.load(f)
                self.table_index[data_table] = read_tree

    def drop_table_dict(self,relation_name:str):

        # YUNI: 0415 TESTED
        # delete path
        # delete attributes
        # delete database
        # YUNI: 0420 check foreign key
        # YUNI: 0420 single foreign key TESTED
        if relation_name in self.foreign_key['foreign_key_1']:
            raise SystemError("DROP ERROR: Violate the foreign key constraint. ")
        
        if relation_name in self.foreign_key['foreign_key_0']:
            drop_constraint = self.foreign_key['foreign_key_0'].pop(relation_name)
            # delete from foreign_key_1
            # 不理解为什么当时我不把他们放一起我是不是有病

            table_1_list = drop_constraint['table_1']
            column_0_list = drop_constraint['col_0']
            column_1_list = drop_constraint['col_1']
            for idx,table_1_name in enumerate(table_1_list):
                col_0_name = column_0_list[idx]
                col_1_name = column_1_list[idx]
                self.foreign_key['foreign_key_1'][table_1_name]['table_0'].remove(relation_name)
                self.foreign_key['foreign_key_1'][table_1_name]['col_0'].remove(col_0_name)
                self.foreign_key['foreign_key_1'][table_1_name]['col_1'].remove(col_1_name)

            if len(self.foreign_key['foreign_key_1'][table_1_name]['table_0']) == 0:
                del self.foreign_key['foreign_key_1'][table_1_name]
            

        if relation_name not in self.table_path:
            raise SystemError("DROP ERROR: Table does not exist.")
        del self.table_path[relation_name]
        del self.table_attributes[relation_name]
        del self.database_tables[relation_name]
        del self.table_index[relation_name]
        del self.avlTree_dict[relation_name]
        
        

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
    def insert_data(self,relation_name:str,insert_cols:list,insert_vals:list):


        # check duplicates
        # TODO: Check len(insert_col) 
        primary_key_list = self.find_primary_key(relation_name)
        inserted_primary_key = []
        for i,column in enumerate(insert_cols):
            if column in primary_key_list:
                if self.table_attributes[relation_name][column][0] == 'INT':
                    insert_vals[i] = int(insert_vals[i])
                inserted_primary_key.append(insert_vals[i])
        if self.check_duplicates(relation_name,inserted_primary_key) == True:
            raise SystemError("Insertion ERROR: There exists DUPLICATES. ")
        # TODO: insert_col not null check?
        
        if relation_name in self.foreign_key['foreign_key_0']:
            table_1_list = self.foreign_key['foreign_key_0'][relation_name]['table_1']
            col_0_list = self.foreign_key['foreign_key_0'][relation_name]['col_0']
            col_1_list = self.foreign_key['foreign_key_0'][relation_name]['col_1'] # primary key TODO: index
            for idx,table_1 in enumerate(table_1_list):
                col_0 = col_0_list[idx]
                col_1 = col_1_list[idx]
                position = insert_cols.index(col_0)
                col_0_val = insert_vals[position]
                try:
                    col_0_val = int(col_0_val)
                except:
                    col_0_val = insert_vals[position]
                if col_0_val not in self.database_tables[table_1][col_1]:
                    raise SystemError("INSERT ERROR: Violate the foreign key constraints. ")
                

                
                

        for i,column in enumerate(insert_cols):
            if self.table_attributes[relation_name][column][0] == 'INT':

                self.database_tables[relation_name][column].append(int(insert_vals[i]))
            else:
                self.database_tables[relation_name][column].append(insert_vals[i])


        
        # TODO: NEED TO BE TESTED 0417
        if relation_name in self.table_index:
            # add index for the inserted data
            inserted_index_value = len(self.database_tables[relation_name][column])-1
            primary_key = primary_key_list[0]
            # print("*******",inserted_index_value)
            inseted_index_key = self.database_tables[relation_name][primary_key][inserted_index_value]
            self.table_index[relation_name].setdefault(inseted_index_key,inserted_index_value)
            self.avlTree_dict[relation_name].tree_insert(inseted_index_key)

            

        
        return

    def insert_data_no_index(self,relation_name:str,insert_cols:list,insert_vals:list):

        # check duplicates
        # TODO: Check len(insert_col) 
        primary_key_list = self.find_primary_key(relation_name)
        inserted_primary_key = []
        
        for i,column in enumerate(insert_cols):
            if column in primary_key_list:
                inserted_primary_key = copy.deepcopy(self.database_tables[relation_name][column])
                if self.table_attributes[relation_name][column][0] == 'INT':
                    # try_insert_list.append(int(insert_vals[i]))
                    insert_num = int(insert_vals[i])
                else:
                    insert_num = insert_vals[i]

                    # try_insert_list.append(insert_vals[i])
                # inserted_primary_key.append(try_insert_list,try_insert_list)
        if self.check_duplicates_no_index(inserted_primary_key,insert_num) == True:
            raise SystemError("Insertion ERROR: There exists DUPLICATES. ")
        # TODO: insert_col not null check?
        
        if relation_name in self.foreign_key['foreign_key_0']:
            table_1_list = self.foreign_key['foreign_key_0'][relation_name]['table_1']
            col_0_list = self.foreign_key['foreign_key_0'][relation_name]['col_0']
            col_1_list = self.foreign_key['foreign_key_0'][relation_name]['col_1'] # primary key TODO: index
            for idx,table_1 in enumerate(table_1_list):
                col_0 = col_0_list[idx]
                col_1 = col_1_list[idx]
                position = insert_cols.index(col_0)
                col_0_val = insert_vals[position]
                try:
                    col_0_val = int(col_0_val)
                except:
                    col_0_val = insert_vals[position]
                # print("****",type(col_0_val))
                # print(self.database_tables[table_1][col_1])
                # print(col_0_val not in self.database_tables[table_1][col_1])
                if col_0_val not in self.database_tables[table_1][col_1]:
                    raise SystemError("INSERT ERROR: Violate the foreign key constraints. ")
                

                
                

        for i,column in enumerate(insert_cols):
            if self.table_attributes[relation_name][column][0] == 'INT':

                self.database_tables[relation_name][column].append(int(insert_vals[i]))
            else:
                self.database_tables[relation_name][column].append(insert_vals[i])


        
        # TODO: NEED TO BE TESTED 0417
        if relation_name in self.table_index:
            # add index for the inserted data
            inserted_index_value = len(self.database_tables[relation_name][column])-1
            primary_key = primary_key_list[0]
            # print("*******",inserted_index_value)
            inseted_index_key = self.database_tables[relation_name][primary_key][inserted_index_value]
            self.table_index[relation_name].setdefault(inseted_index_key,inserted_index_value)
            self.avlTree_dict[relation_name].tree_insert(inseted_index_key)
            

        
        return



    def find_primary_key(self,relation_name):
        primary_key = []

        for attr in self.table_attributes[relation_name].keys():#name,type,primary key
            if self.table_attributes[relation_name][attr][1] == 'True':
                primary_key.append(attr)
        return primary_key
    
    def check_duplicates(self,relation_name:str,new_primary_num:list):
        # return False -> no duplicates
        # return True -> has duplicates
        # if len(primary_column_list) == 1:
        #     if len(primary_column_list[0]) == len(set(primary_column_list[0])):
        #         return False
        # else:
        #     primary_key_pairs = list(zip(*primary_column_list))
        #     if len(primary_key_pairs) == len(set(primary_key_pairs)):
        #         return False
        # print(self.table_index)
        for num in new_primary_num:
            if num in self.table_index[relation_name]:
                return True
        return False
        




 
    def check_duplicates_no_index(self,primary_column_list,new_num):
        # return False -> no duplicates
        # return True -> has duplicates
        # if len(primary_column_list) == 1:
        for p in primary_column_list:
            for q in primary_column_list:
                for r in range(10):
                    if p == new_num:
                        return True

        # else:
        #     primary_key_pairs = list(zip(*primary_column_list))
        #     if len(primary_key_pairs) == len(set(primary_key_pairs)):
        #         return False
        return False
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

        # check constraint!
        if relation_name in self.foreign_key['foreign_key_1']:
            table_0_list = self.foreign_key['foreign_key_1'][relation_name]['table_0']
            col_0_list = self.foreign_key['foreign_key_1'][relation_name]['col_0']
            col_1_list = self.foreign_key['foreign_key_1'][relation_name]['col_1']
            for idx,table_0 in enumerate(table_0_list):
                col_0 = col_0_list[idx]
                col_1 = col_1_list[idx]
                for delete_i in delete_row_list:
                    col_1_val = self.database_tables[relation_name][col_1][delete_i]
                    if col_1_val in self.database_tables[table_0][col_0]:
                        raise SystemError("DELETE ERROR: Violate foreign key constraints.")
        


        delete_row_list_reverse = delete_row_list[::-1]
        primary_key_col = self.find_primary_key(relation_name)[0]
        for delete_idx in delete_row_list_reverse:
            for column in self.table_attributes[relation_name].keys():

                deleted_val = self.database_tables[relation_name][column].pop(delete_idx)

                if primary_key_col == column:
                    self.avlTree_dict[relation_name].tree_delete(deleted_val)
                    del self.table_index[relation_name][deleted_val]
    


        


        # update index key after the first delete row!!
        # TODO: NEED TO BE TESTED 0417
        # print(delete_row_list)
        if len(delete_row_list) == 0:
            return
        if relation_name in self.table_index:
            delete_row_list_first = delete_row_list[0]
            total_number_row -= len(delete_row_list)
            primary_key_col = self.find_primary_key(relation_name)[0]
            for update_idx_value in range(delete_row_list_first,total_number_row):
                try:

                    update_idx_key = int(self.database_tables[relation_name][primary_key_col][update_idx_value])
                except:
                    update_idx_key = self.database_tables[relation_name][primary_key_col][update_idx_value]
                # print("$$$$$$$$$$$",update_idx_key)


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
        
        primary_key_list = self.find_primary_key(relation_name)
        primary_update_list = []
        for check_i,column in enumerate(update_dict['cols']):
            if column in primary_key_list:
                primary_update_list.append(update_dict['vals'][check_i])
        if self.check_duplicates(relation_name,primary_update_list) == True:
            raise SystemError("Insertion ERROR: There exists DUPLICATES. ")
            
        # check constraints! 
        if relation_name in self.foreign_key['foreign_key_0']:
            # print("&&&&","HERE")
            table_1_list = self.foreign_key['foreign_key_0'][relation_name]['table_1']
            col_0_list = self.foreign_key['foreign_key_0'][relation_name]['col_0']
            col_1_list = self.foreign_key['foreign_key_0'][relation_name]['col_1'] # primary key TODO: index?
            for u_idx,updating_col in enumerate(update_dict['cols']):
                for col_0_idx,col_0 in enumerate(col_0_list):
                    if updating_col == col_0:
                        # print("&&&&","HERE")
                        

                        try:
                            col_0_val = int(update_dict['vals'][u_idx])

                        except:
                            col_0_val = update_dict['vals'][u_idx]

                        col_1 = col_1_list[col_0_idx]
                        table_1 = table_1_list[col_0_idx]
                        # print("&&&&&&&",col_0_val)
                        if col_0_val not in self.database_tables[table_1][col_1]:
                            raise SystemError("INSERT ERROR: Violate the foreign key constraints. ")
        if relation_name in self.foreign_key['foreign_key_1']:

            table_0_list = self.foreign_key['foreign_key_1'][relation_name]['table_0']
            col_0_list = self.foreign_key['foreign_key_1'][relation_name]['col_0']
            col_1_list = self.foreign_key['foreign_key_1'][relation_name]['col_1'] # primary key TODO: index?
            for u_idx,updating_col in enumerate(update_dict['cols']):

                for col_1_idx,col_1 in enumerate(col_1_list):
                    if updating_col == col_1:
                        for update_idx in update_row_list:
                            col_1_val = self.database_tables[relation_name][updating_col][update_idx]
                            col_0 = col_0_list[col_1_idx]
                            table_0 = table_0_list[col_1_idx]
                            if col_1_val in self.database_tables[table_0][col_0]:
                                raise SystemError("DELETE ERROR: Violate the foreign key constraints. ")





        for update_idx in update_row_list:
            for j,column in enumerate(update_dict['cols']):
                origin_val = self.database_tables[relation_name][column][update_idx]
                self.database_tables[relation_name][column][update_idx] = update_dict['vals'][j]


                # TODO: NEED TO BE TESTED 0417
                if (relation_name in self.table_index) and (column in primary_key_list):
                    updated_row_num = self.table_index[relation_name].pop(origin_val) 
                    self.table_index[relation_name].setdefault(update_dict['vals'][j],updated_row_num)
                    self.avlTree_dict[relation_name].tree_delete(origin_val)
                    self.avlTree_dict[relation_name].tree_insert(update_dict['vals'][j])


                
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





    def update_data_no_index(self,relation_name,update_dict:dict,where_dict:dict):

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
        # print("###",update_row_list)
        
        primary_key_list = self.find_primary_key(relation_name)
        primary_update_list = []
        for check_i,column in enumerate(update_dict['cols']):
            if column in primary_key_list:
                try_update_list = copy.deepcopy(self.database_tables[relation_name][column])
                for row_idx in update_row_list:
                    try_update_list[row_idx] = update_dict['vals'][check_i]
                primary_update_list.append(try_update_list)
        if self.check_duplicates(primary_update_list) == True:
            raise SystemError("Insertion ERROR: There exists DUPLICATES. ")
            
        # check constraints! 
        if relation_name in self.foreign_key['foreign_key_0']:
            # print("&&&&","HERE")
            table_1_list = self.foreign_key['foreign_key_0'][relation_name]['table_1']
            col_0_list = self.foreign_key['foreign_key_0'][relation_name]['col_0']
            col_1_list = self.foreign_key['foreign_key_0'][relation_name]['col_1'] # primary key TODO: index?
            for u_idx,updating_col in enumerate(update_dict['cols']):
                for col_0_idx,col_0 in enumerate(col_0_list):
                    if updating_col == col_0:
                        # print("&&&&","HERE")
                        

                        try:
                            col_0_val = int(update_dict['vals'][u_idx])

                        except:
                            col_0_val = update_dict['vals'][u_idx]

                        col_1 = col_1_list[col_0_idx]
                        table_1 = table_1_list[col_0_idx]
                        # print("&&&&&&&",col_0_val)
                        if col_0_val not in self.database_tables[table_1][col_1]:
                            raise SystemError("INSERT ERROR: Violate the foreign key constraints. ")
        if relation_name in self.foreign_key['foreign_key_1']:

            table_0_list = self.foreign_key['foreign_key_1'][relation_name]['table_0']
            col_0_list = self.foreign_key['foreign_key_1'][relation_name]['col_0']
            col_1_list = self.foreign_key['foreign_key_1'][relation_name]['col_1'] # primary key TODO: index?
            for u_idx,updating_col in enumerate(update_dict['cols']):

                for col_1_idx,col_1 in enumerate(col_1_list):
                    if updating_col == col_1:
                        for update_idx in update_row_list:
                            col_1_val = self.database_tables[relation_name][updating_col][update_idx]
                            col_0 = col_0_list[col_1_idx]
                            table_0 = table_0_list[col_1_idx]
                            if col_1_val in self.database_tables[table_0][col_0]:
                                raise SystemError("DELETE ERROR: Violate the foreign key constraints. ")





        for update_idx in update_row_list:
            for j,column in enumerate(update_dict['cols']):
                # print(self.database_tables[relation_name][column])
                # print(update_dict['vals'])
                origin_val = self.database_tables[relation_name][column][update_idx]
                self.database_tables[relation_name][column][update_idx] = update_dict['vals'][j]


                # TODO: NEED TO BE TESTED 0417
                if (relation_name in self.table_index) and (column in primary_key_list):
                    updated_row_num = self.table_index[relation_name].pop(origin_val) # should equal to j
                    self.table_index[relation_name].setdefault(update_dict['vals'][j],updated_row_num)
                    self.avlTree_dict[relation_name].tree_delete(origin_val)
                    self.avlTree_dict[relation_name].tree_insert(update_dict['vals'][j])
                
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
    
    def create_avlTree(self,relation_name):
        new_tree = avlTree()
        self.avlTree_dict[relation_name] = new_tree
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

    def check_sort(self,n:int,m:int):
        base = 2
        return n * math.log(n, base) + m * math.log(m, base) + n + m - 1 < n * m

    def join_sort(self, pk, table, col):
        # lst_num = self.table_index[table][pk]
        return self.database_tables[table][col][pk]

    def sorted_merge_join(self,table_1:str, table_1_col:str, 
                         table_2:str, table_2_col:str,
                         projection_cols_1:list,projection_cols_2:list):
        # only support join grammar like 
        # SELECT table_1.column, table_2.column 
        # FROM table_1 INNER JOIN table_2 
        # ON table_1.column2 = table_2.column2;


        # YUNI 0419 TESTED!
        # print(table_1,table_1_col,table_2,table_2_col,projection_cols_1,projection_cols_2)
        row_num_1 = self.get_row_num(table_1)
        row_num_2 = self.get_row_num(table_2)
        new_table = {}  # structure similar to self.database_tables[relation_name] {'column_1':[],'column_2':[]}
        for c_1 in projection_cols_1:
            c_1_name = "{}.{}".format(table_1,c_1)

            new_table[c_1_name] = []
        for c_2 in projection_cols_2:
            c_2_name = "{}.{}".format(table_2,c_2)

            new_table[c_2_name] = []
        
        if self.JOIN_OPTIMIZER == True and self.check_sort(row_num_1, row_num_2):

            primary_key_1 = self.find_primary_key(table_1)[0]
            lst1 = self.database_tables[table_1][primary_key_1]
            lst1 = [x for x in range(row_num_1)]
            
            primary_key_2 = self.find_primary_key(table_2)[0]
            lst2 = self.database_tables[table_2][primary_key_2]
            lst2 = [x for x in range(row_num_2)]

            lst1 = sorted(lst1, key=lambda x: self.join_sort(x, table_1, table_1_col))
            lst2 = sorted(lst2, key=lambda x: self.join_sort(x, table_2, table_2_col))
            ptr1 = 0
            len1 = len(lst1)
            ptr2 = 0
            len2 = len(lst2)

            #         lst_num = self.table_index[table][pk]
            #         return self.database_tables[table][col][lst_num]
            join_list_1 = []
            join_list_2 = []

            import time

            st =  time.time()
            while ptr1<len1 and ptr2<len2:
                ans1 = self.database_tables[table_1][table_1_col][lst1[ptr1]]
                ans2 = self.database_tables[table_2][table_2_col][lst2[ptr2]]

                # print(ans1, ans2)
                #ptr1+=1
                #ptr2+=1
                # [1  1  3  3  5 5 5]
                # [0, 1, 3, 4, 5]
                if ans1 < ans2:
                    ptr1+=1
                    continue
                if ans1 > ans2:
                    ptr2+=1
                    continue
                if ans1 == ans2:
                    step1 = 0
                    for tmp in range(0, len1-ptr1):
                        # print("IAM", self.database_tables[table_2][table_2_col][lst2[ptr2+tmp]])
                        if self.database_tables[table_1][table_1_col][lst1[ptr1+tmp]] == ans1:
                            step1 += 1
                        else:
                            break

                    # print(time.time()-st,"eeee")

                    step2 = 0
                    for tmp in range(0, len2-ptr2):

                        if self.database_tables[table_2][table_2_col][lst2[ptr2+tmp]] == ans2:
                            step2 += 1
                        else:
                            break
                    
                    # print(time.time()-st,"qqqqq")

                    for pp1 in range(ptr1, ptr1+step1):
                        for pp2 in range(ptr2, ptr2+step2):
                            # print("same", self.database_tables[table_1][table_1_col][self.table_index[table_1][lst1[pp1]]], 
                            #       self.database_tables[table_2][table_2_col][self.table_index[table_2][lst2[pp2]]])
                            join_list_1.append(lst1[pp1])
                            join_list_2.append(lst2[pp2])

                            #row_cnt += join_judger(rela1, ans1, ans2, logic, mpAttr, sorted_uu_list1[pp1], sorted_uu_list2[pp2])
                    # print(time.time()-st,"wwww")
                    # return


                    ptr1 = ptr1+step1
                    ptr2 = ptr2+step2
            # print("consume", time.time()-st)
            # print(join_list_1)
            # print(join_list_2)

            for row_idx, p1 in enumerate(join_list_1):
                p2 = join_list_2[row_idx]
                for c_1 in projection_cols_1:
                    c_1_name = "{}.{}".format(table_1,c_1)
                    new_table[c_1_name].append(self.database_tables[table_1][c_1][p1])
                for c_2 in projection_cols_2:
                    c_2_name = "{}.{}".format(table_2,c_2)
                    new_table[c_2_name].append(self.database_tables[table_2][c_2][p2])
        # for r_1 in tqdm(range(row_num_1)):
        #     value_1 = self.database_tables[table_1][table_1_col][r_1]
        #     for r_2 in range(row_num_2):
        #         value_2 = self.database_tables[table_2][table_2_col][r_2]
        #         if value_1 == value_2:
        #             pass
        #         else:
        #             continue
        #          # when come to this line, join two tables
        #         for c_1 in projection_cols_1:
        #             c_1_name = "{}.{}".format(table_1,c_1)
        #             new_table[c_1_name].append(self.database_tables[table_1][c_1][r_1])
        #         for c_2 in projection_cols_2:
        #             c_2_name = "{}.{}".format(table_2,c_2)
        #             new_table[c_2_name].append(self.database_tables[table_2][c_2][r_2])
        # # print(new_table)  
        else:
            new_table = self.nested_loop_join(table_1,table_1_col,table_2,table_2_col,projection_cols_1,projection_cols_2)
        return new_table
            




    def nested_loop_join(self,table_1:str, table_1_col:str, 
                         table_2:str, table_2_col:str,
                         projection_cols_1:list,projection_cols_2:list):
        # only support join grammar like 
        # SELECT table_1.column, table_2.column 
        # FROM table_1 INNER JOIN table_2 
        # ON table_1.column2 = table_2.column2;


        # YUNI 0419 TESTED!
        # print(table_1,table_1_col,table_2,table_2_col,projection_cols_1,projection_cols_2)
        row_num_1 = self.get_row_num(table_1)
        row_num_2 = self.get_row_num(table_2)
        new_table = {}  # structure similar to self.database_tables[relation_name] {'column_1':[],'column_2':[]}
        for c_1 in projection_cols_1:
            c_1_name = "{}.{}".format(table_1,c_1)

            new_table[c_1_name] = []
        for c_2 in projection_cols_2:
            c_2_name = "{}.{}".format(table_2,c_2)

            new_table[c_2_name] = []
        
        for r_1 in tqdm(range(row_num_1)):
            value_1 = self.database_tables[table_1][table_1_col][r_1]
            for r_2 in range(row_num_2):
                value_2 = self.database_tables[table_2][table_2_col][r_2]
                if value_1 == value_2:
                    pass
                else:
                    continue
                 # when come to this line, join two tables
                for c_1 in projection_cols_1:
                    c_1_name = "{}.{}".format(table_1,c_1)
                    new_table[c_1_name].append(self.database_tables[table_1][c_1][r_1])
                for c_2 in projection_cols_2:
                    c_2_name = "{}.{}".format(table_2,c_2)
                    new_table[c_2_name].append(self.database_tables[table_2][c_2][r_2])
        # print(new_table)  
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
    def identify_or(self,op,val,value_in_row):
        if op == ">":
            if value_in_row > val:
                return True
            else:
                return False
        elif op == ">=":
            if value_in_row >= val:
                return True
            else:
                return False
        elif op == "=":
            if value_in_row == val:
                return True
            else:
                return False
        elif op == "<=":
            if value_in_row <= val:
                return True
            else:
                return False
        elif op == "<":
            if value_in_row < val:
                return True
            else:
                return False
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
                col_1 = condition_1[0]
                op_1 = condition_1[1]

                try:
                    val_1 = int(condition_1[2])
                except:
                    val_1 = condition_1[2]
                
                
                for r in range(row_num):
                    value_in_row_1 = data_table[col_1][r]
                    if self.identify_or(op_1,val_1,value_in_row_1):

                    # when come to this line: meet the requirement and select
                        selected_row_num.append(r)
                        continue
                    else:
                        col_2 = condition_2[0]
                        op_2 = condition_2[1]

                        try:
                            val_2 = int(condition_2[2])
                        except:
                            val_2 = condition_2[2]
                        value_in_row_2 = data_table[col_2][r]
                        
                        if self.identify_or(op_2,val_2,value_in_row_2):
                            selected_row_num.append(r)



                
                # _,selected_row_list = self.select_where_from_output(data_table,[condition_1])
                # columns_list_ = list(data_table.keys())
                # row_num_ = len(data_table[columns_list_[0]])
                # not_selected_list = [i for i in range(row_num_)] - selected_row_list
                # data_remain = {}
                # for column in columns_list:
                #     data_remain[column] = []
                # for r_not_s in not_selected_list:
                #     for column in columns_list:
                #         data_remain[column].append(data_table[column][r_not_s])
                    
                # _,selected_row_list_2 = self.select_where_from_output(data_remain,[condition_2])
                # selected_row_total = set(selected_row_list + selected_row_list_2)
                for column in columns_list:
                    selected_table[column] = []
                for s_t in selected_row_num:
                    for column in columns_list:
                        selected_table[column].append(data_table[column][s_t])
                return selected_table,[]
    


                    





            


                
    def get_condition_number(self,relation_name:str,condition:list):
        if len(condition) != 3:
            return 10000000
        op = condition[1]
        # print("here",op)
        # TODO: need to think twice
        try:
            val = int(condition[2])
        except:
            val = condition[2]
        if op == "=":
            condition_num = self.avlTree_dict[relation_name].tree_get_position(val,True) - self.avlTree_dict[relation_name].tree_get_position(val,False)
            return condition_num
        if op == ">=":
            condition_num = self.avlTree_dict[relation_name].tree_weight() - self.avlTree_dict[relation_name].tree_get_position(val,False)
            return condition_num
        if op == ">":
            condition_num = self.avlTree_dict[relation_name].tree_weight() - self.avlTree_dict[relation_name].tree_get_position(val,True)
            return condition_num
        if op == "<":
            condition_num = self.avlTree_dict[relation_name].tree_get_position(val,False)
            return condition_num
        if op == "<=":
            condition_num = self.avlTree_dict[relation_name].tree_get_position(val,True)
            return condition_num
        

        
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
            primary_list = self.find_primary_key(relation_name)
            if (self.TREE_OPTIMIZER == True) and (conditions[0][0] in primary_list) and (conditions[2][0] in primary_list):
                 # put query that easy to be False in the front
                    
                condition_1_num = self.get_condition_number(relation_name,conditions[0])
                # print(condition_1_num,conditions[0])

                condition_2_num = self.get_condition_number(relation_name,conditions[2])
                # print(condition_2_num,conditions[2])
                operation = conditions[1]
                if operation == "AND":
                    if condition_1_num <= condition_2_num:
                        condition_1 = conditions[0]
                        
                        condition_2 = conditions[2]
                        
                    else:
                        condition_1 = conditions[2]
                        condition_2 = conditions[0]
                        # print("swap")
                if operation == "OR":
                    if condition_1_num >= condition_2_num:
                        condition_1 = conditions[0]
                        condition_2 = conditions[2]
                    else:
                        condition_1 = conditions[2]
                        condition_2 = conditions[0]
                        # print("swap")

            else:
                condition_1 = conditions[0]
                operation = conditions[1]
                condition_2 = conditions[2]

            if operation == "AND":

                data_table_output,_ = self.select_where(relation_name,[condition_1]) # meet with condition1 and meet with condition2
                data_table_output_2,_ = self.select_where_from_output(data_table_output,[condition_2])
                return data_table_output_2,[]
            else: # operation == "OR": condition_1 = True
                 # IN condition_1 = False find condition_2 = True
                row_num = self.get_row_num(relation_name)
                
                col_1 = condition_1[0]
                op_1 = condition_1[1]

                try:
                    val_1 = int(condition_1[2])
                except:
                    val_1 = condition_1[2]
                
                
                for r in range(row_num):
                    value_in_row_1 = self.database_tables[relation_name][col_1][r]
                    if self.identify_or(op_1,val_1,value_in_row_1):

                    # when come to this line: meet the requirement and select
                        selected_row_num.append(r)
                        continue
                    else:
                        col_2 = condition_2[0]
                        op_2 = condition_2[1]

                        try:
                            val_2 = int(condition_2[2])
                        except:
                            val_2 = condition_2[2]
                        value_in_row_2 = self.database_tables[relation_name][col_2][r]
                        
                        if self.identify_or(op_2,val_2,value_in_row_2):
                            selected_row_num.append(r)



                
                # _,selected_row_list = self.select_where_from_output(data_table,[condition_1])
                columns_list = self.get_column_list(relation_name)
                # row_num_ = len(data_table[columns_list_[0]])
                # not_selected_list = [i for i in range(row_num_)] - selected_row_list
                # data_remain = {}
                # for column in columns_list:
                #     data_remain[column] = []
                # for r_not_s in not_selected_list:
                #     for column in columns_list:
                #         data_remain[column].append(data_table[column][r_not_s])
                    
                # _,selected_row_list_2 = self.select_where_from_output(data_remain,[condition_2])
                # selected_row_total = set(selected_row_list + selected_row_list_2)
                for column in columns_list:
                    selected_table[column] = []
                for s_t in selected_row_num:
                    for column in columns_list:
                        selected_table[column].append(self.database_tables[relation_name][column][s_t])
                return selected_table,[]
            


            
        # TODO: using index
            
    def limit(self,data_table:dict,limit_clause:list):
        if len(limit_clause) == 0:
            return data_table
        limit_num = limit_clause[0]
        limit_table = {}
        for col in data_table.keys():
            limit_table[col] = data_table[col][:limit_num]
        return limit_table
    
        
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
                    raise SystemError("PROJECTION ERROR: In aggregated query without GROUP BY, SELECT list contains nonaggregated column. ")
                agg_flag = True
            else: # dont have agg_fun (if c_agg == None)
                if agg_flag == True:
                    # TODO: raise error
                    raise SystemError("PROJECTION ERROR: In aggregated query without GROUP BY, SELECT list contains nonaggregated column. ")
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
            # 1. about the "group by" value 
            # OR 
            # 2. about the "aggregate" column in select clause
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
                    raise SystemError("GROUP BY ERROR: Unknown column in having clause. ")
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
                raise SystemError("GROUP BY ERROR: SELECT list is not in GROUP BY clause and contains nonaggregated column.")
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
    
    # mySystem.open_database('CLASS')
    print(mySystem.open_database('CUSTOMERS'))

    # mySystem.drop_table_dict('orders')
    # print(mySystem.database_tables)
    # print(mySystem.foreign_key)

    # mySystem.create_fake_constraint()

    # print(mySystem.foreign_key['foreign_key_1'])
    # mySystem.save_constraint()
    # mySystem.load_constraint()
    # print('==================')
    # print(mySystem.foreign_key)

    # mySystem.create_fake_index()
    # mySystem.store_index()
    # mySystem.read_index()
    

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
    # mySystem.create_index('name_age')
    # for key, value in mySystem.table_index['name_age'].items():
    #     print(key,value)
    # for key, value in mySystem.table_index['name_height'].items():
    #     print(key,value)
    








