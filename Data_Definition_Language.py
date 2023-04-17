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
        print("###",delete_row_list)
        
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
    

    def create_index(self,relation_name):
        # only one primary key
        primary_key = self.find_primary_key(relation_name)[0]
        index_tree = OOBTree()
        for col_idx,primary_key_num in enumerate(self.database_tables[relation_name][primary_key]):
            index_tree.setdefault(primary_key_num,col_idx)
        self.table_index[relation_name] = index_tree
        return 
    
    def drop_index(self,relation_name):

        del self.table_index[relation_name]
        return
    
    
    # def Drop_Index(self):
    #     # TODO
    #     return

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
    






