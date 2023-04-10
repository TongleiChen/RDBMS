from collections import namedtuple
import os
import shutil


# YUNI: 一边骂别人不写注释一边自己不写注释的我本人

class System:
    # Maintain two “tables”: TABLES & COLUMNS
    # the system can only contain one database
    def __init__(self) -> None:
        self.database_name = None
        self.tables_filepath = None
        self.columns_filepath = None
        self.table_path = {}
        self.table_attributes= {}

    
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

                    self.table_attributes[line_2[0]].append(line_2[1:])
                else:
                    self.table_attributes[line_2[0]] = [line_2[1:]]

        # print(self.table_attributes)
        # print(self.table_path)
        return
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
    


    def Drop_Table(self,relation_name):
        # YUNI: 0408 Tested
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
    


    def insert_data(self,relation_name,data:dict):
        # TODO: 
        # Check duplicates
        current_table_path = self.table_path[relation_name]
        attribute_list = self.table_attributes[relation_name]
        data_list = []
        for attri in attribute_list:
            print(attri)
            data_list.append(data[attri[0]])

        
        with open(current_table_path,'r+') as f_t:
            f_t.seek(0,2)
            f_t.write(",".join(map(str, data_list))+"\n")

        return

    def delete_data(self,relation_name,data_pos):
        # YUNI: 0408 Tested
        # YUNI: 搞不懂我自己为什么不直接把整个table更新完丢进去. 征求lzx建议. update_data 也是
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
    

    def update_data(self,relation_name,data_pos,data:dict):
        # TODO: Check duplicates
        current_table_path = self.table_path[relation_name]
        
        attribute_list = self.table_attributes[relation_name]
        data_list = []
        for attri in attribute_list:
            data_list.append(data[attri[0]])
        updated_data = ",".join(map(str,data_list)) + '\n'
        new_data = []
        with open(current_table_path,"r") as f:
            for pos,data_line in enumerate(f.readlines()):
                # YUNI: 这里data_pos 是不包括标题那一行的第几行
                if pos == data_pos + 1:
                    new_data.append(updated_data)
                    continue
                else:
                    new_data.append(data_line)
        with open(current_table_path,"w") as f:
            f.writelines(new_data)
        return
    
    def get_data(self,relation_name):
        # YUNI: 0408 Tested
        # YUNI: 不知道return 应该是list还是dict还是什么
        current_table_path = self.table_path[relation_name]
        data_in_table = []
        data_attributes = []
        for attr in self.table_attributes[relation_name]:
            data_attributes.append(attr[0])
        with open(current_table_path,"r") as f:
            f.readline()
            for data_line in f.readlines():
                data_line = data_line.strip('\n')
                data_in_table.append(data_line.split(","))

        return data_in_table,data_attributes
    

    # def Create_Index(self):
    #     # TODO
    #     return 
    
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
    # mySystem.delete_data('name_age',0)
    # mySystem.Create_Table('name_age',[['name','String',True],['age','INT',False]])
    # mySystem.Create_Table('name_height',[['name','String',True],['height','INT',False]])
    # data = {"name": "suzy","age":13}

    # mySystem.insert_data('name_age',data)
    # data['age'] = 14
    # mySystem.update_data('name_age',1,data)
    # data = {"name": "suzy","height":170}
    # mySystem.insert_data('name_height',data)
    print(mySystem.get_data('name_age'))
    print(mySystem.get_data('name_height'))
    # mySystem.Drop_Table('name_age') 
    # mySystem.Drop_Database()






