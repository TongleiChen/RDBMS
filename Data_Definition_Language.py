from collections import namedtuple
import os

class System:
    # Maintain two “tables”: TABLES & COLUMNS
    def __init__(self) -> None:
        self.DB=[]
        
    
    def Create_Database(self,database_name):
        if os.path.isdir('./' + database_name):
            print(database_name," IS ALREADY EXIST.")
        else:
            os.mkdir(database_name)
            tables_filepath = os.path.join(database_name,'TABLES.db')
            with open(tables_filepath,'w') as f:
                f.write('Store all relations in this database here\n')
            columns_filepath = os.path.join(database_name,'COLUMNS.db')
            with open(columns_filepath,'w') as f:
                f.write('Store all columns in this database here\n')
            

    def Drop_Database(self,database_name):
        if os.path.isdir('./' + database_name):
            # remove table
            ...

        else:
            print(database_name," HAS ALREADY BEEN DROPPED.")

    def Create_Table(self,db_name,relation_name:str,attribute_list:list):
        name=[]
        name.append(name)
        # TODO write relation name data to TABLES.db

        attribute_list=attribute_list
        # TODO write attribute data to COLUMNS.db
        
        table_filepath = os.path.join(db_name,relation_name+'.db')
        with open(table_filepath,'w') as f:
                f.write('my table\n')
        # return 
    def Drop_Table(self,table_name):
        # TODO
        return
    
    def Create_Index(self):
        # TODO
        return 
    
    def Drop_Index(self):
        # TODO
        return

if __name__=='__main__':
    mySystem=System()
    mySystem.Create_Database('ZOO')
    # open database
    ATTRIBUTE=namedtuple('attribute',['name','data_type','offset','p_key'])
    A1=ATTRIBUTE(*['animal_name','STR',0,True])
    A2=ATTRIBUTE(*['animal_age','INT',1,False])
    mySystem.Create_Table('Zoo','name_age',[A1,A2])





