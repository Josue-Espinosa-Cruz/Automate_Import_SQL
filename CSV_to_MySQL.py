# -*- coding: utf-8 -*-
"""
Created on Tue Nov 16 12:47:11 2021

@author: josue
"""
# %%
import os
import pandas as pd
import numpy as np
import pymysql

# %% 
try:
    connection = pymysql.connect(host="127.0.0.1",
                                 user="root",
                                 password="Argonautas@06"
                                 )
    print("Successfully connection to MySQL")
    pConnect = True
    
except  pymysql.err.MySQLError as e:
    code, message = e.args
    print("Error while connecting to MySQL:",message)

if (pConnect==True):
    try:
        cursor = connection.cursor()
        cursor.execute("CREATE DATABASE ENSANUT_2018")
        print("Successfully creation of the database")
        pDataBase = True

    except  pymysql.err.MySQLError as e:
        code, message = e.args
        if (code == 1007):
            print("Error while creating database:", message)
            pDataBase = True

dbDirectory = "Data/CSV/1_Salud/"
carpetaDB = os.listdir(dbDirectory)

if pDataBase == True:
    try:
        connection = pymysql.connect(host="127.0.0.1",
                                     user="root",
                                     password="Argonautas@06",
                                     database="ENSANUT_2018"
                                     )
        print("Successfully connection to database ENSANUT_2018")
        pConnect1 = True
    except  pymysql.err.MySQLError as e:
        code, message = e.args
        print("Error while connecting to MySQL:", message)

if pConnect1 == True:
    for table in carpetaDB:
        print(table)
        csv = pd.read_csv(dbDirectory + table)
        csv.replace(to_replace=np.nan,value=999, inplace=True)
        
        for i in range(csv.dtypes.count()):
            DTcolumn = csv.iloc[:,i]
            if(DTcolumn.dtype == object):
                DTcolumn = DTcolumn.astype("str")
            else:
                DTcolumn = DTcolumn.astype("int64")
        
        with connection.cursor() as cursor:
            try:
                create2 = "CREATE TABLE "
        
                create2 += (table[0:-4] + " (")
        
                for name in csv.columns:
                    columna = csv.loc[:,name]
                    if(columna.dtype == object):
                        create2 += "{} VARCHAR({}), ".format(name,columna.str.len().max())
                    else:
                        Cmax = columna.max()
                        Cmin = columna.min()
                        if(Cmin<0):
                            if(Cmax<=127 and Cmin>=-128):
                                create2 += "{} TINYINT, ".format(name)
                            elif(Cmax<=32767 and Cmin>=-32768):
                                create2 += "{} SMALLINT, ".format(name)
                            elif(Cmax<=8388607 and Cmin>=-8388608):
                                create2 += "{} MEDIUMINT, ".format(name)
                        else:
                            if(Cmax<=255):
                                create2 += "{} TINYINT UNSIGNED, ".format(name)
                            elif(Cmax<=65535):
                                create2 += "{} SMALLINT UNSIGNED, ".format(name)
                            elif(Cmax<=16777215):
                                create2 += "{} MEDIUMINT UNSIGNED, ".format(name)
                            
                            
                create2 = create2[0:-2] + ");"
                
                cursor.execute(create2)
                connection.commit()
                pInsert = True
            except pymysql.err.MySQLError as e:
                code, message = e.args
                print("Error while connecting to MySQL:", message)
                
                if(code == 1050):
                    pInsert = True
            
            if(pInsert==True):
                for row in range(csv.shape[0]):
                    insert = "INSERT INTO {} VALUES (".format(table[0:-4])
                    for column in range(csv.shape[1]):
                        if(type(csv.iloc[row,column])==str):
                            insert += "'{}', ".format(str(csv.iloc[row,column]))
                        else:
                            insert += "{}, ".format(str(csv.iloc[row,column]))
                    insert = "{});".format(insert[0:-2])
                    cursor.execute(insert)
                    connection.commit()
                    
# CLOSE CONNECTION WITH MYSQL
try:
    connection.close()
    print("Successfully disconnection to MySQL")
except  pymysql.err.MySQLError as e:
    print("Error while connecting to MySQL:",e)