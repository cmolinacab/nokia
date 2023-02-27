import pandas as pd
import time
import os
import sqlite3
import re
import numpy as np


    #plist
def CM_plists(max_date, conn):

    print('Processing CM p_lists\n')

    con = sqlite3.connect(str(max_date['last_date'][0]).replace("-","")+f'_sqlite_FINAL.db')

    start_time = time.time()



    i=0


    cursor = conn.cursor()

    cursor.execute(f"""
        select distinct mo_class
        from hive.network_data.cm a
        
        where  a.raml_date=date '{max_date['last_date'][0]}'  and p_lists  is not null


    """)

    data = cursor.fetchall()
    columns = [c[0] for c in cursor.description]

    tables_CM = pd.DataFrame(data,columns=columns)

    tables_CM.drop_duplicates(inplace=True)






    for table in tables_CM['mo_class']:
        


        cursor = conn.cursor()

        cursor.execute(f"""
            select mo_class, ids,  p_lists, filename
            from hive.network_data.cm a
            where  a.raml_date=date '{max_date['last_date'][0]}' and p_lists  is not null and mo_class ='{table}'


        """)

        data = cursor.fetchall()
        columns = [c[0] for c in cursor.description]

        data_CM= pd.DataFrame(data,columns=columns)



        data_CM['PLMN_orig']=data_CM['filename'].apply(lambda x: re.findall('rc[0-9]+',x)[0].upper())        
        #data_CM=all_CM[all_CM['mo_class']==table]
        

        if data_CM.shape[0]>0:
            
            i=i+1
        
            plists=data_CM['p_lists'].apply(pd.Series)

            for column in plists.columns:

                print(i, table, column)

                plists_sub=plists[column]

                data=pd.concat([data_CM['PLMN_orig'], data_CM['ids'].apply(pd.Series).add_suffix('_id'),
                          plists_sub], axis=1)
                
                data['PLMN_id']=data['PLMN_orig']
                
                data.drop('PLMN_orig', axis=1, inplace=True)

                data.columns = [*data.columns[:-1], 'Value']

                result=data.explode('Value').reset_index().rename(columns={'index' : 'optionId'})
                result['optionId'] = result.groupby('optionId').cumcount()

                temp_cols=result.columns.tolist()
                new_cols=temp_cols[1:-1] + temp_cols[:1]+temp_cols[-1:]
                result=result[new_cols].dropna()

                

                result.replace(np.nan, 0, inplace=True)

                for field in result.columns:
                    try:
                        result[field]=result[field].astype(np.int64)
                    except:
                        pass


                result.to_sql(name=str(table).upper()+'_'+str(column).upper(), con=con, if_exists='replace', index=False)


    print("Total Time:",(time.time()-start_time)/60, "minutes")

    con.close()    


    return 0




    #itemlists


def CM_itemlists(max_date, conn):

    print('Processing CM item_lists\n')

    con = sqlite3.connect(str(max_date['last_date'][0]).replace("-","")+f'_sqlite_FINAL.db')

    start_time = time.time()



    i=0

    cursor = conn.cursor()

    cursor.execute(f"""
        select mo_class, count(mo_class) as count_elem
        from hive.network_data.cm a
        where  a.raml_date= date '{max_date['last_date'][0]}' and item_lists  is not null
        group by mo_class


    """)

    data = cursor.fetchall()
    columns = [c[0] for c in cursor.description]

    tables_CM = pd.DataFrame(data,columns=columns)

    tables_CM.drop_duplicates(inplace=True)   



    i=0


    for table in tables_CM['mo_class']:

        cursor = conn.cursor()

        cursor.execute(f"""
            select ids,  item_lists
            from hive.network_data.cm a
            where  a.raml_date= date '{max_date['last_date'][0]}' and a.item_lists  is not null and  mo_class = '{table}'
            limit 1

        """)

        data = cursor.fetchall()
        columns = [c[0] for c in cursor.description]

        data_CM = pd.DataFrame(data,columns=columns)

        str_table=data_CM.head(1)

        item_lists=str_table['item_lists'].apply(pd.Series)


        for column in item_lists.columns:


            #GET TABLE STRUCTURE


            cursor = conn.cursor()

            cursor.execute(f"""
            SELECT ids,
            element_at(item_lists,'{column}')[1] as {column}
            FROM hive.network_data.cm a
            where  a.raml_date=date '{max_date['last_date'][0]}' and item_lists  is not null and mo_class = '{table}' limit 1

            """)

            data = cursor.fetchall()
            columns = [c[0] for c in cursor.description]

            data_CM = pd.DataFrame(data,columns=columns)

            data_CM_str=data_CM[column].apply(pd.Series).columns

            sql_str='select '

            #Detect distinct ids
            
            cursor = conn.cursor()

            cursor.execute(f"""
                select distinct map_keys(ids) as ids_keys
                from hive.network_data.cm a
                where  a.raml_date= date '{max_date['last_date'][0]}' and a.item_lists  is not null and  mo_class = '{table}'


            """)

            data = cursor.fetchall()
            columns = [c[0] for c in cursor.description]

            ids_keys = pd.DataFrame(data,columns=columns)

            ik=[]

            for iks in ids_keys['ids_keys']:
                ik=ik+iks   
                

      
            
            
            for col in set(ik):
                sql_str=sql_str+f"element_at(ids,'{col}') as {col}_id, "

            for col in data_CM_str:
                sql_str=sql_str+f"element_at({column},'{col}') as {col}, "


            sql_str=sql_str[:-2] + f""", filename from (
            SELECT ids, filename,
            element_at(item_lists,'{column}')[1] as {column}
            FROM hive.network_data.cm a
            where  a.raml_date=date '{max_date['last_date'][0]}' and item_lists  is not null and mo_class = '{table}')

            """

            
            cursor = conn.cursor()

            cursor.execute(sql_str)

            data = cursor.fetchall()
            columns = [c[0] for c in cursor.description]

            data_CM_final = pd.DataFrame(data,columns=columns)
            
            
            #RC
            
            data_CM_final['PLMN_orig']=data_CM_final['filename'].apply(lambda x: re.findall('rc[0-9]+',x)[0].upper())

            data_CM_final['PLMN_id']=data_CM_final['PLMN_orig']

            data_CM_final.drop(['filename','PLMN_orig'], axis=1, inplace=True)
            
            print(i,table,column, data_CM_final.shape)


            data_CM_final.replace(np.nan, 0, inplace=True)

            for field in data_CM_final.columns:
                try:
                    data_CM_final[field]=data_CM_final[field].astype(np.int64)
                except:
                    pass

            
            data_CM_final.to_sql(name=str(table).upper()+'_'+str(column).upper(), con=con, if_exists='replace', index=False)
        i=i+1

    print("Total Time:",(time.time()-start_time)/60, "minutes")

    con.close()
    return 0




def CM_Parameters(tables, max_date, process_num):

    con = sqlite3.connect(str(max_date['last_date'][0]).replace("-","")+f'_sqlite_{process_num}.db')

    start_time = time.time()

    import prestodb

    #Presto Connection

    conn = prestodb.dbapi.connect(
        host=os.getenv('presto_host'),
        port=os.getenv('presto_port'),
        user='admin',
        catalog='hive',
        schema='network_data'
    )


    i=0

    error_count=[]

    for table in tables:

        exec_flag=False
        error_times=0

        while not exec_flag and error_times<100:
        
            try:

                #GET TABLE STRUCTURE

                cursor = conn.cursor()

                sql_str=f"""
                    select ids, mo_version, parameters
                    from hive.network_data.cm a
                    where  a.raml_date= date '{max_date['last_date'][0]}' and a.parameters  is not null and  mo_class = '{table}'
                    limit 1

                """

                #print(sql_str)

                cursor.execute(sql_str)

                data = cursor.fetchall()
                columns = [c[0] for c in cursor.description]

                data_CM = pd.DataFrame(data,columns=columns)




                #GET ARRANGED DATA
                
                #Detect distinct ids
                
                cursor = conn.cursor()

                cursor.execute(f"""
                    select distinct map_keys(ids) as ids_keys
                    from hive.network_data.cm a
                    where  a.raml_date= date '{max_date['last_date'][0]}' and a.parameters  is not null and  mo_class = '{table}'


                """)

                data = cursor.fetchall()
                columns = [c[0] for c in cursor.description]

                ids_keys = pd.DataFrame(data,columns=columns)

                ik=[]

                for iks in ids_keys['ids_keys']:
                    ik=ik+iks  
                    
                #Detect distinct parameters
                
                cursor = conn.cursor()

                cursor.execute(f"""
                    select distinct map_keys(parameters) as p_keys
                    from hive.network_data.cm a
                    where  a.raml_date= date '{max_date['last_date'][0]}' and a.parameters  is not null and  mo_class = '{table}'


                """)

                data = cursor.fetchall()
                columns = [c[0] for c in cursor.description]

                p_keys = pd.DataFrame(data,columns=columns)

                pk=[]

                for pks in p_keys['p_keys']:
                    pk=pk+pks  
                    
                    
                    
                    
                cursor = conn.cursor()

                sql_str='select '
                for col in set(ik):
                    sql_str=sql_str+f"element_at(ids,'{col}') as {col}_id, "

                sql_str=sql_str+"mo_version as moVersion, filename, "

                for col in set(pk):
                    sql_str=sql_str+f"element_at(parameters,'{col}') as {col}, "

                sql_str=sql_str[:-2]+f" from hive.network_data.cm a where  a.raml_date= date '{max_date['last_date'][0]}' and a.parameters  is not null and  mo_class = '{table}'"

                cursor.execute(sql_str)

                data = cursor.fetchall()
                columns = [c[0] for c in cursor.description]

                data_CM = pd.DataFrame(data,columns=columns)


                if data_CM.shape[0]>0:

                    print(i, table, data_CM.shape, process_num)
                    i=i+1

                    # PLMN

                    data_CM['PLMN_orig']=data_CM['filename'].apply(lambda x: re.findall('rc[0-9]+',x)[0].upper())

                    data_CM['PLMN_id']=data_CM['PLMN_orig']

                    data_CM.drop(['filename','PLMN_orig'], axis=1, inplace=True)


                    #Verificar columnas duplicadas
                    columns_up=[x.upper() for x in data_CM.columns]
                    freq_col=[columns_up.count(element) for element in columns_up]

                    while 2 in freq_col:

                        data_CM.drop(data_CM.columns[freq_col.index(2)], axis=1, inplace=True)
                        
                        columns_up=[x.upper() for x in data_CM.columns]
                        freq_col=[columns_up.count(element) for element in columns_up]
                        print("COLUMNA REPETIDA !!!!!!")
                    
                    #print(data_CM.shape)

                    data_CM.replace(np.nan, None, inplace=True)

                    for field in data_CM.columns:
                        try:
                            data_CM[field]=data_CM[field].astype(np.int64)
                        except:
                            pass

                    data_CM.to_sql(name=str(table).upper(), con=con, if_exists='replace', index=False)
                    
                    exec_flag=True

            except Exception as e:
                print("Oops!", e.__class__, "occurred.") 
                error_times=error_times+1
                
                error_count.append([table, error_times,str(data_CM.shape)])

    #import csv
    #with open(f'file_{str(process_num)}.csv', 'w', newline='') as f:
        #writer = csv.writer(f)
        #writer.writerows(error_count)



    print("Total Time:",(time.time()-start_time)/60, "minutes")
    import csv

    return 0





if __name__ == '__main__':

    import requests
    import json
    import pandas as pd
    import time


    start_time_total = time.time()




    #Presto DB
    import prestodb
    #Presto Connection

    conn = prestodb.dbapi.connect(
        host=os.getenv('presto_host'),
        port=os.getenv('presto_port'),
        user='admin',
        catalog='hive',
        schema='network_data'
    )


    #Last Date

    cursor = conn.cursor()

    cursor.execute(f"""
        select max(raml_date) as last_date from hive.network_data.cm where mo_class='LNCEL' 
    """)

    data = cursor.fetchall()
    columns = [c[0] for c in cursor.description]

    max_date = pd.DataFrame(data,columns=columns)

    print(max_date['last_date'][0])

    max_date_str=str(max_date['last_date'][0]).replace('-','')



    filename_in = f"{os.getcwd()}/{max_date_str}_sqlite_FINAL.db"
    filename_out = f"{os.getcwd()}/{max_date_str}_sqlite_FINAL.db.gz"

    print(filename_in)
    print(filename_out)
    
    print(max_date['last_date'][0])

    
    #plist & itemlists



    #exit()

    #Parameters Tables

    cursor = conn.cursor()

    cursor.execute(f"""
        select mo_class, count(mo_class) as count_elem
        from hive.network_data.cm a  
        where  a.raml_date= date '{max_date['last_date'][0]}' and parameters  is not null
        group by mo_class


    """)

    data = cursor.fetchall()
    columns = [c[0] for c in cursor.description]

    tables_CM = pd.DataFrame(data,columns=columns)

    tables_CM.drop_duplicates(inplace=True)

    print(tables_CM.shape)


    #Divide Tables

    from random import randrange



    # Numero de procesos en paralelo
    p_paralell=6


    index_process=[randrange(p_paralell) for i in range(0,tables_CM.shape[0])]
    tables_CM['index_process']=index_process

    print(tables_CM.shape)

    #tables_CM.to_csv('tablas.csv')

    #exit()


    #ENTRENAMIENTO DE MODELOS CON MULTIPROCESSING

    
    #"""
    i=0

    import multiprocessing as mp
    processes=[]



    

    for i in range(0,p_paralell):

        tables = tables_CM[tables_CM['index_process']==i]['mo_class']

        #print(tables)


        processes.append(mp.Process(target=CM_Parameters, args=(tables, max_date, i)))      
            

    
    print('Procesos en Paralelo:\n',processes)
    


    # Run processes
    for p in processes:
        p.start()

    # Exit the completed processes
    for p in processes:
        p.join()




# Merge Databases

    conn0 = sqlite3.connect(f"{os.getcwd()}/{max_date_str}_sqlite_0.db")
    cur = conn0.cursor()

    for i in range(1,p_paralell):

        conn2 = sqlite3.connect(f"{os.getcwd()}/{max_date_str}_sqlite_{i}.db")
        cur2 = conn2.cursor()
        cur2.execute("SELECT * FROM sqlite_master where type='table'")
        rows = cur2.fetchall()

        start_time = time.time()

        for row in rows:

            try:
                df = pd.read_sql_query(f"SELECT * FROM '{row[1]}'", conn2)
                df.to_sql(name=row[1], con=conn0, if_exists='replace', index=False)
            except:
                print(f'Error en tabla {row[1]}')
        
        os.remove(f"{os.getcwd()}/{max_date_str}_sqlite_{i}.db")

        print("Total Time:",(time.time()-start_time)/60, "minutes")

    
    os.rename(f"{os.getcwd()}/{max_date_str}_sqlite_0.db", 
    f"{os.getcwd()}/{max_date_str}_sqlite_FINAL.db")





    print("\n\nCM Parameters EJECUTADO! \n")  
    print("\nTiempo de Ejecucion Total: "+str(round((time.time()-start_time_total)/60,2))+" minutos \n")


    #exit()




    result=CM_itemlists(max_date, conn)
    result=CM_plists(max_date, conn)


    # Zip file and upload to s3

    import os, sys, shutil, gzip
    print("Comprimiendo db!")
    with open(filename_in, "rb") as fin, gzip.open(filename_out, "wb") as fout:
        shutil.copyfileobj(fin, fout)





    import boto3

# S3 Connection

    s3_credentials = json.loads(str(os.environ['s3_credentials']))

    s3_bucket = s3_credentials["bucket"]

    s3 = boto3.session.Session().client(
        service_name='s3',
        aws_access_key_id=s3_credentials["access_key"],
        aws_secret_access_key=s3_credentials["secret_key"],
        endpoint_url=s3_credentials["endpoint"])


    print('Subiendo al s3')
    s3.upload_file(filename_out, 'claro-colombia-production' , f"Reports/LTE_700/CM/{max_date_str}_sqlite_FINAL.db.gz")
    
    os.remove(filename_out)  
    os.remove(filename_in)   
