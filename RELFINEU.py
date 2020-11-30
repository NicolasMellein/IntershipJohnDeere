import pandas as pd
import cx_Oracle

pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)



#Open password From File
with open("C:\\Nico\\KEMPER.txt") as f:

    password = f.read()

#con = Verbindung zu Infor aufbauen read password from file

con = cx_Oracle.connect("kemper/" + password + "@")
print('The Connection to Infor is open')


try:


#Query1 für RELAC.MNR = Materialnummer
#           RELAB.MNR = LIEFERANT
#           RELAB.IPOS = Erster Lieferant
#           RELAB.MINM = DISPOSITION BESTELLMENGE




    query = '''SELECT RELAC.MNR, RELAB.MNR AS LIEFERANT, RELAB.IPOS, RELACD.MINM AS LIMIT
                   FROM INFOR.RELAC RELAC
                   JOIN INFOR.RELAB ON RELAB.RLNR = RELAC.RLNR
                   JOIN INFOR.RELACD ON RELACD.MNR = RELAC.MNR
                   AND RELAB.IPOS = 1
                   AND RELAC.SAINT = 90
                   --AND RELAC.MNR IN ('M1905', 'LCA103788B',  '49762B', '47034E', '50666E')
         '''

    print('read SQL1 successfully')
    print('------------------------------------------------------------------------------------------------------------------------------------')
    df = pd.read_sql_query(query, con=con)

    df = df.drop(['IPOS'], axis=1)




    query1 = '''SELECT RELFI.artnr AS MNR , RELFI.periode4,RELFI.limit AS LIMIT, RELFI.BASIS, RELFI.finr AS LIEFERANT, RELFIRMA.NAME
                FROM INFOR.RELFI RELFI
                JOIN INFOR.RELFIRMA ON RELFIRMA.FIRMANR = RELFI.FINR
                WHERE FINR LIKE 'L%'
                --AND artnr IN ('M1905',  'LCA103788B', '49762B', '47034E', '50666E')
     '''

    df1 = pd.read_sql_query(query1, con=con)

    print('read SQL2 is successfully')
    print('------------------------------------------------------------------------------------------------------------------------------------')



    query2 = '''SELECT MNR,
                ktxt,
                anr,
                TERM_3 AS FRCST_DATE,
                MENG_3  AS FRCST_OUM_QTY,
               UTNR,
                zdesc AS Zustand,
                'FA_Ab' AS Ursprung
                FROM INFOR.reldb
                WHERE saint = 90
                AND zust < 5
                AND reserviert < meng_3
               UNION ALL
                SELECT MNR,
                ktxt,
                anr,
                SEGM3_TERM AS FRCST_DATE,
                SEGM3_MENG AS FRCST_OUM_QTY,
                UTNR,
                zdesc AS Zustand,
                'Dispo_Ab' AS Ursprung
                FROM INFOR.relcb
                WHERE saint = 90 AND zust < 5'''


    df2 = pd.read_sql_query(query2, con=con)



    print('read SQL3 is successfully')
    print('------------------------------------------------------------------------------------------------------------------------------------')

    #print(df1)



    df1 = pd.merge(df, df1, on=['MNR', 'LIEFERANT'], how='inner')



    print('Merge Lieferanten where IPOS == 1 jedoch mit Staffelpreise ')
    #print(type(df1))





    #print(df1.head())

    print('------------------------------------------------------------------------------------------------------------------------------------')


    #len == 1 Alle Artikel wo es nur 1 Preis gibt

    #len >1 Staffelpreise, Artikel wo man kein Preis zuordnen kann

    df_single_values = pd.concat(g for _, g in df1.groupby('MNR') if len(g) == 1)

    df_dublicated = pd.concat(g for _,g in df1.groupby('MNR') if len(g)>1)

    #filter wo Minm = limit

    df_periode4_where_limit_equal_minm = df_dublicated[df_dublicated['LIMIT_x'] == df_dublicated['LIMIT_y']]


    #frames sind die DF die zusammengeführt werden


    frames = [df_single_values,df_periode4_where_limit_equal_minm]


    #pd.concat = UNION = Daten werden angehängt
    df_single_values_plus_df_where_limit_minm = pd.concat(frames)


    #dropduplicates
    df_single_values_plus_df_where_limit_minm = df_single_values_plus_df_where_limit_minm.drop_duplicates()

    #drop Limit und Minm da diese nicht benötigt werden
    df_single_values_plus_df_where_limit_minm = df_single_values_plus_df_where_limit_minm.drop(['LIMIT_x'], axis=1)

    #merge preise mit Forcecast Artikel
    #df_single_values_plus_df_where_limit_minm = Sind Einkaufsartikel mit richtigem Preis
    #df2 = Forecast Abgänge

    df_single_values_plus_df_where_limit_minm = pd.merge(df_single_values_plus_df_where_limit_minm, df2, on=['MNR'], how='left')

    df_single_values_plus_df_where_limit_minm = df_single_values_plus_df_where_limit_minm.drop(["URSPRUNG", "UTNR", "ANR" ], axis=1)

    print(df_single_values_plus_df_where_limit_minm.head())

    print('------------------------------------------------------------------------------------------------------------------------------------')

    print('df to xlsx and csv')

    df_single_values_plus_df_where_limit_minm.to_excel('C:/Users/junxonm/Desktop/df_single_values_plus_df_where_limit_minm.xlsx', index=False)

    df_single_values_plus_df_where_limit_minm.to_csv(r'C:/Users/junxonm/Desktop/df_single_values_plus_df_where_limit_minm.csv', index=False, header=True)

    print( '------------------------------------------------------------------------------------------------------------------------------------')

    print('starts hyperAPI process')

    import tableauserverclient as TSC
    from tableauhyperapi import HyperProcess, Connection, TableDefinition, SqlType, Telemetry, Inserter, CreateMode, TableName


    PATH_TO_CSV = 'C:/Users/junxonm/Desktop/df_single_values_plus_df_where_limit_minm.csv'

    PATH_TO_HYPER = 'Kemper_Forcecast.hyper'


    my_df = pd.read_csv(PATH_TO_CSV)
    my_df['FRCST_DATE'] = my_df['FRCST_DATE'].str.slice(0, 10)
    my_df['FRCST_DATE'] = pd.to_datetime(my_df["FRCST_DATE"], format="%Y-%m-%d").dt.strftime('%Y-%m-%d')
    my_df['KTXT'] = my_df['KTXT'].astype(str)
    my_df['ZUSTAND'] = my_df['ZUSTAND'].astype(str)
    my_df['MNR'] = my_df['MNR'].astype(str)
    my_df['LIEFERANT'] = my_df['LIEFERANT'].astype(str)
    my_df['NAME'] = my_df['NAME'].astype(str)


# Step 1: Start a new private local Hyper instance

    with HyperProcess(Telemetry.SEND_USAGE_DATA_TO_TABLEAU, 'myapp') as hyper:
        print("The HyperProcess has started.")
    # Step 2:  Create the the .hyper file, replace it if it already exists
        with Connection(endpoint=hyper.endpoint, create_mode=CreateMode.CREATE_AND_REPLACE, database=PATH_TO_HYPER) as connection:
            print("The connection to the Hyper file is open.")
        # Step 3 : Create the Schema
            connection.catalog.create_schema('Extract')

        # Step 4: Create the table definition
            schema = TableDefinition(table_name=TableName('Extract', 'Extract'),
                columns=[
                    TableDefinition.Column('MNR', SqlType.text()),
                    TableDefinition.Column('LIEFERANT', SqlType.text()),
                    TableDefinition.Column('PERIODE4', SqlType.double()),
                    TableDefinition.Column('LIMIT_y', SqlType.double()),
                    TableDefinition.Column('BASIS', SqlType.double()),
                    TableDefinition.Column('NAME', SqlType.text()),
                    TableDefinition.Column('KTXT', SqlType.text()),
                    TableDefinition.Column('FRCST_DATE', SqlType.text()),
                    TableDefinition.Column('FRCST_OUM_QTY', SqlType.double()),
                    TableDefinition.Column('ZUSTAND', SqlType.text()),

                ])
            print("The table is defined.")
        # Step 5: Create the table in the connection catalog
            connection.catalog.create_table(schema)
            with Inserter(connection, schema) as inserter:
                for index, row in my_df.iterrows():
                    inserter.add_row(row)

                inserter.execute()

            print("The data was added to the table.")
            print("The connection to the Hyper extract file is closed.")
            print("The HyperProcess has shut down.")
        # create a new instance of a TableauAuth object for authentication

        #Open password From File

    with open("C:\\Nico\\Password.txt") as f:
            password = f.read()




            #Updating/Loading  Extracts on Tableau Server - Tableau


            #create an auth object
            tableau_auth = TSC.TableauAuth(username='junxonm', password=password)


            #create an instance for your server
            server = TSC.Server('https://tableau.deere.com/')

            #call the sign-in method with the auth object
            server.auth.sign_in(tableau_auth)

            with server.auth.sign_in(tableau_auth):

                #datasources.publish(datasource_item, file_path, mode, connection_credentials=None)

                #Documentation: https://tableau.github.io/server-client-python/docs/api-ref#datasourcespublish

                #projectID = Project-ID von “Supply Management Shared Services Region 2 (Restricted)”: d0e92725-c4be-49c4-afb3-e4d0159722b1

                project_id = ''
                file_path = 'Kemper_Forcecast.hyper'

                #Use the project id to create new datsource_item
                new_datasource = TSC.DatasourceItem(project_id)

                #publish data source (specified in file_path)
                new_datasource = server.datasources.publish(new_datasource, file_path, 'Overwrite') #or Createnew

            #Specifies whether you are publishing a new data source (CreateNew), overwriting an existing data source (Overwrite),
            #or appending data to a data source (Append). If you are appending to a data source, the data source on the server and the
            #data source you are publishing must be be extracts (.tde files) and they must share the same schema. You can also use the publish mode
            #attributes, for example: TSC.Server.PublishMode.Overwrite.


    print('hyperAPI process successfully ')


except cx_Oracle.DatabaseError as de:
    print(de)

finally:

    con.close()
    print('Connection to Infor is close')

