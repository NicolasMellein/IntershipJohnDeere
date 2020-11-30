#Autor: Nicolas Mellein Intern Supply Management
#Code stellt Daten von Kemper Stadlohn für JSDN zur Verfügung.
#verwendetet Library:
#cx_Oracle API https://cx-oracle.readthedocs.io/en/latest/
#Pandas https://pandas.pydata.org/docs/
#Numpy https://numpy.org/doc/
#Daten kommen direkt aus Infor und werden mit Pandas und Numpy aufbereitet


#Librarys
import cx_Oracle
import pandas as pd
import numpy as np


print('import cx_Oracle')
print('pandas as pd')
print('import numpy as np')

print('-----------------------------------------------------------------------------------------------------------------------------------------')

#Formateinstellunng in der Console um ganzen Inhalt zu zeigen
pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)



#Open password From File
with open("C:\\Nico\\KEMPER.txt") as f:

    password = f.read()

#Verbindung zu Infor aufbauen read password from file

con = cx_Oracle.connect("kemper/" + password + "")
print('connected to Infor successfully')
print('-----------------------------------------------------------------------------------------------------------------------------------------')





try:


#1. SQL: Primär Stammdaten
#RELFI.ARTNR = Materialnummer
#RELAC.KTXT = Materialbeschreibung
#RELFIRMA.SACHBEARBEITER = Sachbearbeiter
#RELFIRMA.EAN = Johndeere Lieferantennummer
#RELAC.KANBANITEM = Kanbanteil Y/N
#RELPERSON.NAME Sachbearbeiter= Name
#RELFI.FINR= Kemper Firmennummer

    query1 = '''SELECT RELFI.ARTNR AS ITEM_CD, RELAC.KTXT AS SHORT_DSC, RELFIRMA.SACHBEARBEITER AS TCTCL_CNTCT_ID, RELFIRMA.EAN AS ORD_SPLR_SS_CD, RELAC.KANBANITEM AS TRGD_ITEM_CD,
                RELPERSON.NAME AS TCTCL_CNTCT_NM, RELFI.FINR AS SUPPLIER
                FROM INFOR.RELFI RELFI
                JOIN INFOR.RELFIRMA ON RELFIRMA.FIRMANR = RELFI.FINR
                JOIN INFOR.RELAC ON RELAC.MNR = RELFI.ARTNR
                JOIN INFOR.RELPERSON ON RELPERSON.PERSONNR = RELFIRMA.SACHBEARBEITER
                --WHERE artnr IN ('LCA107540E', 'LCA106973E') 
                
                
                '''

#SQL in DF laden

    df1 = pd.read_sql_query(query1, con=con)
    print('read SQL1 successfully')

    df1 = df1.drop_duplicates()
    print(df1.head())

#2.SQL Lieferanten, Materialnummer
#RELFA.MNR = Materialnummer
#RELFA.EXTARTNR = Externe Lieferantennummer

######!!!!!!!!!  RELFA.EXTARTNR konnte nicht verwendet werden


    #query2 = '''SELECT  RELFA.MNR AS ITEM_CD, RELFA.EXTARTNR AS SPLR_ITM_CD
     #             FROM INFOR.RELFA
     #             WHERE MNR IN ('LCA107540E', 'LCA106973E')
     #             '''

# load SQL in DF
   # df2 = pd.read_sql_query(query2, con=con)

    #print('SQL2 complete ')
    #print(df2.head())

#3. SQL
#RELEPRELEASEORDERHEAD.ITEMNO = Materialnummer
#RELEPRELEASEORDERHEAD.Releaseorderno = Auftragsnummer (Abrufrahmenvertrag)
#WHERE Releaseorderno LIKE '700%' Abrufauftraege fangen immer mit 700 an!


    query3 = '''SELECT RELEPRELEASEORDERHEAD.ITEMNO AS ITEM_CD, RELEPRELEASEORDERHEAD.Releaseorderno AS PRCHG_REF_DOC, RELEPRELEASEORDERHEAD.Supplier AS SUPPLIER
                FROM INFOR.RELEPRELEASEORDERHEAD
                WHERE Releaseorderno LIKE '700%'  
               
                '''


# load SQL in DF

    df3 = pd.read_sql_query(query3, con=con)

#drop duplicates
    df3 = df3.drop_duplicates()

    print('read SQL3 successfully')
    print(df3.head())

#4. SQL
#MNR = Materialnummer
#ktxt = Materialbeschreibung
#TERM_3 = Lieferdatum
#MENG_3 = Quantity
#



    query4 = '''
               SELECT MNR AS ITEM_CD,
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
                --AND MNR IN ('LCA107540E', 'LCA106973E'  )
                UNION ALL
                SELECT MNR AS ITEM_CD,
                ktxt,
                anr,
                SEGM3_TERM AS FRCST_DATE,
                SEGM3_MENG AS FRCST_OUM_QTY,
                UTNR,
                zdesc AS Zustand,
                'Dispo_Ab' AS Ursprung
                FROM INFOR.relcb
                WHERE saint = 90 AND zust < 5
                 
              
                '''


#load SQL 4 in DF

    df4 = pd.read_sql_query(query4, con=con)
    print('read SQL4 successfully')
    print('-----------------------------------------------------------------------------------------------------------------------------------------')

    df4['FRCST_DATE'] = df4['FRCST_DATE'].dt.normalize()

#Dropna wirft dropt rows in denen keine Werte Vorhanden sind Bsp. NaN NaT
    df4 = df4.dropna(inplace=False)

#dt.strftime('%Y-%m-%d') wandelt datetime in String um
    df4['FRCST_DATE'] = pd.to_datetime(df4["FRCST_DATE"], format="%Y%m%d%").dt.strftime('%Y%m%d')



#drop nicht verwendete columns axis:
#Axis 0 will act on all the ROWS in each COLUMN
#Axis 1 will act on all the COLUMNS in each ROW
    df4 = df4.drop(['KTXT', 'UTNR', 'ZUSTAND', 'URSPRUNG', 'ANR',], axis=1)



#Groupby Materialnummer und ForcastDate plus sum()


    df4 = df4.groupby(['ITEM_CD', 'FRCST_DATE']).sum()



#reset index da druch groubby nicht in jeder row die Artikelnummer vorhanden ist
#druch reset_index() habe ich das entsprechende Format

    df4 = df4.reset_index()

    print(df4.head())

#Mergen der DataFrame
    print('start with merge')

#Merge1 SQL1 und SQL2 auf Materialnummer left join da nicht immer ein Fremd Materialnummer vorhanden ist.

    #df1 = pd.merge(df1,df2, on="ITEM_CD", how='left' )
    #print('merge1')
    #print(df1.head())

#Merge1 SQL1 und SQL3 auf Materialnummer und Lieferantennummer

    df1 = pd.merge(df1,df3, on=["ITEM_CD" ,"SUPPLIER"], how='inner')

    print('merge2')
    print(df1.head())

#Merge1 SQL1 und SQL3 auf Materialnummer

    df1 = pd.merge(df1,df4, on="ITEM_CD", how='inner')


    print('merge3')
    print(df1.head())

    print('merge successful ')
    print('-----------------------------------------------------------------------------------------------------------------------------------------')


#Template für Flatfile
#Reihenfolge nach Definition


#str.slice = definiert die Länge des Strings
#str.ljust The ljust() method will left align the string, using a specified character (space is default) as the fill character.


    #Hardcode "KM00"
    df1['UNIT_CD']= 'KM00'
    df1['UNIT_CD'] = df1['UNIT_CD'].str.slice(0, 4)
    df1['UNIT_CD'] = df1['UNIT_CD'].str.ljust(4, ' ')


    #Materialnnummer
    #df['ITEM_CD']='ITEM_CD'
    df1['ITEM_CD'] = df1['ITEM_CD'].str.slice(0, 18)
    df1['ITEM_CD'] = df1['ITEM_CD'].str.ljust(18, ' ')


    #Itemcorp
    df1['ITEM_MTHD_CD']= 'ITEMCORP'
    df1['ITEM_MTHD_CD'] = df1['ITEM_MTHD_CD'].str.slice(0, 8)
    df1['ITEM_MTHD_CD'] = df1['ITEM_MTHD_CD'].str.ljust(8, ' ')

    #Halbfabrikat
    df1['ITEM_PRCHG_MTHD_CD']= 'HALB'
    df1['ITEM_PRCHG_MTHD_CD'] = df1['ITEM_PRCHG_MTHD_CD'].str.slice(0, 4)
    df1['ITEM_PRCHG_MTHD_CD'] = df1['ITEM_PRCHG_MTHD_CD'].str.ljust(4, ' ')

    #Production
    df1['DMND_TYP_CD']= 'P'
    df1['DMND_TYP_CD'] = df1['DMND_TYP_CD'].str.slice(0, 1)
    df1['DMND_TYP_CD'] = df1['DMND_TYP_CD'].str.slice(0, 1)


    #JD Lieferantennummer
    #df1['ORD_SPLR_SS_CD']= ""
    df1['ORD_SPLR_SS_CD'] = df1['ORD_SPLR_SS_CD'].str.slice(0, 15)
    df1['ORD_SPLR_SS_CD'] = df1['ORD_SPLR_SS_CD'].astype(str).str.zfill(15)


    #Hardcode “SUPLCORP”
    df1['ORD_SPLR_MTHD_CD']= 'SUPLCORP'
    df1['ORD_SPLR_MTHD_CD'] = df1['ORD_SPLR_MTHD_CD'].str.slice(0, 8)
    df1['ORD_SPLR_MTHD_CD'] = df1['ORD_SPLR_MTHD_CD'].str.ljust(8, ' ')


    #Lieferdatum
    #wird nach SQL 4 Formatiert
    #df1['FRCST_DATE'] = ""
    #df1.replace({'NaT': ''}, inplace=True)



    #Nochmal JD Lieferantennummer
    df1['SHP_TO_SPLR_SS_CD']= df1['ORD_SPLR_SS_CD']
    df1['SHP_TO_SPLR_SS_CD'] = df1['SHP_TO_SPLR_SS_CD'].astype(str).str.zfill(15)
    df1['SHP_TO_SPLR_SS_CD'] = df1['SHP_TO_SPLR_SS_CD'].str.slice(0,15)


    #Hardcode "SUPLCORP"
    df1['SHP_TO_SPLR_MTHD_CD']= 'SUPLCORP'
    df1['SHP_TO_SPLR_MTHD_CD'] = df1['SHP_TO_SPLR_MTHD_CD'].str.slice(0, 8)
    df1['SHP_TO_SPLR_MTHD_CD'] = df1['SHP_TO_SPLR_MTHD_CD'].str.ljust(8, ' ')


    #Lieferanten-Materialnummer
    # Numpy.replace nimmt NAN  Werte und fühlt diese mit leeren String
    #df1 = df1.replace(np.nan, '', regex=True)
    df1['SPLR_ITM_CD'] = '0000000000000000000000'
    df1['SPLR_ITM_CD'] = df1['SPLR_ITM_CD'].str.slice(0, 22)
    df1['SPLR_ITM_CD'] = df1['SPLR_ITM_CD'].str.ljust(22, ' ')

    #Kanban-Teil? (Y/N)
    #Mapping von 1: 'Y' und 0 : 'N'
    df1['TRGD_ITEM_CD'] = df1['TRGD_ITEM_CD'].map({1: 'Y', 0: 'N'})
    df1['TRGD_ITEM_CD'] = df1['TRGD_ITEM_CD'].astype(str)
    df1['TRGD_ITEM_CD'] = df1['TRGD_ITEM_CD'].str.slice(0, 1)


    #Bedarfsmenge
    #display.float_format Stellt float ohne Nachkommastelle dar
    #pd.options.display.float_format = '{:,.0f}'.format == wurde leider dann falschh angezeigt daher so
    # == erst in int umwandeln dann in Str und mit O auffüllen
    df1['FRCST_OUM_QTY'] = df1['FRCST_OUM_QTY'].astype(int)
    df1['FRCST_OUM_QTY'] = df1['FRCST_OUM_QTY'].astype(str)
    df1['FRCST_OUM_QTY'] = df1['FRCST_OUM_QTY'].astype(str).str.zfill(7)

    #'PC ' HARDCODE
    df1['ORD_UOM_SS_CD'] = 'PC '
    df1['ORD_UOM_SS_CD'] = df1['ORD_UOM_SS_CD'].str.slice(0, 3)
    df1['ORD_UOM_SS_CD'] = df1['ORD_UOM_SS_CD'].str.ljust(3, ' ')


    # 'PC ' HARDCODE
    df1['ORD_UOM_STD_CD'] = 'PC '
    df1['ORD_UOM_STD_CD'] = df1['ORD_UOM_STD_CD'].str.slice(0, 3)
    df1['ORD_UOM_STD_CD'] = df1['ORD_UOM_STD_CD'].str.ljust(3, ' ')

    # 'PC ' HARDCODE
    df1['USG_UOM_SS_CD'] = 'PC '
    df1['USG_UOM_SS_CD'] = df1['USG_UOM_SS_CD'].str.slice(0, 3)
    df1['USG_UOM_SS_CD'] = df1['USG_UOM_SS_CD'].str.ljust(3, ' ')

    # 'PC ' HARDCODE
    df1['USG_UOM_STD_CD'] = 'PC '
    df1['USG_UOM_STD_CD'] = df1['USG_UOM_STD_CD'].str.slice(0, 3)
    df1['USG_UOM_STD_CD'] = df1['USG_UOM_STD_CD'].str.ljust(3, ' ')

    #'1.000000' HARDCODE
    df1['CVRSN_FCTR_AMT']= '00001000000'
    df1['CVRSN_FCTR_AMT'] = df1['CVRSN_FCTR_AMT'].str.slice(0, 11)


    #Einkäufer Gruppe HARDCODE
    df1['PRCHG_ORG_CD']= 'A000'
    df1['PRCHG_ORG_CD'] = df1['PRCHG_ORG_CD'].str.slice(0, 4)
    df1['PRCHG_ORG_CD'] = df1['PRCHG_ORG_CD'].str.ljust(4, ' ')

    #Einkäufer
    df1['PRCHG_GRP_CD']= '000'
    df1['PRCHG_GRP_CD'] = df1['PRCHG_GRP_CD'].str.slice(0, 3)
    df1['PRCHG_GRP_CD'] = df1['PRCHG_GRP_CD'].str.ljust(3, ' ')

    #Materialbezeichnung
    #df['SHORT_DSC']=''
    df1['SHORT_DSC'] = df1['SHORT_DSC'].str.slice(0, 20)
    df1['SHORT_DSC'] = df1['SHORT_DSC'].str.ljust(20, ' ')

    #Kürzel Lieferplan
    df1['PRCHG_DOC_TYP_CD']= 'KM'
    df1['PRCHG_DOC_TYP_CD'] = df1['PRCHG_DOC_TYP_CD'].str.slice(0, 5)
    df1['PRCHG_DOC_TYP_CD'] = df1['PRCHG_DOC_TYP_CD'].str.ljust(5, ' ')


    #Disponent UserID
    df1['TCTCL_CNTCT_ID'] = df1['TCTCL_CNTCT_ID'].str.slice(0, 7)
    df1['TCTCL_CNTCT_ID'] = df1['TCTCL_CNTCT_ID'].str.ljust(7, ' ')


    #Disponent Name
    #df1['TCTCL_CNTCT_NM'] = ''
    df1['TCTCL_CNTCT_NM'] = df1['TCTCL_CNTCT_NM'].str.slice(0, 25)
    df1['TCTCL_CNTCT_NM'] = df1['TCTCL_CNTCT_NM'].str.ljust(25, ' ')


    #TCTCL_CNTCT_DSK

    df1['TCTCL_CNTCT_DSK']=  df1['TCTCL_CNTCT_ID'].str.slice(0, 2)
    df1['TCTCL_CNTCT_DSK'] = df1['TCTCL_CNTCT_DSK'].str.ljust(2, ' ')

    #Kanban - Teil? (Y / N)
    #Mapping von  1: 'Y' und 0 : 'N'
    #astype umwandel in String für str.slice
    df1['TRGD_ITEM_TYP_CD'] = df1['TRGD_ITEM_CD'].map({1: 'Y', 0: 'N'})
    df1['TRGD_ITEM_TYP_CD'] = df1['TRGD_ITEM_CD'].astype(str)
    df1['TRGD_ITEM_TYP_CD'] = df1['TRGD_ITEM_CD'].str.slice(0, 1)
    #df['TRGD_ITEM_TYP_CD'] = df['TRGD_ITEM_TYP_CD'].astype(str).str.zfill(1)

    #Timestamp
    # insert fügt Timestamp ein
    # in pd.to_dateime um format zu ändern
    # astype umwandel in String für str.slice
    df1.insert(0, 'REC_EXT_TS', pd.datetime.now())
    df1['REC_EXT_TS'] = pd.to_datetime(df1["REC_EXT_TS"], format='%Y%m%d').dt.strftime('%Y-%m-%d')
    df1['REC_EXT_TS'] = df1['REC_EXT_TS'].str.ljust(10, ' ')


    #Hardcode Timestamp hours, minutes, seconds wird als String an den Timestamp angehängt
    df1['REC_EXT_TS2'] = "-00.00.00.000000"
    df1['REC_EXT_TS2'] = df1['REC_EXT_TS2'].str.ljust(16, ' ')

    #SPCL_PROC_CD
    df1['SPCL_PROC_CD']= '00'
    df1['SPCL_PROC_CD'] = df1['SPCL_PROC_CD'].astype(str).str.zfill(2)
    df1['SPCL_PROC_CD'] = df1['SPCL_PROC_CD'].str.slice(0, 2)


    # Lieferplannummer
    #df1['PRCHG_REF_DOC']= ''
    df1['PRCHG_REF_DOC'] = df1['PRCHG_REF_DOC'].str.ljust(10, ' ')

    #Lieferplan-Position
    df1['PRCHG_REF_LINE_NO']= '10'
    df1['PRCHG_REF_LINE_NO'] = df1['PRCHG_REF_LINE_NO'].astype(str)
    df1['PRCHG_REF_LINE_NO'] = df1['PRCHG_REF_LINE_NO'].astype(str).str.zfill(5)
    df1['PRCHG_REF_LINE_NO'] = df1['PRCHG_REF_LINE_NO'].str.slice(0, 5)

    # leer lassen
    df1['FSPLR']= '0000000000'
    df1['FSPLR'] = df1['FSPLR'].str.slice(0, 10)
    df1['FSPLR'] = df1['FSPLR'].astype(str).str.zfill(10)


    #leer lassen
    df1['REC_FILLER']= '00000000000000000000000'
    df1['REC_FILLER'] = df1['REC_FILLER'].str.slice(0, 23)
    df1['REC_FILLER'] = df1['REC_FILLER'].astype(str).str.zfill(23)





#Sortierung der Datefelder laut Definition


    df1 = df1[['UNIT_CD', 'ITEM_CD', 'ITEM_MTHD_CD', 'ITEM_PRCHG_MTHD_CD', 'DMND_TYP_CD', 'ORD_SPLR_SS_CD','ORD_SPLR_MTHD_CD', 'FRCST_DATE','SHP_TO_SPLR_SS_CD', 'SHP_TO_SPLR_MTHD_CD', 'SPLR_ITM_CD'
        , 'TRGD_ITEM_CD','FRCST_OUM_QTY','ORD_UOM_SS_CD','ORD_UOM_STD_CD','USG_UOM_SS_CD','USG_UOM_STD_CD', 'CVRSN_FCTR_AMT','PRCHG_ORG_CD','PRCHG_GRP_CD','SHORT_DSC'
        , 'PRCHG_DOC_TYP_CD','TCTCL_CNTCT_ID','TCTCL_CNTCT_NM','TCTCL_CNTCT_DSK','TRGD_ITEM_TYP_CD','REC_EXT_TS','REC_EXT_TS2', 'SPCL_PROC_CD', 'PRCHG_REF_DOC', 'PRCHG_REF_LINE_NO','FSPLR'
        , 'REC_FILLER']]

#Sortierung nach Materialnummer und Datum absteigend

    #df1['FRCST_DATE'] = df1['FRCST_DATE'].dt.normalize()
    df1 = df1.sort_values(by=['ITEM_CD', 'FRCST_DATE'])



    #print(df1)



#DF zusammenführen für TXT-File
    

    dfx =       df1['UNIT_CD'] + df1['ITEM_CD'] + df1['ITEM_MTHD_CD'] + df1['ITEM_PRCHG_MTHD_CD']+ df1['DMND_TYP_CD']+ df1['ORD_SPLR_SS_CD'] + df1['ORD_SPLR_MTHD_CD'] + df1['FRCST_DATE']  + df1['SHP_TO_SPLR_SS_CD'] + df1['SHP_TO_SPLR_MTHD_CD'] + \
                df1['SPLR_ITM_CD'] + df1['TRGD_ITEM_CD'] + df1['FRCST_OUM_QTY'] + df1['ORD_UOM_SS_CD'] + df1['ORD_UOM_STD_CD'] + df1['USG_UOM_SS_CD'] + \
                df1['USG_UOM_STD_CD'] + df1['CVRSN_FCTR_AMT'] + df1['PRCHG_ORG_CD']+df1['PRCHG_GRP_CD'] + df1['SHORT_DSC'] + df1['PRCHG_DOC_TYP_CD'] + \
                df1['TCTCL_CNTCT_ID'] + df1['TCTCL_CNTCT_NM'] + df1['TCTCL_CNTCT_DSK'] + df1['TRGD_ITEM_TYP_CD']+ df1['REC_EXT_TS']+df1['REC_EXT_TS2'] +df1['SPCL_PROC_CD'] +\
                df1['PRCHG_REF_DOC'] + df1['PRCHG_REF_LINE_NO'] + df1['FSPLR']+df1['REC_FILLER']

    #print(dfx.head())
    print('-----------------------------------------------------------------------------------------------------------------------------------------')



#DF To EXEL index = False

    df1.to_excel('C:/Users/junxonm/Desktop/RDJBIAFD.xlsx', index= False)
    #print('load to xlsx')

#DF to TXT index= False header = False
#Df safe as Txt ohne qoutes speichern



    #print('load to txt')



except cx_Oracle.DatabaseError as e:
    print(cx_Oracle.DatabaseError)



finally:

#Close Connection
    con.close()
    print('Connection close')


print('-----------------------------------------------------------------------------------------------------------------------------------------')
print('-----------------------------------------------------------------------------------------------------------------------------------------')


import os
import zosftplib


dirpath = os.getcwd() # directory where this Python program is in


ftp_handler = zosftplib.Zftp("", "A341878", "", timeout=500.0, sbdataconn="(IBM-037,ISO8859-1)")


# Documentation of IBM Mainframe FTP parameters: https://www.ibm.com/support/pages/sitelocsite-commands-mvs-ftp
# Separate multiple parameters by a blank space.
# LRecl=nnn Specifies the record size in a data set.
# RECfm=format Specifies the record format of a data set. F=Fixed record length, B=Blocked records
# BLocksize=nnnn Specifies the block size of a data set.




# File wird aktuell aus Pycharm gezogen

ftp_handler.upload_text(dirpath + "\\Filetestfull.txt", "", sitecmd="LRECL=285 RECFM=FB BLKSIZE=27930")

print('FTP transfer successfuly')
