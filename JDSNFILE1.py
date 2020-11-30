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

#SQL Daten aus INFOR

try:



    print('----------------------------------------------------------------------------------------------------------------------------------------')
    print('SQL for PERIODE4')
    ## query1x und query2x werden hier als erstes aufgeführt, hier kommt es zu Code- Redundanzen dieser wird aber zu bessern Lessbarkeit des Codes in Kauf genommen

    query1x = '''SELECT RELAC.MNR AS ITEM_CD, RELAB.MNR AS FINR, RELAB.IPOS, RELACD.MINM AS LIMIT, RELAC.WBZ AS PLANNED_DLVRY
                   FROM INFOR.RELAC RELAC
                   JOIN INFOR.RELAB ON RELAB.RLNR = RELAC.RLNR
                   JOIN INFOR.RELACD ON RELACD.MNR = RELAC.MNR
                   AND RELAB.IPOS = 1
                   AND RELAC.SAINT = 90
                  
         '''


    df1x = pd.read_sql_query(query1x, con=con)

    df1x = df1x.drop(['IPOS'], axis=1)


    query2x = '''SELECT RELFI.artnr AS ITEM_CD , RELFI.periode4 AS NET_PRICE,RELFI.limit AS LIMIT, RELFI.BASIS AS UNIT_PER, RELFI.FINR
                FROM INFOR.RELFI RELFI
                JOIN INFOR.RELFIRMA ON RELFIRMA.FIRMANR = RELFI.FINR
                WHERE FINR LIKE 'L%'
                
     '''

    df2x = pd.read_sql_query(query2x, con=con)


    df2x = pd.merge(df1x, df2x, on=['ITEM_CD', 'FINR'], how='inner')

    # Wir unterscheiden zwischen 2 Artikel
    # 1. Artikel wo  nur ein Preis hinterlegt ist == länge 1
    # 2. Artikel wo Staffelpreise hinterlegt sind == länge > 1

    df_single_values = pd.concat(g for _, g in df2x.groupby('ITEM_CD') if len(g) == 1)

    df_dublicated = pd.concat(g for _, g in df2x.groupby('ITEM_CD') if len(g) > 1)

    # 2. Artikel wo Staffelpreise sind  merge where Limit = Menge

    df_periode4_where_limit_equal_minm = df_dublicated[df_dublicated['LIMIT_x'] == df_dublicated['LIMIT_y']]

    # Frames zu verstehen wie ein Union. Füge df wo Limit == menge ist an df wo es nur 1 Artikel und 1 Preis gibt
    frames = [df_single_values, df_periode4_where_limit_equal_minm]

    #pd.concat ist wie ein Union

    df_single_values_plus_df_where_limit_minm = pd.concat(frames)

    # drop duplicates

    df_single_values_plus_df_where_limit_minm = df_single_values_plus_df_where_limit_minm.drop_duplicates()

    # drop nicht benötige Columns

    df_single_values_plus_df_where_limit_minm = df_single_values_plus_df_where_limit_minm.drop(['LIMIT_x', 'LIMIT_y'], axis=1)


    print('------------------------------------------------------------------------------------------------------------------------------------')




    query = '''SELECT RELFI.ARTNR AS ITEM_CD, RELFIRMA.EAN AS ORD_FRM_SPLR_CS_CD, RELFI.WE, RELFI.WE AS NET_PRICE_CRNCY, RELFI.ME AS ORD_PRICE_UNIT, RELFI.FINR
                FROM INFOR.RELFI RELFI
                JOIN INFOR.RELFIRMA ON RELFIRMA.FIRMANR = RELFI.FINR  
                WHERE FINR LIKE 'L%'             
               
                
                '''

    df = pd.read_sql_query(query, con = con)



    print('read SQL1 successfully')
    df = pd.merge(df,df_single_values_plus_df_where_limit_minm, on=['ITEM_CD', 'FINR'], how='inner')

    df = df.drop_duplicates()



    query2 = '''SELECT RELEPRELEASEORDERHEAD.ITEMNO AS ITEM_CD, RELEPRELEASEORDERHEAD.Releaseorderno AS SCHDL_AGRMT, RELEPRELEASEORDERHEAD.Supplier AS FINR, RELEPRELEASEORDERHEAD.Createdate AS VLD_FROM_DT,
                    RELACP.Text0, RELZTLB.INCOTERM AS INCO_TERMS
                    FROM INFOR.RELEPRELEASEORDERHEAD
                    JOIN INFOR.RELACP ON RELACP.MNR = RELEPRELEASEORDERHEAD.Supplier 
                    JOIN INFOR.RELZTLB ON RELZTLB.Ztkey = RELACP.Text0
                    WHERE Releaseorderno LIKE '700%'
                   
                  '''


    df2 = pd.read_sql_query(query2, con=con)
    print('read SQL2 successfully')

    df2 = df2.groupby(['ITEM_CD'], as_index=True).agg(lambda x: x.min())

    df = pd.merge(df, df2, on=['ITEM_CD', 'FINR'], how='inner')

    df = df.drop_duplicates()





    #Framework für Daten

    # PLANT-CD Hardcode
    df['PLANT_CD'] = 'KM00'
    df['PLANT_CD'] = df['PLANT_CD'].str.slice(0, 4)
    df['PLANT_CD'] = df['PLANT_CD'].str.ljust(4, ' ')


    # ITEM-CD
    #df['ITEM_CD'] = 'ITEM_CD'
    df['ITEM_CD'] = df['ITEM_CD'].str.slice(0, 18)
    df['ITEM_CD'] = df['ITEM_CD'].str.ljust(18, ' ')

    # ORD-FRM-SPLR-CS-CD
    #df['ORD_FRM_SPLR_CS_CD'] = 'ORD_FRM_SPLR_CS_CD'
    df['ORD_FRM_SPLR_CS_CD'] = df['ORD_FRM_SPLR_CS_CD'].str.slice(0, 10)
    df['ORD_FRM_SPLR_CS_CD'] = df['ORD_FRM_SPLR_CS_CD'].astype(str).str.zfill(10)

    # SCHDL-AGRMT
    #df['SCHDL_AGRMT'] = 'SCHDL_AGRMT'
    df['SCHDL_AGRMT'] = df['SCHDL_AGRMT'].str.slice(0, 10)
    #df['SCHDL_AGRMT'] = df['SCHDL_AGRMT'].astype(str).str.zfill(10)
    df['SCHDL_AGRMT'] = df['SCHDL_AGRMT'].str.ljust(10, ' ')

    # SHP-FRM-SPLR-CS-CD
    df['SHP_FRM_SPLR_CS_CD'] = df['ORD_FRM_SPLR_CS_CD']
    df['SHP_FRM_SPLR_CS_CD'] = df['SHP_FRM_SPLR_CS_CD'].str.slice(0, 10)
    df['SHP_FRM_SPLR_CS_CD'] = df['SHP_FRM_SPLR_CS_CD'].astype(str).str.zfill(10)

    # PRCH-DOC-TYP-CD Hardcode KM
    df['PRCH_DOC_TYP_CD'] = 'LPA '
    df['PRCH_DOC_TYP_CD'] = df['PRCH_DOC_TYP_CD'].str.slice(0, 4)
    #df['PRCH_DOC_TYP_CD'] = df['PRCH_DOC_TYP_CD'].str.rjust(4, ' ')

    # ITM-CTGRY
    df['ITM_CTGRY'] = 'P'
    df['ITM_CTGRY'] = df['ITM_CTGRY'].str.slice(0, 1)
    df['ITM_CTGRY'] = df['ITM_CTGRY'].str.ljust(1, ' ')

    # BINDING-ON-MRP-IND
    df['BINDING_ON_MRP_IND'] = '2'
    df['BINDING_ON_MRP_IND'] = df['BINDING_ON_MRP_IND'].str.slice(0, 1)
    df['BINDING_ON_MRP_IND'] = df['BINDING_ON_MRP_IND'].str.ljust(1, ' ')

    # CNFRM-CTRL-CD
    df['CNFRM_CTRL_CD'] = '0000'
    df['CNFRM_CTRL_CD'] = df['CNFRM_CTRL_CD'].str.slice(0, 4)
    df['CNFRM_CTRL_CD'] = df['CNFRM_CTRL_CD'].str.ljust(4, ' ')

    # FIRM-ZONE
    df['FIRM_ZONE'] = '000'
    df['FIRM_ZONE'] = df['FIRM_ZONE'].str.slice(0, 3)
    df['FIRM_ZONE'] = df['FIRM_ZONE'].str.ljust(3, ' ')

    # PLANNED-DLVRY
    #df['PLANNED_DLVRY'] = 'PLANNED_DLVRY'
    df['PLANNED_DLVRY'] = df['PLANNED_DLVRY'].astype(int)
    df['PLANNED_DLVRY'] = df['PLANNED_DLVRY'].astype(str)
    df['PLANNED_DLVRY'] = df['PLANNED_DLVRY'].astype(str).str.zfill(3)
    df['PLANNED_DLVRY'] = df['PLANNED_DLVRY'].str.slice(0, 3)
    #df['PLANNED_DLVRY'] = df['PLANNED_DLVRY'].str.ljust(3, ' ')


    # TRADE-OFF-ZN
    df['TRADE_OFF_ZN'] = '000'
    df['TRADE_OFF_ZN'] = df['TRADE_OFF_ZN'].str.slice(0, 3)
    df['TRADE_OFF_ZN'] = df['TRADE_OFF_ZN'].str.ljust(3, ' ')

    # GR-INVC-VERF
    df['GR_INVC_VERF'] = 'Y'
    df['GR_INVC_VERF'] = df['GR_INVC_VERF'].str.slice(0, 1)
    df['GR_INVC_VERF'] = df['GR_INVC_VERF'].str.ljust(1, ' ')

    # CP-NUM
    df['CP_NUM'] = 'WEEK'
    df['CP_NUM'] = df['CP_NUM'].str.slice(0, 4)
    df['CP_NUM'] = df['CP_NUM'].str.ljust(4, ' ')

    # EVAL-RCPT-STLMT
    df['EVAL_RCPT_STLMT'] = 'N'
    df['EVAL_RCPT_STLMT'] = df['EVAL_RCPT_STLMT'].str.slice(0, 1)
    df['EVAL_RCPT_STLMT'] = df['EVAL_RCPT_STLMT'].str.ljust(1, ' ')

    # ASN-INVTY-QTY
    df['ASN_INVTY_QTY'] = '0000000000000'
    df['ASN_INVTY_QTY'] = df['ASN_INVTY_QTY'].str.slice(0, 13)
    df['ASN_INVTY_QTY'] = df['ASN_INVTY_QTY'].str.ljust(13, ' ')

    # INBND-OFFSET 2 mal auf der gleichen Positio
    df['INBND_OFFSET'] = '000000'
    df['INBND_OFFSET'] = df['INBND_OFFSET'].str.slice(0, 6)
    df['INBND_OFFSET'] = df['INBND_OFFSET'].str.ljust(6, ' ')



    # NET-PRICE
    #df['NET_PRICE'] = 'NET_PRICE'
    df['NET_PRICE'] = [f'{a:.6f}' for a in df['NET_PRICE']]
    df['NET_PRICE'] = [ i.replace(".","") for i in df['NET_PRICE'] ]

    #df['NET_PRICE'] = df['NET_PRICE'].astype(float)
    df['NET_PRICE'] = df['NET_PRICE'].astype(str)
    df['NET_PRICE'] = df['NET_PRICE'].str.zfill(18)
    #df['NET_PRICE'] = df['NET_PRICE'].str.slice(0, 18)



    # NET-PRICE-CRNCY
    #df['NET_PRICE_CRNCY'] = 'NET_PRICE_CRNCY'
    df['NET_PRICE_CRNCY'] = df['NET_PRICE_CRNCY'].map({'EUR' :'EUR', 'US-$': 'USD' })
    df['NET_PRICE_CRNCY'] = df['NET_PRICE_CRNCY'].str.slice(0, 5)
    df['NET_PRICE_CRNCY'] = df['NET_PRICE_CRNCY'].str.ljust(5, ' ')

    # NET-PRICE-UNIT
    #df['ORD_PRICE_UNIT'] = 'ORD_PRICE_UNIT'
    #STK im SAP VARIABLEN EA
    df['ORD_PRICE_UNIT'] = df['ORD_PRICE_UNIT'].map({'Stk': 'EA'})
    df['ORD_PRICE_UNIT'] = df['ORD_PRICE_UNIT'].str.slice(0, 3)
    df['ORD_PRICE_UNIT'] = df['ORD_PRICE_UNIT'].str.ljust(3, ' ')

    # UNIT-PER
    #df['UNIT_PER'] = 'UNIT_PER'
    df['UNIT_PER'] = df['UNIT_PER'].astype(int)
    df['UNIT_PER'] = df['UNIT_PER'].astype(str)
    df['UNIT_PER'] = df['UNIT_PER'].str.zfill(5)
    df['UNIT_PER'] = df['UNIT_PER'].str.slice(0, 5)
    #df['UNIT_PER'] = df['UNIT_PER'].str.ljust(5, ' ')

    # GOOD-RCPT-PRC-TM
    df['GOOD_RCPT_PRC_TM'] = '000'
    df['GOOD_RCPT_PRC_TM'] = df['GOOD_RCPT_PRC_TM'].str.slice(0, 3)
    df['GOOD_RCPT_PRC_TM'] = df['GOOD_RCPT_PRC_TM'].str.ljust(3, ' ')

    # STOR-LOC
    df['STOR_LOC'] = '   '
    df['STOR_LOC'] = df['STOR_LOC'].str.slice(0, 4)
    df['STOR_LOC'] = df['STOR_LOC'].str.ljust(4, ' ')

    # INCO-TERMS
    #df['INCO_TERMS'] = 'INCO_TERMS'
    df['INCO_TERMS'] = df['INCO_TERMS'].str.slice(0, 3)
    df['INCO_TERMS'] = df['INCO_TERMS'].str.ljust(3, ' ')

    # POINT-OF-USE
    df['POINT_OF_USE'] = '0000000000'
    df['POINT_OF_USE'] = df['POINT_OF_USE'].str.slice(0, 10)
    df['POINT_OF_USE'] = df['POINT_OF_USE'].str.ljust(10, ' ')

    # UNLTD-IND
    df['UNLTD_IND'] = '0'
    df['UNLTD_IND'] = df['UNLTD_IND'].str.slice(0, 1)
    df['UNLTD_IND'] = df['UNLTD_IND'].str.ljust(1, ' ')

    # OVERDLVRY-TLRNC
    df['OVERDLVRY_TLRNC'] = '100'
    df['OVERDLVRY_TLRNC'] = df['OVERDLVRY_TLRNC'].str.slice(0, 3)
    df['OVERDLVRY_TLRNC'] = df['OVERDLVRY_TLRNC'].str.ljust(3, ' ')

    # UNDERDLVRY-TLRNC
    df['UNDERDLVRY_TLRNC'] = '999'
    df['UNDERDLVRY_TLRNC'] = df['UNDERDLVRY_TLRNC'].str.slice(0, 3)
    df['UNDERDLVRY_TLRNC'] = df['UNDERDLVRY_TLRNC'].str.ljust(3, ' ')

    # SHIP-TO-LOC
    df['SHIP_TO_LOC'] = '0000315483'
    df['SHIP_TO_LOC'] = df['SHIP_TO_LOC'].str.slice(0, 10)
    df['SHIP_TO_LOC'] = df['SHIP_TO_LOC'].str.ljust(10, ' ')

    # VLD-FROM-DT
    #df['VLD_FROM_DT'] = 'VLD_FROM_DT'
    df['VLD_FROM_DT'] = pd.to_datetime(df["VLD_FROM_DT"], format='%Ymd').dt.strftime('%Y%m%d')
    df['VLD_FROM_DT'] = df['VLD_FROM_DT'].str.slice(0, 8)
    df['VLD_FROM_DT'] = df['VLD_FROM_DT'].str.ljust(8, ' ')

    # VLD-TO-DT
    df['VLD_TO_DT'] = '99991231'
    df['VLD_TO_DT'] = df['VLD_TO_DT'].str.slice(0, 8)
    df['VLD_TO_DT'] = df['VLD_TO_DT'].str.ljust(8, ' ')

    # QUOTA-AR
    df['QUOTA_AR'] = '0000000000'
    df['QUOTA_AR'] = df['QUOTA_AR'].str.slice(0, 10)
    df['QUOTA_AR'] = df['QUOTA_AR'].str.ljust(10, ' ')

    # MRP-AREA
    df['MRP_AREA'] = '0000000000'
    df['MRP_AREA'] = df['MRP_AREA'].str.slice(0, 10)
    df['MRP_AREA'] = df['MRP_AREA'].str.ljust(10, ' ')

    # DLVRY-GATE
    df['DLVRY_GATE'] = '0000'
    df['DLVRY_GATE'] = df['DLVRY_GATE'].str.slice(0, 4)
    df['DLVRY_GATE'] = df['DLVRY_GATE'].str.ljust(4, ' ')

    # DLVRY-GATE
    df['DLVRY_GATE'] = '0000'
    df['DLVRY_GATE'] = df['DLVRY_GATE'].str.slice(0, 4)
    df['DLVRY_GATE'] = df['DLVRY_GATE'].str.ljust(4, ' ')

    # DLVRY-DOCK
    df['DLVRY_DOCK'] = '0000'
    df['DLVRY_DOCK'] = df['DLVRY_DOCK'].str.slice(0, 4)
    df['DLVRY_DOCK'] = df['DLVRY_DOCK'].str.ljust(4, ' ')

    # PART-PATH
    df['PART_PATH'] = '00000000000000000000'
    df['PART_PATH'] = df['PART_PATH'].str.slice(0, 20)
    df['PART_PATH'] = df['PART_PATH'].str.ljust(20, ' ')

    # REC-EXT-TS
    #df['REC_EXT_TS'] = 'REC_EXT_TS'
    df.insert(0, 'REC_EXT_TS', pd.datetime.now())
    df['REC_EXT_TS'] = pd.to_datetime(df["REC_EXT_TS"], format='%Y%m%d').dt.strftime('%Y-%m-%d')
    df['REC_EXT_TS'] = df['REC_EXT_TS'].str.ljust(10, ' ')

    # Hardcode Timestamp hours, minutes, seconds wird als String an den Timestamp angehängt
    df['REC_EXT_TS2'] = "-00.00.00.000000"
    df['REC_EXT_TS2'] = df['REC_EXT_TS2'].str.ljust(16, ' ')









    #DataFrame sortieren


    df = df[['PLANT_CD', 'ITEM_CD', 'ORD_FRM_SPLR_CS_CD', 'SCHDL_AGRMT', 'SHP_FRM_SPLR_CS_CD', 'PRCH_DOC_TYP_CD',
               'ITM_CTGRY', 'BINDING_ON_MRP_IND', 'CNFRM_CTRL_CD', 'FIRM_ZONE', 'PLANNED_DLVRY'
        , 'TRADE_OFF_ZN', 'GR_INVC_VERF', 'CP_NUM', 'EVAL_RCPT_STLMT', 'ASN_INVTY_QTY', 'INBND_OFFSET',
               'NET_PRICE', 'NET_PRICE_CRNCY','ORD_PRICE_UNIT', 'UNIT_PER', 'GOOD_RCPT_PRC_TM'
        , 'STOR_LOC', 'INCO_TERMS', 'POINT_OF_USE', 'UNLTD_IND', 'OVERDLVRY_TLRNC', 'UNDERDLVRY_TLRNC',
               'SHIP_TO_LOC', 'VLD_FROM_DT', 'VLD_TO_DT', 'QUOTA_AR', 'MRP_AREA'
        , 'DLVRY_GATE','DLVRY_DOCK','PART_PATH','REC_EXT_TS','REC_EXT_TS2']]




    df = df.dropna(inplace=False)

    #Dataframe zusammenführen für csv datei


    dfx =       df['PLANT_CD'] + df['ITEM_CD'] + df['ORD_FRM_SPLR_CS_CD'] + df['SCHDL_AGRMT']+ df['SHP_FRM_SPLR_CS_CD']+ df['PRCH_DOC_TYP_CD'] + df['ITM_CTGRY'] + df['BINDING_ON_MRP_IND']  + df['CNFRM_CTRL_CD'] + df['FIRM_ZONE'] + \
                df['PLANNED_DLVRY'] + df['TRADE_OFF_ZN'] + df['GR_INVC_VERF']+df['CP_NUM'] + df['EVAL_RCPT_STLMT'] + df['ASN_INVTY_QTY'] + \
                df['INBND_OFFSET']+ df['NET_PRICE'] + df['NET_PRICE_CRNCY'] + df['ORD_PRICE_UNIT']+df['UNIT_PER'] + df['GOOD_RCPT_PRC_TM']+  \
                df['STOR_LOC'] + df['INCO_TERMS'] + df['POINT_OF_USE']+ df['UNLTD_IND']+df['OVERDLVRY_TLRNC'] +df['UNDERDLVRY_TLRNC'] + \
                df['SHIP_TO_LOC'] + df['VLD_FROM_DT'] + df['VLD_TO_DT']+df['QUOTA_AR']+df['MRP_AREA'] + df['DLVRY_GATE'] + df['DLVRY_DOCK']+df['PART_PATH']+ df['REC_EXT_TS']+df['REC_EXT_TS2']


    print('load into txt')


    # load into Excel
    df.to_excel('C:/Users/junxonm/Desktop/FILE1.xlsx', index=False)

    #erstelle Textdatei ohne seperator

    dfx = np.savetxt('C:/Users/junxonm/Desktop/RDJBIAVA.txt', dfx.values, fmt="%s")







except cx_Oracle.DatabaseError as e:
    print(cx_Oracle.DatabaseError)



finally:

    #Close Connection
    #con.close()
    print('Connection close')
    print('start FTP process')

    print(
        '-----------------------------------------------------------------------------------------------------------------------------------------')
    print(
        '-----------------------------------------------------------------------------------------------------------------------------------------')

    import os
    import zosftplib

    dirpath = os.getcwd()  # directory where this Python program is in

    ftp_handler = zosftplib.Zftp("", "", "", timeout=500.0,
                                 sbdataconn="(IBM-037,ISO8859-1)")

    # Documentation of IBM Mainframe FTP parameters: https://www.ibm.com/support/pages/sitelocsite-commands-mvs-ftp
    # Separate multiple parameters by a blank space.
    # LRecl=nnn Specifies the record size in a data set.
    # RECfm=format Specifies the record format of a data set. F=Fixed record length, B=Blocked records
    # BLocksize=nnnn Specifies the block size of a data set.


    # file wird aktuell aus Pycharm gezogen \\Verzeichnis

    ftp_handler.upload_text(dirpath + "\\", "",
                            sitecmd="LRECL=285 RECFM=FB BLKSIZE=27930")

    print('FTP transfer successfuly')
