import pyodbc
import datetime
import os

try:
    tmp = datetime.datetime.now().strftime('%y%m%d')
    tmpf = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    calea = os.path.dirname(__file__)
    text_file = open(str(calea) + '\\log_ind_maintenance.txt', 'a+')
    conn = pyodbc.connect("""
        DRIVER={NetezzaSQL};
        SERVER=bacc-pda2-wall.portsmouth.uk.ibm.com;
        PORT=5480;DATABASE=BACC_PRD_IDM_ACS;
        UID=roy55683;
        PWD=stefan01juneibm;
        DSN=NZSQL
    """)

    print("Connection successful")
    text_file.write ("\nlog industry " + str(tmpf) + "\n" + "Connection successful\n")

    cursor = conn.cursor()

    cursor.execute ("drop table BACC_PRD_IDM_ACS.ACS_DEES0.IND_INACT_URN_" + tmp + "_V1 IF EXISTS;")
    cursor.execute ("drop table BACC_PRD_IDM_ACS.ACS_DEES0.IND_INACT_URN_" + tmp + "_V2 IF EXISTS;")
    cursor.execute ("drop table BACC_PRD_IDM_ACS.ACS_DEES0.IND_INACT_URN_" + tmp + "_V3 IF EXISTS;")
    cursor.execute ("drop table BACC_PRD_IDM_ACS.ACS_DEES0.IND_INACT_URN_" + tmp + "_V4 IF EXISTS;")
    cursor.execute ("drop table BACC_PRD_IDM_ACS.ACS_DEES0.IND_INACT_URN_" + tmp + "_V5 IF EXISTS;")
    cursor.execute ("drop table BACC_PRD_IDM_ACS.ACS_DEES0.IND_INACT_URN_" + tmp + "_V6 IF EXISTS;")
    cursor.execute ("drop table BACC_PRD_IDM_ACS.ACS_DEES0.IND_INACT_URN_" + tmp + "_V7 IF EXISTS;")

    conn.commit ()
    print ("the temporary tables were dropped")
    text_file.write("the temporary tables were dropped\n")

    cursor.execute ("""
        CREATE TABLE BACC_PRD_IDM_ACS.ACS_DEES0.IND_INACT_URN_""" + tmp + """_V1 AS (
        select distinct 
        a.*
        , B.ACTV_URN_IDM_COMP
        from
        BACC_PRD_IDM_ACS.ACS_ZLNK0.COMP_INDUSTRY_XREF a
        inner join BACC_PRD_IDM_IIW.V2INAT2.V_COMP B
        ON A.URN_IDM_COMP = B.URN_IDM_COMP
        WHERE
        A.INACT_FLG = 'N' AND B.INACT_FLG = 'Y'
        );
    """)
    conn.commit ()
    cursor.execute ("select count (*) from BACC_PRD_IDM_ACS.ACS_DEES0.IND_INACT_URN_" + tmp + "_V1")
    rezultat = cursor.fetchall()
    for row in rezultat: print ("Select inactivated urns - " + str(row[0]) + " records were affected")
    for row in rezultat: text_file.write("Select inactivated urns - " + str(row[0]) + " records were affected\n")


    cursor.execute ("""
        select count (*) from BACC_PRD_IDM_ACS.ACS_ZLNK0.COMP_INDUSTRY_XREF 
        where URN_IDM_COMP IN (SELECT URN_IDM_COMP FROM BACC_PRD_IDM_ACS.ACS_DEES0.IND_INACT_URN_""" + tmp + """_V1)
        AND INACT_FLG = 'N'
        """)
    rezultat = cursor.fetchall()
    for row in rezultat: print (str(row[0]) + " linkages will be updated")
    for row in rezultat: text_file.write (str(row[0]) + " linkages will be updated\n")

    cursor.execute ("""
        update 
        BACC_PRD_IDM_ACS.ACS_ZLNK0.COMP_INDUSTRY_XREF
        set
        inact_flg = 'Y'
        , UPDT_TS = CURRENT_TIMESTAMP
        , comment = case when comment is null then 'Python Maintenance Script' else comment end
        WHERE
        URN_IDM_COMP IN (SELECT URN_IDM_COMP FROM BACC_PRD_IDM_ACS.ACS_DEES0.IND_INACT_URN_""" + tmp + """_V1)
        AND INACT_FLG = 'N';
    """)
    conn.commit ()
    print ("Inactivate linkages with incativated urns - Done")
    text_file.write ("Inactivate linkages with incativated urns - Done\n")

    cursor.execute ("""
        create table BACC_PRD_IDM_ACS.ACS_DEES0.IND_INACT_URN_""" + tmp + """_V2 as (
        SELECT DISTINCT 
        A.ACTV_URN_IDM_COMP AS URN_IDM_COMP
        , a.MAIN_IND_CD
            , a.MAIN_SUB_IND_CD
            , a.MAIN_SIC
            , a.INACT_FLG
            , a.CREATE_TS
            , a.UPDT_TS
            , a.ID
			, a.comment

        FROM
        BACC_PRD_IDM_ACS.ACS_DEES0.IND_INACT_URN_""" + tmp + """_V1 a
        );
    """)
    conn.commit ()

    cursor.execute ("select count (*) from BACC_PRD_IDM_ACS.ACS_DEES0.IND_INACT_URN_" + tmp + "_V2")
    rezultat = cursor.fetchall()
    for row in rezultat: print ("Replace inactivated urns with the active urns - " + str(row[0]) + " records were affetced")
    for row in rezultat: text_file.write ("Replace inactivated urns with the active urns - " + str(row[0]) + " records were affetced\n")

    print ("Deduplication start")
    text_file.write("Deduplication start\n")


    cursor.execute ("""
        create table BACC_PRD_IDM_ACS.ACS_DEES0.IND_INACT_URN_""" + tmp + """_V3 as (
        select a.*, b.confidence_lvl
        from
        BACC_PRD_IDM_ACS.ACS_DEES0.IND_INACT_URN_""" + tmp + """_V2 a 
        left join BACC_PRD_IDM_ACS.ACS_zlnk0.COMP_INDUSTRY_id_XREF b
        on substring (a.id, 12, 7) = b.id_source_cd
        );
    """)

    cursor.execute ("""
        create table BACC_PRD_IDM_ACS.ACS_DEES0.IND_INACT_URN_""" + tmp + """_V4 as (
        select urn_idm_comp, max (confidence_lvl) as max_confidence_lvl
        from
        BACC_PRD_IDM_ACS.ACS_DEES0.IND_INACT_URN_""" + tmp + """_V3
        group by urn_idm_comp
        );
    """)

    cursor.execute ("""
        create table BACC_PRD_IDM_ACS.ACS_DEES0.IND_INACT_URN_""" + tmp + """_V5 as (
        select distinct
        a.*
        from
        BACC_PRD_IDM_ACS.ACS_DEES0.IND_INACT_URN_""" + tmp + """_V3 a
        inner join BACC_PRD_IDM_ACS.ACS_DEES0.IND_INACT_URN_""" + tmp + """_V4 b
        on a.urn_idm_comp = b.urn_idm_comp and a.confidence_lvl = b.max_confidence_lvl
        );
    """)

    cursor.execute ("""
        create table BACC_PRD_IDM_ACS.ACS_DEES0.IND_INACT_URN_""" + tmp + """_V6 as (
        select urn_idm_comp, min (id) as min_id
        from
        BACC_PRD_IDM_ACS.ACS_DEES0.IND_INACT_URN_""" + tmp + """_V5 
        group by urn_idm_comp
        );
    """)

    cursor.execute ("""
        create table BACC_PRD_IDM_ACS.ACS_DEES0.IND_INACT_URN_""" + tmp + """_V7 as (
        select distinct a.*
        from
        BACC_PRD_IDM_ACS.ACS_DEES0.IND_INACT_URN_""" + tmp + """_V5 a
        inner join BACC_PRD_IDM_ACS.ACS_DEES0.IND_INACT_URN_""" + tmp + """_V6 b
        on a.id = b.min_id
        );
    """)

    conn.commit()
    cursor.execute ("select count (*) from BACC_PRD_IDM_ACS.ACS_DEES0.IND_INACT_URN_" + tmp + "_V7")
    rezultat = cursor.fetchall()
    for row in rezultat: print ("Deduplication Done - " + str(row[0]) + " records remain")
    for row in rezultat: text_file.write("Deduplication Done - " + str(row[0]) + " records remain\n")

    cursor.execute ("""
        select count(*) from 
        (SELECT DISTINCT
        A.URN_IDM_COMP
            , A.MAIN_IND_CD
            , A.MAIN_SUB_IND_CD
            , A.MAIN_SIC
            , cast ('N' AS VARCHAR (1)) as INACT_FLG
            , A.CREATE_TS
            , CAST (CURRENT_TIMESTAMP AS TIMESTAMP) AS UPDT_TS
            , A.ID
        FROM
        BACC_PRD_IDM_ACS.ACS_DEES0.IND_INACT_URN_""" + tmp + """_V7 A
        LEFT join BACC_PRD_IDM_ACS.ACS_ZLNK0.COMP_INDUSTRY_XREF B
        ON A.URN_IDM_COMP = B.URN_IDM_COMP and b.inact_flg = 'N'
        WHERE b.urn_idm_comp IS NULL) as x;
    """)
    rezultat = cursor.fetchall ()
    for row in rezultat: print (str(row[0]) + " new linkages will be loaded")
    for row in rezultat: text_file.write(str(row[0]) + " new linkages will be loaded\n")

    cursor.execute ("""
        INSERT INTO
        BACC_PRD_IDM_ACS.ACS_ZLNK0.COMP_INDUSTRY_XREF
        (
        URN_IDM_COMP
            , MAIN_IND_CD
            , MAIN_SUB_IND_CD
            , MAIN_SIC
            , INACT_FLG
            , CREATE_TS
            , UPDT_TS
            , ID
            , comment
        )
        SELECT DISTINCT
        A.URN_IDM_COMP
            , A.MAIN_IND_CD
            , A.MAIN_SUB_IND_CD
            , A.MAIN_SIC
            , cast ('N' AS VARCHAR (1)) as INACT_FLG
            , A.CREATE_TS
            , CAST (CURRENT_TIMESTAMP AS TIMESTAMP) AS UPDT_TS
            , A.ID
            , case when a.comment is null then 'Python Maintenance Script' else a.comment end as comment
        FROM
        BACC_PRD_IDM_ACS.ACS_DEES0.IND_INACT_URN_""" + tmp + """_V7 A
        LEFT join BACC_PRD_IDM_ACS.ACS_ZLNK0.COMP_INDUSTRY_XREF B
        ON A.URN_IDM_COMP = B.URN_IDM_COMP and b.inact_flg = 'N'
        WHERE b.urn_idm_comp IS NULL;
    """)
    conn.commit ()
    print ("Load complete")
    text_file.write("Load complete\n")

    cursor.execute ("drop table BACC_PRD_IDM_ACS.ACS_DEES0.IND_INACT_URN_" + tmp + "_V1 IF EXISTS;")
    cursor.execute ("drop table BACC_PRD_IDM_ACS.ACS_DEES0.IND_INACT_URN_" + tmp + "_V2 IF EXISTS;")
    cursor.execute ("drop table BACC_PRD_IDM_ACS.ACS_DEES0.IND_INACT_URN_" + tmp + "_V3 IF EXISTS;")
    cursor.execute ("drop table BACC_PRD_IDM_ACS.ACS_DEES0.IND_INACT_URN_" + tmp + "_V4 IF EXISTS;")
    cursor.execute ("drop table BACC_PRD_IDM_ACS.ACS_DEES0.IND_INACT_URN_" + tmp + "_V5 IF EXISTS;")
    cursor.execute ("drop table BACC_PRD_IDM_ACS.ACS_DEES0.IND_INACT_URN_" + tmp + "_V6 IF EXISTS;")
    cursor.execute ("drop table BACC_PRD_IDM_ACS.ACS_DEES0.IND_INACT_URN_" + tmp + "_V7 IF EXISTS;")

    conn.commit ()
    print ("The final temporary tables were dropped")
    text_file.write("The final temporary tables were dropped\n")
    
finally:
    cursor.close()
    conn.close()
    print ("The connection is closed")
    text_file.write("The connection is closed\n")
    text_file.close()