import pyodbc
import datetime
import os
#modif
try:
    tmp = datetime.datetime.now().strftime('%y%m%d')
    tmpf = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    calea = os.path.dirname(__file__)
	
    text_file = open(str(calea) + '\\log_dnb_maintenance.txt', 'a')
    text_file.write ("\nlog dnb " + str(tmpf) + "\n")

    conn = pyodbc.connect("""
        DRIVER={NetezzaSQL};
        SERVER=bacc-pda2-wall.portsmouth.uk.ibm.com;
        PORT=5480;DATABASE=BACC_PRD_IDM_ACS;
        UID=roy55683;
        PWD=stefan01juneibm;
        DSN=NZSQL
    """)

    print("Connection successful")
    text_file.write ("Connection successful" + str(tmpf) + " \n")

    cursor = conn.cursor()
    cursor.execute ("drop table  BACC_PRD_IDM_ACS.ACS_DEES0.DNB_INACT_URN_" + tmp + "_V1 IF EXISTS;")
    cursor.execute ("drop table  BACC_PRD_IDM_ACS.ACS_DEES0.DNB_INACT_URN_" + tmp + "_V2 IF EXISTS;")
    cursor.execute ("drop table  BACC_PRD_IDM_ACS.ACS_DEES0.DNB_INACT_URN_" + tmp + "_V3 IF EXISTS;")
    cursor.execute ("drop table  BACC_PRD_IDM_ACS.ACS_DEES0.DNB_INACT_URN_" + tmp + "_V3_tmp IF EXISTS;")
    cursor.execute ("drop table  BACC_PRD_IDM_ACS.ACS_DEES0.DNB_INACT_URN_" + tmp + "_V4 IF EXISTS;")
    cursor.execute ("drop table  BACC_PRD_IDM_ACS.ACS_DEES0.DNB_INACT_URN_" + tmp + "_V4_tmp IF EXISTS;")
    cursor.execute ("drop table  BACC_PRD_IDM_ACS.ACS_DEES0.DNB_INACT_URN_" + tmp + "_V5 IF EXISTS;")

    conn.commit ()
    print ("the temporary tables were dropped")
    text_file.write ("Temporary INITIAL tables were dropped \n")

    cursor.execute ("""
            CREATE TABLE BACC_PRD_IDM_ACS.ACS_DEES0.DNB_INACT_URN_""" + tmp + """_V1 AS (
            select distinct 
            a.*
            , B.ACTV_URN_IDM_COMP
            from
            BACC_PRD_IDM_ACS.ACS_ZLNK0.COMP_DUNS_XREF a
            inner join BACC_PRD_IDM_IIW.V2INAT2.V_COMP B
            ON A.URN_IDM_COMP = B.URN_IDM_COMP
            WHERE
            A.INACT_FLG = 'N' AND B.INACT_FLG = 'Y');
     """)
    conn.commit ()

    cursor.execute ("select count (*) from BACC_PRD_IDM_ACS.ACS_DEES0.DNB_INACT_URN_" + tmp + "_V1")
    resultat = cursor.fetchall()

    for row in resultat:print ("select inactivated urns - " + str(row[0]) + " records were affected")
    for row in resultat:text_file.write ("select inactivated urns - " + str(row[0]) + " records were affected \n")

    cursor.execute ("""
        select count (*) from BACC_PRD_IDM_ACS.ACS_ZLNK0.COMP_DUNS_XREF
        WHERE
         URN_IDM_COMP IN (SELECT URN_IDM_COMP FROM BACC_PRD_IDM_ACS.ACS_DEES0.DNB_INACT_URN_""" + tmp + """_V1)
         AND INACT_FLG = 'N';
     """)
    resultat = cursor.fetchall()

    for row in resultat:print (str(row[0]) + " rows will be inactivated")
    for row in resultat:text_file.write (str(row[0]) + " rows will be inactivated \n")

    cursor.execute ("""
        update 
        BACC_PRD_IDM_ACS.ACS_ZLNK0.COMP_DUNS_XREF
        set
        inact_flg = 'Y'
        , UPDT_TS = CURRENT_TIMESTAMP
        , comment = case when comment is null then 'Python Maintenance Script' else comment end
        WHERE
        URN_IDM_COMP IN (SELECT URN_IDM_COMP FROM BACC_PRD_IDM_ACS.ACS_DEES0.DNB_INACT_URN_""" + tmp + """_V1)
        AND INACT_FLG = 'N';
    """)
    conn.commit ()
    print ("incativate linkages - done")
    text_file.write ("incativate linkages - done \n")

    cursor.execute ("""
        create table BACC_PRD_IDM_ACS.ACS_DEES0.DNB_INACT_URN_""" + tmp + """_V2 as (
        SELECT DISTINCT 
        A.ACTV_URN_IDM_COMP AS URN_IDM_COMP
            , A.LE_DUNS_NUM
            , A.DU_DUNS_NUM
            , A.HQ_DUNS_NUM
            , A.GU_DUNS_NUM
            , A.INACT_FLG
            , A.CREATE_TS
            , A.UPDT_TS
            , A.ID
            , A.FLG_600K
			, a.comment
        FROM
        BACC_PRD_IDM_ACS.ACS_DEES0.DNB_INACT_URN_""" + tmp + """_V1 a
    );
    """)
    conn.commit ()
    cursor.execute ("select count (*) from BACC_PRD_IDM_ACS.ACS_DEES0.DNB_INACT_URN_" + tmp + "_V2")
    resultat = cursor.fetchall()

    for row in resultat:print ("Replace inactivated urns with the active urns - " + str(row[0]) + " rows will be replaced")
    for row in resultat:text_file.write ("Replace inactivated urns with the active urns - " + str(row[0]) + " rows will be replaced \n")

    cursor.execute ("""
        create table BACC_PRD_IDM_ACS.ACS_DEES0.DNB_INACT_URN_""" + tmp + """_V3 as (
        select distinct a.*, cast (0 as int) as score
        from
        BACC_PRD_IDM_ACS.ACS_DEES0.DNB_INACT_URN_""" + tmp + """_V2 a
        inner join BACC_PRD_IDM_IIW.V2INAT2.V_DUNS_WB_ACTV B
        ON A.LE_DUNS_NUM = B.DUNS_NUM
        where
        b.duns_stat_cd <> '2' 
        );
    """)
    conn.commit ()

    cursor.execute ("select count (*) from BACC_PRD_IDM_ACS.ACS_DEES0.DNB_INACT_URN_" + tmp + "_V3")
    resultat = cursor.fetchall()
    
    for row in resultat:print ("extract only dunses that are active and are not branches - " + str(row[0]) + " rows were selected")
    for row in resultat:text_file.write ("extract only dunses that are active and are not branches - " + str(row[0]) + " rows were selected \n")


    cursor.execute ("update BACC_PRD_IDM_ACS.ACS_DEES0.DNB_INACT_URN_" + tmp + "_V3 set score = 0;")
    cursor.execute ("update BACC_PRD_IDM_ACS.ACS_DEES0.DNB_INACT_URN_" + tmp + "_V3 set score = score + 1 WHERE HQ_DUNS_NUM IS NOT NULL;")
    cursor.execute ("update BACC_PRD_IDM_ACS.ACS_DEES0.DNB_INACT_URN_" + tmp + "_V3 set score = score + 1 WHERE DU_DUNS_NUM IS NOT NULL;")
    cursor.execute ("update BACC_PRD_IDM_ACS.ACS_DEES0.DNB_INACT_URN_" + tmp + "_V3 set score = score + 1 WHERE GU_DUNS_NUM IS NOT NULL;")
    conn.commit ()

    print ("create deduplication score")
    text_file.write ("create deduplication score \n")

    cursor.execute ("""
        CREATE TABLE BACC_PRD_IDM_ACS.ACS_DEES0.DNB_INACT_URN_""" + tmp + """_V3_tmp AS (
        SELECT MAX(SCORE) AS SCORE, URN_IDM_COMP
        FROM
        BACC_PRD_IDM_ACS.ACS_DEES0.DNB_INACT_URN_""" + tmp + """_V3
        GROUP BY URN_IDM_COMP
        );
    """)
    cursor.execute ("""
        create table BACC_PRD_IDM_ACS.ACS_DEES0.DNB_INACT_URN_""" + tmp + """_V4 as (
        select distinct 
        a.*
        from
        BACC_PRD_IDM_ACS.ACS_DEES0.DNB_INACT_URN_""" + tmp + """_V3 a
        inner join BACC_PRD_IDM_ACS.ACS_DEES0.DNB_INACT_URN_""" + tmp + """_V3_tmp b
        on a.urn_idm_comp = b.urn_idm_comp and a.score = b.score
        );
    """)
    cursor.execute ("""
        create table BACC_PRD_IDM_ACS.ACS_DEES0.DNB_INACT_URN_""" + tmp + """_V4_tmp as (
        select
        min(id) as id, urn_idm_comp
        from
        BACC_PRD_IDM_ACS.ACS_DEES0.DNB_INACT_URN_""" + tmp + """_V4
        group by urn_idm_comp
        );
    """)
    cursor.execute ("""
        create table BACC_PRD_IDM_ACS.ACS_DEES0.DNB_INACT_URN_""" + tmp + """_V5 as (
        select distinct 
        a.*
        from
        BACC_PRD_IDM_ACS.ACS_DEES0.DNB_INACT_URN_""" + tmp + """_V4 a
        inner join BACC_PRD_IDM_ACS.ACS_DEES0.DNB_INACT_URN_""" + tmp + """_V4_tmp b
        on a.id = b.id
        );
    """)

    conn.commit ()

    cursor.execute ("select count (*) from BACC_PRD_IDM_ACS.ACS_DEES0.DNB_INACT_URN_" + tmp + "_V5")
    resultat = cursor.fetchall()

    for row in resultat:print ("urn deduplication done - " + str(row[0]) + " rows were affected")
    for row in resultat:text_file.write ("urn deduplication done - " + str(row[0]) + " rows were affected \n")
        
    cursor.execute ("""
    SELECT COUNT (*) FROM
    (select distinct 
        a.URN_IDM_COMP
            , a.LE_DUNS_NUM
            , a.DU_DUNS_NUM
            , a.HQ_DUNS_NUM
            , a.GU_DUNS_NUM
            , cast ('N' AS VARCHAR (1)) as INACT_FLG
            , A.CREATE_TS
            , CAST (CURRENT_TIMESTAMP AS TIMESTAMP) AS UPDT_TS
            , A.ID
            , A.FLG_600K
        FROM
        BACC_PRD_IDM_ACS.ACS_DEES0.DNB_INACT_URN_""" + tmp + """_V5 a
        left join BACC_PRD_IDM_ACS.ACS_ZLNK0.COMP_DUNS_XREF b
        on a.urn_idm_comp = b.urn_idm_comp and b.inact_flg = 'N'
        WHERE
        b.urn_idm_comp IS NULL
        ) AS X;
    """)
    resultat = cursor.fetchall()
    for row in resultat:print ("will be inserted into the final table " + str(row[0]) + " records")
    for row in resultat:text_file.write ("will be inserted into the final table " + str(row[0]) + " records \n")
        
    cursor.execute ("""
        insert into
        BACC_PRD_IDM_ACS.ACS_ZLNK0.COMP_DUNS_XREF
        (
        URN_IDM_COMP
            , LE_DUNS_NUM
            , DU_DUNS_NUM
            , HQ_DUNS_NUM
            , GU_DUNS_NUM
            , INACT_FLG
            , CREATE_TS
            , UPDT_TS
            , ID
            , FLG_600K
            , comment
        )
        select distinct 
        a.URN_IDM_COMP
            , a.LE_DUNS_NUM
            , a.DU_DUNS_NUM
            , a.HQ_DUNS_NUM
            , a.GU_DUNS_NUM
            , cast ('N' AS VARCHAR (1)) as INACT_FLG
            , A.CREATE_TS
            , CAST (CURRENT_TIMESTAMP AS TIMESTAMP) AS UPDT_TS
            , A.ID
            , A.FLG_600K
            , case when a.comment is null then 'Python Maintenance Script' else a.comment end as comment
        FROM
        BACC_PRD_IDM_ACS.ACS_DEES0.DNB_INACT_URN_""" + tmp + """_V5 a
        left join BACC_PRD_IDM_ACS.ACS_ZLNK0.COMP_DUNS_XREF b
        on a.urn_idm_comp = b.urn_idm_comp and b.inact_flg = 'N'
        WHERE
        b.urn_idm_comp IS NULL;
    """)

    conn.commit ()
    print ("insert new linkages into the main table - finshed")
    text_file.write ("insert new linkages into the main table - finshed \n")

    cursor.execute ("""
        select count (*) from 
        BACC_PRD_IDM_ACS.ACS_ZLNK0.COMP_DUNS_XREF
        where
        le_duns_num in (select duns_num from BACC_PRD_IDM_IIW.V2INAT2.V_DUNS_WB_ACTV where duns_stat_cd = '2')
        and inact_flg = 'N';
    """)
    resultat = cursor.fetchall ()
    for row in resultat:print ("inactivate linkages that have branches or were inactivated in DNB - " +str(row[0]) + " rows will be affected")
    for row in resultat:text_file.write ("inactivate linkages that have branches or were inactivated in DNB - " +str(row[0]) + " rows will be affected \n")
    
    cursor.execute ("""
        update 
        BACC_PRD_IDM_ACS.ACS_ZLNK0.COMP_DUNS_XREF
        set
        inact_flg = 'Y'
        , UPDT_TS = CURRENT_TIMESTAMP
        , comment = case when comment is null then 'Python Maintenance Script' else comment end
        WHERE
        le_duns_num in (select duns_num from BACC_PRD_IDM_IIW.V2INAT2.V_DUNS_WB_ACTV where  duns_stat_cd = '2')
        and inact_flg = 'N';
    """)    

    conn.commit ()

    print ("inactivate linkages that have branches or were inactivated in DNB - done")
    text_file.write ("inactivate linkages that have branches or were inactivated in DNB - done \n")

    cursor.execute ("drop table  BACC_PRD_IDM_ACS.ACS_DEES0.DNB_INACT_URN_" + tmp + "_V1 IF EXISTS;")
    cursor.execute ("drop table  BACC_PRD_IDM_ACS.ACS_DEES0.DNB_INACT_URN_" + tmp + "_V2 IF EXISTS;")
    cursor.execute ("drop table  BACC_PRD_IDM_ACS.ACS_DEES0.DNB_INACT_URN_" + tmp + "_V3 IF EXISTS;")
    cursor.execute ("drop table  BACC_PRD_IDM_ACS.ACS_DEES0.DNB_INACT_URN_" + tmp + "_V3_tmp IF EXISTS;")
    cursor.execute ("drop table  BACC_PRD_IDM_ACS.ACS_DEES0.DNB_INACT_URN_" + tmp + "_V4 IF EXISTS;")
    cursor.execute ("drop table  BACC_PRD_IDM_ACS.ACS_DEES0.DNB_INACT_URN_" + tmp + "_V4_tmp IF EXISTS;")
    cursor.execute ("drop table  BACC_PRD_IDM_ACS.ACS_DEES0.DNB_INACT_URN_" + tmp + "_V5 IF EXISTS;")

    conn.commit ()
    print ("the temporary tables were dropped")
    text_file.write ("the temporary tables were dropped \n")

finally:
    cursor.close()
    conn.close()
    print ("The connection is closed")
    text_file.write ("The connection is closed \n")
    text_file.close()