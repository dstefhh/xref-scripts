import pyodbc
import datetime
import os

try:
    tmp = datetime.datetime.now().strftime('%y%m%d')
    tmpf = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    calea = os.path.dirname(__file__)

    text_file = open(str(calea) + '\\log_rdc_maintenance.txt', 'a+')
    text_file.write ("\nlog rdc " + str(tmpf) + "\n")
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

    cursor.execute ("DROP TABLE BACC_PRD_IDM_ACS.ACS_DEES0.COMP_RDC_XREF_INACT_" + tmp + "_URN IF EXISTS;")
    cursor.execute ("DROP TABLE BACC_PRD_IDM_ACS.ACS_DEES0.COMP_RDC_XREF_INACT_" + tmp + "_URN_V2 IF EXISTS;")
    cursor.execute ("DROP TABLE BACC_PRD_IDM_ACS.ACS_DEES0.COMP_RDC_XREF_INACT_" + tmp + "_URN_V2_max IF EXISTS;")
    cursor.execute ("DROP TABLE BACC_PRD_IDM_ACS.ACS_DEES0.COMP_RDC_XREF_INACT_" + tmp + "_URN_V3 IF EXISTS;")
    cursor.execute ("DROP TABLE BACC_PRD_IDM_ACS.ACS_DEES0.COMP_RDC_XREF_INACT_" + tmp + "_URN_V4 IF EXISTS;")
    cursor.execute ("DROP TABLE BACC_PRD_IDM_ACS.ACS_DEES0.COMP_RDC_XREF_INACT_" + tmp + "_URN_V5 IF EXISTS;")

    conn.commit ()

    cursor.execute ("DROP TABLE BACC_PRD_IDM_ACS.ACS_DEES0.RDC_DIF_custnum_" + tmp + "_V1 IF EXISTS;")
    cursor.execute ("DROP TABLE BACC_PRD_IDM_ACS.ACS_DEES0.COMP_RDC_XREF_INACT_" + tmp + "_SAP_V1 IF EXISTS;")
    cursor.execute ("DROP TABLE BACC_PRD_IDM_ACS.ACS_DEES0.COMP_RDC_XREF_INACT_" + tmp + "_SAP_V2 IF EXISTS;")
    cursor.execute ("DROP TABLE BACC_PRD_IDM_ACS.ACS_DEES0.RDC_DIF_custnum_" + tmp + "_V2 IF EXISTS;")
    cursor.execute ("DROP TABLE BACC_PRD_IDM_ACS.ACS_DEES0.COMP_RDC_XREF_INACT_" + tmp + "_SAP_V3 IF EXISTS;")
    cursor.execute ("DROP TABLE BACC_PRD_IDM_ACS.ACS_DEES0.RDC_DIF_custnum_" + tmp + "_V1 IF EXISTS;")
    cursor.execute ("DROP TABLE BACC_PRD_IDM_ACS.ACS_DEES0.COMP_RDC_XREF_INACT_" + tmp + "_SAP_V1 IF EXISTS;")
    cursor.execute ("DROP TABLE BACC_PRD_IDM_ACS.ACS_DEES0.COMP_RDC_XREF_INACT_" + tmp + "_SAP_V2 IF EXISTS;")
    cursor.execute ("DROP TABLE BACC_PRD_IDM_ACS.ACS_DEES0.RDC_DIF_custnum_" + tmp + "_V2 IF EXISTS;")
    cursor.execute ("DROP TABLE BACC_PRD_IDM_ACS.ACS_DEES0.COMP_RDC_XREF_INACT_" + tmp + "_SAP_V3 IF EXISTS;")

    conn.commit ()

    print ("Temporary INITIAL tables were dropped")
    text_file.write ("Temporary INITIAL tables were dropped \n")

    cursor.execute ("""
        CREATE TABLE BACC_PRD_IDM_ACS.ACS_DEES0.COMP_RDC_XREF_INACT_""" + tmp + """_URN AS (
        select distinct a.*, B.ACTV_URN_IDM_COMP
        from
        BACC_PRD_IDM_ACS.ACS_ZLNK0.COMP_RDC_XREF A
        INNER JOIN BACC_PRD_IDM_IIW.V2INAT2.V_COMP B
        ON A.URN_IDM_COMP = B.URN_IDM_COMP
        WHERE A.INACT_FLG = 'N' AND B.INACT_FLG = 'Y'
        );
    """)
    conn.commit ()

    cursor.execute ("select count (*) as cnt from BACC_PRD_IDM_ACS.ACS_DEES0.COMP_RDC_XREF_INACT_" + tmp + "_URN")
    rezultat = cursor.fetchall ()

    for row in rezultat: print ("Inactivated urns table was created - " + str(row[0]) + "  records were inserted")
    for row in rezultat: text_file.write ("Inactivated urns table was created - " + str(row[0]) + "  records were inserted \n")

    cursor.execute ("""
        select count (*) from
        BACC_PRD_IDM_ACS.ACS_ZLNK0.COMP_RDC_XREF
        WHERE SAP_CUST_NUM IN (SELECT SAP_CUST_NUM FROM BACC_PRD_IDM_ACS.ACS_DEES0.COMP_RDC_XREF_INACT_""" + tmp + """_URN)
        AND INACT_FLG = 'N';    
    """)
    rezultat = cursor.fetchall ()

    for row in rezultat: print (str(row[0]) + "  records will be inactivated in the main table")
    for row in rezultat: text_file.write (str(row[0]) + "  records will be inactivated in the main table \n")

    cursor.execute ("""
        UPDATE 
        BACC_PRD_IDM_ACS.ACS_ZLNK0.COMP_RDC_XREF
        SET
        INACT_FLG = 'Y'
        , UPDT_TS = CURRENT_TIMESTAMP
        , comment = case when comment is null then 'Python Maintenance Script' else comment end
        WHERE SAP_CUST_NUM IN (SELECT SAP_CUST_NUM FROM BACC_PRD_IDM_ACS.ACS_DEES0.COMP_RDC_XREF_INACT_""" + tmp + """_URN)
        AND INACT_FLG = 'N';
    """)

    conn.commit ()

    print ("Inactivate the linkages that have inactive urns - in the main table - Done")
    text_file.write ("Inactivate the linkages that have inactive urns - in the main table - Done \n")

    cursor.execute ("""
        select count (*) from
        BACC_PRD_IDM_ACS.ACS_ZLNK0.CR_DEE_COMP_RDC_XREF_HIST_022118
        WHERE SAP_CUST_NUM IN (SELECT SAP_CUST_NUM FROM BACC_PRD_IDM_ACS.ACS_DEES0.COMP_RDC_XREF_INACT_""" + tmp + """_URN)
        AND INACT_FLG = 'N';
    """)
    rezultat = cursor.fetchall ()
    for row in rezultat: print (str(row[0]) + "  records will be inactivated in the history table")
    for row in rezultat: text_file.write (str(row[0]) + "  records will be inactivated in the history table \n")

    cursor.execute ("""
        UPDATE 
        BACC_PRD_IDM_ACS.ACS_ZLNK0.CR_DEE_COMP_RDC_XREF_HIST_022118
        SET
        INACT_FLG = 'Y'
        , UPDT_TS = CURRENT_TIMESTAMP
        , comment = case when comment is null then 'Python Maintenance Script' else comment end
        WHERE SAP_CUST_NUM IN (SELECT SAP_CUST_NUM FROM BACC_PRD_IDM_ACS.ACS_DEES0.COMP_RDC_XREF_INACT_""" + tmp + """_URN)
        AND INACT_FLG = 'N';
    """)

    conn.commit ()
    print ("Inactivate the linkages that have inactive urns - in the history table - Done")
    text_file.write ("Inactivate the linkages that have inactive urns - in the history table - Done \n")

    cursor.execute ("""
        CREATE TABLE BACC_PRD_IDM_ACS.ACS_DEES0.COMP_RDC_XREF_INACT_""" + tmp + """_URN_V2 AS (
        SELECT DISTINCT ACTV_URN_IDM_COMP AS URN_IDM_COMP
            , LE_DUNS_NUM
            , SAP_CUST_NUM
            , CUST_NUM
            , RDC_CTRY_NUM
            , CTRY
            , CREATE_TS
            , cast (current_timestamp as timestamp) as UPDT_TS
            , ID
			, comment
        FROM BACC_PRD_IDM_ACS.ACS_DEES0.COMP_RDC_XREF_INACT_""" + tmp + """_URN
        WHERE
        ACTV_URN_IDM_COMP IS NOT NULL);
    """)

    conn.commit ()
    cursor.execute ("select count (*) from BACC_PRD_IDM_ACS.ACS_DEES0.COMP_RDC_XREF_INACT_" + tmp + "_URN_V2")
    rezultat = cursor.fetchall ()

    for row in rezultat: print ("The table with active urns, corresponding to inactivated urns was created - " + str(row[0]) + " distinct records were inserted")
    for row in rezultat: text_file.write ("The table with active urns, corresponding to inactivated urns was created - " + str(row[0]) + " distinct records were inserted \n")

    print ("Start Updating the ord num using the history table")

    cursor.execute ("""
        create table BACC_PRD_IDM_ACS.ACS_DEES0.COMP_RDC_XREF_INACT_""" + tmp + """_URN_V2_max as (
        select a.urn_idm_comp, max(a.ord_num) as max_ord_num
        from
        BACC_PRD_IDM_ACS.ACS_ZLNK0.CR_DEE_COMP_RDC_XREF_HIST_022118 a 
        inner join BACC_PRD_IDM_ACS.ACS_DEES0.COMP_RDC_XREF_INACT_""" + tmp + """_URN_V2 b
        on a.urn_idm_comp = b.urn_idm_comp
        group by a.urn_idm_comp
        );
    """)

    cursor.execute ("""
        create table BACC_PRD_IDM_ACS.ACS_DEES0.COMP_RDC_XREF_INACT_""" + tmp + """_URN_V3 as (
        select distinct a.*, b.max_ord_num
        from
        BACC_PRD_IDM_ACS.ACS_DEES0.COMP_RDC_XREF_INACT_""" + tmp + """_URN_V2 a
        left join BACC_PRD_IDM_ACS.ACS_DEES0.COMP_RDC_XREF_INACT_""" + tmp + """_URN_V2_max b
        on a.urn_idm_comp = b.urn_idm_comp
        );
    """)

    cursor.execute ("""
        create table BACC_PRD_IDM_ACS.ACS_DEES0.COMP_RDC_XREF_INACT_""" + tmp + """_URN_V4 as (
        select
        a.*
        , cast (row_number () over (partition by a.urn_idm_comp order by a.urn_idm_comp asc) as int) as ord_num
        from
        BACC_PRD_IDM_ACS.ACS_DEES0.COMP_RDC_XREF_INACT_""" + tmp + """_URN_V3 a
        ) distribute on (sap_cust_num);
    """)

    cursor.execute ("""
        update 
        BACC_PRD_IDM_ACS.ACS_DEES0.COMP_RDC_XREF_INACT_""" + tmp + """_URN_V4
        set
        ord_num = ord_num + max_ord_num
        where
        max_ord_num is not null;
    """)

    conn.commit ()

    print ("Update Ord Num - Done")
    text_file.write ("Update Ord Num - Done \n")

    cursor.execute ("""
        create table BACC_PRD_IDM_ACS.ACS_DEES0.COMP_RDC_XREF_INACT_""" + tmp + """_URN_V5 as (
        select distinct
        a.urn_idm_comp
        , a.le_duns_num
        , a.sap_cust_num
        , a.cust_num
        , a.rdc_ctry_num
        , a.ctry
        , a.ord_num
        , cast ('N' AS VARCHAR (1)) as INACT_FLG
        , a.CREATE_TS
        , cast (current_timestamp as timestamp) as updt_ts
        , a.id
        , case when a.comment is null then 'Python Maintenance Script' else a.comment end as comment
        from
        BACC_PRD_IDM_ACS.ACS_DEES0.COMP_RDC_XREF_INACT_""" + tmp + """_URN_V4 a
        );
    """)

    conn.commit ()

    cursor.execute ("select count (*) from BACC_PRD_IDM_ACS.ACS_DEES0.COMP_RDC_XREF_INACT_" + tmp + "_URN_V5")
    rezultat = cursor.fetchall ()

    for row in rezultat: print ("Recreate correct format for the records that have active urns - " + str(row[0]) + " records were inserted")
    for row in rezultat: text_file.write ("Recreate correct format for the records that have active urns - " + str(row[0]) + " records were inserted \n")

    cursor.execute ("""
        select count (*) from 
        (select distinct
        A.urn_idm_comp
        , A.le_duns_num
        , A.sap_cust_num
        , A.cust_num
        , a.rdc_ctry_num
        , a.ctry
        , A.ord_num
        , a.INACT_FLG
        , A.CREATE_TS
        , A.updt_ts
        , A.id
        from
        BACC_PRD_IDM_ACS.ACS_DEES0.COMP_RDC_XREF_INACT_""" + tmp + """_URN_V5 a) as x;
    """)
    rezultat = cursor.fetchall()
    for row in rezultat: print ("Will be inserted in the main table - " +str (row[0]) + " records")
    for row in rezultat: text_file.write ("Will be inserted in the main table - " +str (row[0]) + " records \n")

    cursor.execute ("""
        insert into
        BACC_PRD_IDM_ACS.ACS_ZLNK0.COMP_RDC_XREF
        (
        urn_idm_comp
        , le_duns_num
        , sap_cust_num
        , cust_num
        , rdc_ctry_num
        , ctry
        , ord_num
        , INACT_FLG
        , CREATE_TS
        , updt_ts
        , id
        , comment
        )

        select distinct
        A.urn_idm_comp
        , A.le_duns_num
        , A.sap_cust_num
        , A.cust_num
        , a.rdc_ctry_num
        , a.ctry
        , A.ord_num
        , a.INACT_FLG
        , A.CREATE_TS
        , A.updt_ts
        , A.id
        , a.comment
        from
        BACC_PRD_IDM_ACS.ACS_DEES0.COMP_RDC_XREF_INACT_""" + tmp + """_URN_V5 a;
    """)

    conn.commit ()
    print ("Insert the data into the main xref table - Done")
    text_file.write ("Insert the data into the main xref table - Done \n")

    cursor.execute ("""
        insert into
        BACC_PRD_IDM_ACS.ACS_ZLNK0.CR_DEE_COMP_RDC_XREF_HIST_022118
        (
        urn_idm_comp
        , le_duns_num
        , sap_cust_num
        , cust_num
        , rdc_ctry_num
        , ctry
        , ord_num
        , INACT_FLG
        , CREATE_TS
        , updt_ts
        , id
        , comment
        )

        select distinct
        A.urn_idm_comp
        , A.le_duns_num
        , A.sap_cust_num
        , A.cust_num
        , a.rdc_ctry_num
        , a.ctry
        , A.ord_num
        , a.INACT_FLG
        , A.CREATE_TS
        , A.updt_ts
        , A.id
        , a.comment
        from
        BACC_PRD_IDM_ACS.ACS_DEES0.COMP_RDC_XREF_INACT_""" + tmp + """_URN_V5 a;
    """)

    conn.commit ()
    print ("Insert the data into the history table - done")
    text_file.write ("Insert the data into the history table - Done \n")

    cursor.execute ("""
        select count (*)
        from
        BACC_PRD_IDM_ACS.ACS_ZLNK0.COMP_RDC_XREF
        where
        inact_flg = 'Y';    
    """)
    rezultat = cursor.fetchall()
    for row in rezultat: print (str(row[0]) + " inactive linkages will be deleted from the main table")
    for row in rezultat: text_file.write (str(row[0]) + " inactive linkages will be deleted from the main table \n")

    cursor.execute ("""
        delete from 
        BACC_PRD_IDM_ACS.ACS_ZLNK0.COMP_RDC_XREF
        where
        inact_flg = 'Y';
    """)

    conn.commit ()
    print ("Delete the records from the main table that have inactive linkages - Done")
	
    cursor.execute ("DROP TABLE BACC_PRD_IDM_ACS.ACS_DEES0.COMP_RDC_XREF_INACT_" + tmp + "_URN IF EXISTS;")
    cursor.execute ("DROP TABLE BACC_PRD_IDM_ACS.ACS_DEES0.COMP_RDC_XREF_INACT_" + tmp + "_URN_V2 IF EXISTS;")
    cursor.execute ("DROP TABLE BACC_PRD_IDM_ACS.ACS_DEES0.COMP_RDC_XREF_INACT_" + tmp + "_URN_V2_max IF EXISTS;")
    cursor.execute ("DROP TABLE BACC_PRD_IDM_ACS.ACS_DEES0.COMP_RDC_XREF_INACT_" + tmp + "_URN_V3 IF EXISTS;")
    cursor.execute ("DROP TABLE BACC_PRD_IDM_ACS.ACS_DEES0.COMP_RDC_XREF_INACT_" + tmp + "_URN_V4 IF EXISTS;")
    cursor.execute ("DROP TABLE BACC_PRD_IDM_ACS.ACS_DEES0.COMP_RDC_XREF_INACT_" + tmp + "_URN_V5 IF EXISTS;")

    conn.commit ()

    print ("Temporary FINAL tables were dropped")
    text_file.write ("Temporary FINAL tables were dropped \n")

    #replace old rdc kwys with the new ones
    cursor.execute ("""create table BACC_PRD_IDM_ACS.ACS_DEES0.RDC_DIF_custnum_""" + tmp + """_V1 as (
		select distinct a.cust_num, a.rdc_ctry_num, a.sap_cust_num, b.cust_num as new_cust_num, b.rdc_ctry_num as new_rdc_ctry_num, a.comment
		from
		BACC_PRD_IDM_ACS.ACS_ZLNK0.COMP_RDC_XREF a 
		inner join 
		(
		select a.sap_cust_num, a.cust_num, a.rdc_ctry_num from BACC_PRD_IDM_IIW.V2INAT2.V_RDC_ON_PAGE_CURR a inner join BACC_PRD_IDM_ACS.ACS_ZLNK0.COMP_RDC_XREF b on a.sap_cust_num = b.sap_cust_num union
		select a.sap_cust_num, a.cust_num, a.rdc_ctry_num from BACC_PRD_IDM_IIW.V2INAT2.V_RDC_ON_PAGE_HIST a inner join BACC_PRD_IDM_ACS.ACS_ZLNK0.COMP_RDC_XREF b on a.sap_cust_num = b.sap_cust_num union
		select a.sap_cust_num, a.cust_num, a.rdc_ctry_num from BACC_PRD_IDM_IIW.V2INAT2.V_RDC_ON_PAGE_TEMP a inner join BACC_PRD_IDM_ACS.ACS_ZLNK0.COMP_RDC_XREF b on a.sap_cust_num = b.sap_cust_num
		) b on a.sap_cust_num = b.sap_cust_num
		where 
		a.cust_num <> b.cust_num or a.rdc_ctry_num <> b.rdc_ctry_num);""")
    conn.commit ()
	
    cursor.execute ("select count (*) from BACC_PRD_IDM_ACS.ACS_DEES0.RDC_DIF_custnum_" + tmp + "_V1")
    rezultat = cursor.fetchall()
    for row in rezultat: print ("Records that have dif cust_num for the same sap_cust_num - " +str (row[0]))
    for row in rezultat: text_file.write ("Records that have dif cust_num for the same sap_cust_num - " +str (row[0]) + "\n")

    cursor.execute ("""create table BACC_PRD_IDM_ACS.ACS_DEES0.COMP_RDC_XREF_INACT_""" + tmp + """_SAP_V1 AS 
		(select distinct A.* from
		BACC_PRD_IDM_ACS.ACS_ZLNK0.COMP_RDC_XREF A LEFT JOIN
		(
		select sap_cust_num from BACC_PRD_IDM_IIW.V2INAT2.V_RDC_ON_PAGE_CURR union
		select sap_cust_num from BACC_PRD_IDM_IIW.V2INAT2.V_RDC_ON_PAGE_HIST union
		select sap_cust_num from BACC_PRD_IDM_IIW.V2INAT2.V_RDC_ON_PAGE_TEMP
		) b on a.SAP_CUST_NUM = b.sap_cust_num
		where b.sap_cust_num is null);""")
    conn.commit ()

    cursor.execute ("select count (*) from BACC_PRD_IDM_ACS.ACS_DEES0.COMP_RDC_XREF_INACT_" + tmp + "_SAP_V1")
    rezultat = cursor.fetchall()
    for row in rezultat: print ("Records that have inactive sap_cust_num - " +str (row[0]))
    for row in rezultat: text_file.write ("Records that have inactive sap_cust_num - " +str (row[0]) + "\n")
	
    cursor.execute ("""create table BACC_PRD_IDM_ACS.ACS_DEES0.COMP_RDC_XREF_INACT_""" + tmp + """_SAP_V2 AS 
		(
		select distinct a.*, b.sap_cust_num as new_sap_cust_num
		from
		BACC_PRD_IDM_ACS.ACS_DEES0.COMP_RDC_XREF_INACT_""" + tmp + """_SAP_V1 a
		inner join 
		(
		select a.sap_cust_num, a.cust_num, a.rdc_ctry_num from BACC_PRD_IDM_IIW.V2INAT2.V_RDC_ON_PAGE_CURR a inner join BACC_PRD_IDM_ACS.ACS_DEES0.COMP_RDC_XREF_INACT_""" + tmp + """_SAP_V1 b on a.cust_num = b.cust_num and a.rdc_ctry_num = b.rdc_ctry_num union
		select a.sap_cust_num, a.cust_num, a.rdc_ctry_num from BACC_PRD_IDM_IIW.V2INAT2.V_RDC_ON_PAGE_HIST a inner join BACC_PRD_IDM_ACS.ACS_DEES0.COMP_RDC_XREF_INACT_""" + tmp + """_SAP_V1 b on a.cust_num = b.cust_num and a.rdc_ctry_num = b.rdc_ctry_num union
		select a.sap_cust_num, a.cust_num, a.rdc_ctry_num from BACC_PRD_IDM_IIW.V2INAT2.V_RDC_ON_PAGE_TEMP a inner join BACC_PRD_IDM_ACS.ACS_DEES0.COMP_RDC_XREF_INACT_""" + tmp + """_SAP_V1 b on a.cust_num = b.cust_num and a.rdc_ctry_num = b.rdc_ctry_num
		) b on a.cust_num = b.cust_num and a.rdc_ctry_num = b.rdc_ctry_num
		left join BACC_PRD_IDM_ACS.ACS_ZLNK0.COMP_RDC_XREF c on b.sap_cust_num = c.sap_cust_num);""")
    conn.commit ()
	
    cursor.execute ("select count (*) from BACC_PRD_IDM_ACS.ACS_DEES0.COMP_RDC_XREF_INACT_" + tmp + "_SAP_V2")
    rezultat = cursor.fetchall()
    for row in rezultat: print ("Records that have different sap_ust_num for the inactive sap_cust_num - " +str (row[0]))
    for row in rezultat: text_file.write ("Records that have different sap_ust_num for the inactive sap_cust_num - " +str (row[0]) + "\n")
	
    cursor.execute ("""create table BACC_PRD_IDM_ACS.ACS_DEES0.RDC_DIF_custnum_""" + tmp + """_V2 as (
		SELECT 
		a.URN_IDM_COMP
			   , a.LE_DUNS_NUM
			   , a.SAP_CUST_NUM
			   , b.NEW_CUST_NUM AS CUST_NUM
			   , b.NEW_RDC_CTRY_NUM AS RDC_CTRY_NUM
			   , a.CTRY
			   , a.ORD_NUM
			   , CAST ('N' AS NVARCHAR (1)) AS INACT_FLG
			   , a.CREATE_TS
			   , CAST (CURRENT_TIMESTAMP AS TIMESTAMP) AS UPDT_TS
			   , a.ID
			   , case when a.comment is null then 'Python Maintenance Script' else a.comment end as comment
		FROM
		BACC_PRD_IDM_ACS.ACS_ZLNK0.COMP_RDC_XREF A
		INNER JOIN BACC_PRD_IDM_ACS.ACS_DEES0.RDC_DIF_custnum_""" + tmp + """_V1 b
		on a.sap_cust_num = b.sap_cust_num
		);""")
    conn.commit ()
	
    cursor.execute ("select count (*) from BACC_PRD_IDM_ACS.ACS_DEES0.RDC_DIF_custnum_" + tmp + "_V2")
    rezultat = cursor.fetchall()
    for row in rezultat: print ("Prepare the format for the records that have dif cust_num for the same sap_cust_num - " +str (row[0]))
    for row in rezultat: text_file.write ("Prepare the format for the records that have dif cust_num for the same sap_cust_num - " +str (row[0]) + "\n")

    cursor.execute ("""select count (*) from
				BACC_PRD_IDM_ACS.ACS_ZLNK0.COMP_RDC_XREF
		where
		sap_cust_num in (select sap_cust_num from BACC_PRD_IDM_ACS.ACS_DEES0.RDC_DIF_custnum_""" + tmp + """_V1)""")
    rezultat = cursor.fetchall()
    for row in rezultat: print ("Inactivate the records that have dif cust_num for the same sap_cust_num in Main Table - " +str (row[0]))
    for row in rezultat: text_file.write ("Inactivate the records that have dif cust_num for the same sap_cust_num in Main Table - " +str (row[0]))
	
    cursor.execute ("""update
		BACC_PRD_IDM_ACS.ACS_ZLNK0.COMP_RDC_XREF
				SET
				INACT_FLG = 'Y'
				, UPDT_TS = CURRENT_TIMESTAMP
				, comment = case when comment is null then 'Python Maintenance Script' else comment end
		where
		sap_cust_num in (select sap_cust_num from BACC_PRD_IDM_ACS.ACS_DEES0.RDC_DIF_custnum_""" + tmp + """_V1);""")
    conn.commit ()
    print ("The records have been updated")
    text_file.write (" - The records have been updated\n")

    cursor.execute ("""select count (*) from
				BACC_PRD_IDM_ACS.ACS_ZLNK0.CR_DEE_COMP_RDC_XREF_HIST_022118
		where
		sap_cust_num in (select sap_cust_num from BACC_PRD_IDM_ACS.ACS_DEES0.RDC_DIF_custnum_""" + tmp + """_V1)
		and inact_flg = 'N'""")

    rezultat = cursor.fetchall()
    for row in rezultat: print ("Inactivate the records that have dif cust_num for the same sap_cust_num in Hist Table - " +str (row[0]))
    for row in rezultat: text_file.write ("Inactivate the records that have dif cust_num for the same sap_cust_num in Hist Table - " +str (row[0]))
	
    cursor.execute ("""update
		BACC_PRD_IDM_ACS.ACS_ZLNK0.CR_DEE_COMP_RDC_XREF_HIST_022118
				SET
				INACT_FLG = 'Y'
				, UPDT_TS = CURRENT_TIMESTAMP
				, comment = case when comment is null then 'Python Maintenance Script' else comment end
		where
		sap_cust_num in (select sap_cust_num from BACC_PRD_IDM_ACS.ACS_DEES0.RDC_DIF_custnum_""" + tmp + """_V1)
		and inact_flg = 'N';""")
    conn.commit ()
    print ("The records have been updated")
    text_file.write (" - The records have been updated\n")

    cursor.execute ("""select count (*) from
		(select distinct
			URN_IDM_COMP
			, LE_DUNS_NUM
			, SAP_CUST_NUM
			, CUST_NUM
			, RDC_CTRY_NUM
			, CTRY
			, ORD_NUM
			, INACT_FLG
			, CREATE_TS
			, UPDT_TS
			, ID
			, comment
			from
			BACC_PRD_IDM_ACS.ACS_DEES0.RDC_DIF_custnum_""" + tmp + """_V2) as x""")
    rezultat = cursor.fetchall()
    for row in rezultat: print ("Loading the records that have dif cust_num for the same sap_cust_num in Main Table - " +str (row[0]))
    for row in rezultat: text_file.write ("Loading the records that have dif cust_num for the same sap_cust_num in Main Table - " +str (row[0]))
	
    cursor.execute ("""insert into
		BACC_PRD_IDM_ACS.ACS_ZLNK0.COMP_RDC_XREF
		(
		URN_IDM_COMP
		, LE_DUNS_NUM
		, SAP_CUST_NUM
		, CUST_NUM
		, RDC_CTRY_NUM
		, CTRY
		, ORD_NUM
		, INACT_FLG
		, CREATE_TS
		, UPDT_TS
		, ID
		, comment
		)
		select distinct
		URN_IDM_COMP
		, LE_DUNS_NUM
		, SAP_CUST_NUM
		, CUST_NUM
		, RDC_CTRY_NUM
		, CTRY
		, ORD_NUM
		, INACT_FLG
		, CREATE_TS
		, UPDT_TS
		, ID
		, comment
		from
		BACC_PRD_IDM_ACS.ACS_DEES0.RDC_DIF_custnum_""" + tmp + """_V2;""")
    conn.commit ()
    print (" - The records were loaded")
    text_file.write (" - The records were loaded\n")
	
    cursor.execute ("""insert into
		BACC_PRD_IDM_ACS.ACS_ZLNK0.CR_DEE_COMP_RDC_XREF_HIST_022118
		(
		URN_IDM_COMP
		, LE_DUNS_NUM
		, SAP_CUST_NUM
		, CUST_NUM
		, RDC_CTRY_NUM
		, CTRY
		, ORD_NUM
		, INACT_FLG
		, CREATE_TS
		, UPDT_TS
		, ID
		, comment
		)
		select distinct
		URN_IDM_COMP
		, LE_DUNS_NUM
		, SAP_CUST_NUM
		, CUST_NUM
		, RDC_CTRY_NUM
		, CTRY
		, ORD_NUM
		, INACT_FLG
		, CREATE_TS
		, UPDT_TS
		, ID
		, comment
		from
		BACC_PRD_IDM_ACS.ACS_DEES0.RDC_DIF_custnum_""" + tmp + """_V2;""")
    conn.commit ()
    print ("The records that have dif cust_num for the same sap_cust_num were loaded in Hist")
    text_file.write ("The records that have dif cust_num for the same sap_cust_num were loaded in Hist\n")
	
    cursor.execute ("""create table BACC_PRD_IDM_ACS.ACS_DEES0.COMP_RDC_XREF_INACT_""" + tmp + """_SAP_V3 as (
		SELECT 
		a.URN_IDM_COMP
			   , a.LE_DUNS_NUM
			   , b.NEW_SAP_CUST_NUM AS SAP_CUST_NUM
			   , a.CUST_NUM
			   , a.RDC_CTRY_NUM
			   , a.CTRY
			   , a.ORD_NUM
			   , CAST ('N' AS NVARCHAR (1)) AS INACT_FLG
			   , a.CREATE_TS
			   , CAST (CURRENT_TIMESTAMP AS TIMESTAMP) AS UPDT_TS
			   , a.ID
			   , case when a.comment is null then 'Python Maintenance Script' else a.comment end as comment
		FROM
		BACC_PRD_IDM_ACS.ACS_ZLNK0.COMP_RDC_XREF A
		INNER JOIN BACC_PRD_IDM_ACS.ACS_DEES0.COMP_RDC_XREF_INACT_""" + tmp + """_SAP_V2 b
		on a.sap_cust_num = b.sap_cust_num);""")
    conn.commit ()
	
    cursor.execute ("select count (*) from BACC_PRD_IDM_ACS.ACS_DEES0.COMP_RDC_XREF_INACT_" + tmp + "_SAP_V3")
	
    rezultat = cursor.fetchall()
    for row in rezultat: print ("Prepare the records that have different sap_ust_num for the inactive sap_cust_num - " +str (row[0]))
    for row in rezultat: text_file.write ("Prepare the records that have different sap_ust_num for the inactive sap_cust_num - " +str (row[0]) + "\n")
	
    cursor.execute ("""select count (*) from 
		BACC_PRD_IDM_ACS.ACS_ZLNK0.COMP_RDC_XREF
		where
		sap_cust_num in (select sap_cust_num from BACC_PRD_IDM_ACS.ACS_DEES0.COMP_RDC_XREF_INACT_""" + tmp + """_SAP_V1)""")
	
    rezultat = cursor.fetchall()
    for row in rezultat: print ("Inactivate the records that have inactive sap_cust_num in Main Table - " +str (row[0]))
    for row in rezultat: text_file.write ("Inactivate the records that have inactive sap_cust_num in Main Table - " +str (row[0]))
	
    cursor.execute ("""update
		BACC_PRD_IDM_ACS.ACS_ZLNK0.COMP_RDC_XREF
				SET
				INACT_FLG = 'Y'
				, UPDT_TS = CURRENT_TIMESTAMP
				, comment = case when comment is null then 'Python Maintenance Script' else comment end
		where
		sap_cust_num in (select sap_cust_num from BACC_PRD_IDM_ACS.ACS_DEES0.COMP_RDC_XREF_INACT_""" + tmp + """_SAP_V1);""")
    conn.commit ()
    print (" - The records have been inactivated in the main table")
    text_file.write (" - The records have been inactivated in the main table\n")
	
    cursor.execute ("""select count (*) from 
			BACC_PRD_IDM_ACS.ACS_ZLNK0.CR_DEE_COMP_RDC_XREF_HIST_022118
		where
		sap_cust_num in (select sap_cust_num from BACC_PRD_IDM_ACS.ACS_DEES0.COMP_RDC_XREF_INACT_""" + tmp + """_SAP_V1)
		and inact_flg = 'N';""")
		
    rezultat = cursor.fetchall()
    for row in rezultat: print ("Inactivate the records that have inactive sap_cust_num in Hist Table - " +str (row[0]))
    for row in rezultat: text_file.write ("Inactivate the records that have inactive sap_cust_num in Hist Table - " +str (row[0]))
	
    cursor.execute ("""update
		BACC_PRD_IDM_ACS.ACS_ZLNK0.CR_DEE_COMP_RDC_XREF_HIST_022118
				SET
				INACT_FLG = 'Y'
				, UPDT_TS = CURRENT_TIMESTAMP
				, comment = case when comment is null then 'Python Maintenance Script' else comment end
		where
		sap_cust_num in (select sap_cust_num from BACC_PRD_IDM_ACS.ACS_DEES0.COMP_RDC_XREF_INACT_""" + tmp + """_SAP_V1)
		and inact_flg = 'N';""")
    conn.commit ()
    print (" - The records have been inactivated in the hist table")
    text_file.write (" - The records have been inactivated in the hist table\n")
	
    cursor.execute ("""select count (*) from (
		select distinct
			URN_IDM_COMP
			, LE_DUNS_NUM
			, SAP_CUST_NUM
			, CUST_NUM
			, RDC_CTRY_NUM
			, CTRY
			, ORD_NUM
			, INACT_FLG
			, CREATE_TS
			, UPDT_TS
			, ID
			, comment
			from
			BACC_PRD_IDM_ACS.ACS_DEES0.COMP_RDC_XREF_INACT_""" + tmp + """_SAP_V3) as x;""")
			
    rezultat = cursor.fetchall()
    for row in rezultat: print ("Loading the records have different sap_ust_num for the inactive sap_cust_num - " +str (row[0]))
    for row in rezultat: text_file.write ("Loading the records have different sap_ust_num for the inactive sap_cust_num - " +str (row[0]))
	
    cursor.execute ("""insert into
		BACC_PRD_IDM_ACS.ACS_ZLNK0.COMP_RDC_XREF
		(
		URN_IDM_COMP
		, LE_DUNS_NUM
		, SAP_CUST_NUM
		, CUST_NUM
		, RDC_CTRY_NUM
		, CTRY
		, ORD_NUM
		, INACT_FLG
		, CREATE_TS
		, UPDT_TS
		, ID
		, comment
		)
		select distinct
		URN_IDM_COMP
		, LE_DUNS_NUM
		, SAP_CUST_NUM
		, CUST_NUM
		, RDC_CTRY_NUM
		, CTRY
		, ORD_NUM
		, INACT_FLG
		, CREATE_TS
		, UPDT_TS
		, ID
		, comment
		from
		BACC_PRD_IDM_ACS.ACS_DEES0.COMP_RDC_XREF_INACT_""" + tmp + """_SAP_V3;""")
    conn.commit ()
	
    print (" - The records have been loaded in Main Table")
    text_file.write (" - The records have been loaded in Main Table")

    cursor.execute ("""insert into
		BACC_PRD_IDM_ACS.ACS_ZLNK0.CR_DEE_COMP_RDC_XREF_HIST_022118
		(
		URN_IDM_COMP
		, LE_DUNS_NUM
		, SAP_CUST_NUM
		, CUST_NUM
		, RDC_CTRY_NUM
		, CTRY
		, ORD_NUM
		, INACT_FLG
		, CREATE_TS
		, UPDT_TS
		, ID
		, comment
		)
		select distinct
		URN_IDM_COMP
		, LE_DUNS_NUM
		, SAP_CUST_NUM
		, CUST_NUM
		, RDC_CTRY_NUM
		, CTRY
		, ORD_NUM
		, INACT_FLG
		, CREATE_TS
		, UPDT_TS
		, ID
		, comment
		from
		BACC_PRD_IDM_ACS.ACS_DEES0.COMP_RDC_XREF_INACT_""" + tmp + """_SAP_V3;""")
    conn.commit ()
    print (" - The records have been loaded in Hist Table")
    text_file.write (" - The records have been loaded in Hist Table\n")

    cursor.execute ("select count (*) from BACC_PRD_IDM_ACS.ACS_ZLNK0.COMP_RDC_XREF where INACT_FLG = 'Y'")
    rezultat = cursor.fetchall()
    for row in rezultat: print ("The inactive records from Main table will be deleted - " + str(row[0]))
    for row in rezultat: text_file.write ("The inactive records from Main table will be deleted - " + str(row[0]))

    cursor.execute ("""delete from
		BACC_PRD_IDM_ACS.ACS_ZLNK0.COMP_RDC_XREF
		where
		INACT_FLG = 'Y';""")
    conn.commit ()
    print (" - The inactive records where deleted from Main")
    text_file.write (" - The inactive records where deleted from Main\n")

    cursor.execute ("DROP TABLE BACC_PRD_IDM_ACS.ACS_DEES0.RDC_DIF_custnum_" + tmp + "_V1 IF EXISTS;")
    cursor.execute ("DROP TABLE BACC_PRD_IDM_ACS.ACS_DEES0.COMP_RDC_XREF_INACT_" + tmp + "_SAP_V1 IF EXISTS;")
    cursor.execute ("DROP TABLE BACC_PRD_IDM_ACS.ACS_DEES0.COMP_RDC_XREF_INACT_" + tmp + "_SAP_V2 IF EXISTS;")
    cursor.execute ("DROP TABLE BACC_PRD_IDM_ACS.ACS_DEES0.RDC_DIF_custnum_" + tmp + "_V2 IF EXISTS;")
    cursor.execute ("DROP TABLE BACC_PRD_IDM_ACS.ACS_DEES0.COMP_RDC_XREF_INACT_" + tmp + "_SAP_V3 IF EXISTS;")
    cursor.execute ("DROP TABLE BACC_PRD_IDM_ACS.ACS_DEES0.RDC_DIF_custnum_" + tmp + "_V1 IF EXISTS;")
    cursor.execute ("DROP TABLE BACC_PRD_IDM_ACS.ACS_DEES0.COMP_RDC_XREF_INACT_" + tmp + "_SAP_V1 IF EXISTS;")
    cursor.execute ("DROP TABLE BACC_PRD_IDM_ACS.ACS_DEES0.COMP_RDC_XREF_INACT_" + tmp + "_SAP_V2 IF EXISTS;")
    cursor.execute ("DROP TABLE BACC_PRD_IDM_ACS.ACS_DEES0.RDC_DIF_custnum_" + tmp + "_V2 IF EXISTS;")
    cursor.execute ("DROP TABLE BACC_PRD_IDM_ACS.ACS_DEES0.COMP_RDC_XREF_INACT_" + tmp + "_SAP_V3 IF EXISTS;")

    conn.commit ()

    print ("Temporary tables were dropped - for replaceing old keys")
    text_file.write ("Temporary tables were dropped  - for replaceing old keys\n")

finally:
    cursor.close()
    conn.close()
    print ("The connection is closed")
    text_file.write ("The connection is closed \n")
    text_file.close()
    