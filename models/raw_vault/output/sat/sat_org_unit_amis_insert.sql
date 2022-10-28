-- INSERT
INSERT INTO DATA_VAULT.CORE.SAT_ORG_UNIT_AMIS (SAT_ORG_UNIT_AMIS_SK, HUB_ORG_UNIT_KEY, SOURCE, LOAD_DTS, ETL_JOB_ID,
                                               HASH_MD5, ORG_UNIT_CD, EFFCT_DT, EXPIRY_DT, ORG_UNIT_NM,
                                               ORG_UNIT_SHORT_NM, ORG_UNIT_TYPE_CD, PARENT_ORG_UNIT_CD, AC_ORG_UNIT_CD,
                                               EXAM_ADMIN_FG, VERS, CRUSER, CRDATEI, CRTIMEI, CRTERM, CRWINDOW,
                                               LAST_MOD_USER, LAST_MOD_DATEI, LAST_MOD_TIMEI, LAST_MOD_TERM,
                                               LAST_MOD_WINDOW, EAP_SEARCH_FG, KEY_CONTACT_STAFF_ID, MANAGER_STAFF_ID,
                                               TRADING_NAME, ADDR_1, ADDR_2, ADDR_3, SUBURB, STATE, POSTCODE,
                                               COUNTRY_CODE, VALIDATION_STATUS, DPID, LATITUDE, LONGITUDE, IS_DELETED)
SELECT DATA_VAULT.CORE.SEQ.nextval               SAT_ORG_UNIT_AMIS_SK,
       MD5(IFNULL(ORG.ORG_UNIT_CD, '')
           )                                     HUB_ORG_UNIT_KEY,
       'AMIS'                                    SOURCE,
       ORG.EFFCT_DT                             LOAD_DTS,
       'SQL' || CURRENT_TIMESTAMP::TIMESTAMP_NTZ ETL_JOB_ID,
       MD5(
                   IFNULL(ORG.ORG_UNIT_CD, '') || ',' ||
                   IFNULL(TO_CHAR(ORG.EFFCT_DT, 'YYYY-MM-DD'), '') || ',' ||
                   IFNULL(TO_CHAR(ORG.EXPIRY_DT, 'YYYY-MM-DD'), '') || ',' ||
                   IFNULL(ORG.ORG_UNIT_NM, '') || ',' ||
                   IFNULL(ORG.ORG_UNIT_SHORT_NM, '') || ',' ||
                   IFNULL(ORG.ORG_UNIT_TYPE_CD, '') || ',' ||
                   IFNULL(ORG.PARENT_ORG_UNIT_CD, '') || ',' ||
                   IFNULL(ORG.AC_ORG_UNIT_CD, '') || ',' ||
                   IFNULL(ORG.EXAM_ADMIN_FG, '') || ',' ||
                   IFNULL(ORG.VERS, 0) || ',' ||
                   IFNULL(ORG.CRUSER, '') || ',' ||
                   IFNULL(TO_CHAR(ORG.CRDATEI, 'YYYY-MM-DD'), '') || ',' ||
                   IFNULL(ORG.CRTIMEI, 0) || ',' ||
                   IFNULL(ORG.CRTERM, '') || ',' ||
                   IFNULL(ORG.CRWINDOW, '') || ',' ||
                   IFNULL(ORG.LAST_MOD_USER, '') || ',' ||
                   IFNULL(TO_CHAR(ORG.LAST_MOD_DATEI, 'YYYY-MM-DD'), '') || ',' ||
                   IFNULL(ORG.LAST_MOD_TIMEI, 0) || ',' ||
                   IFNULL(ORG.LAST_MOD_TERM, '') || ',' ||
                   IFNULL(ORG.LAST_MOD_WINDOW, '') || ',' ||
                   IFNULL(ORG.EAP_SEARCH_FG, '') || ',' ||
                   IFNULL(ORG.KEY_CONTACT_STAFF_ID, '') || ',' ||
                   IFNULL(ORG.MANAGER_STAFF_ID, '') || ',' ||
                   IFNULL(ORG.TRADING_NAME, '') || ',' ||
                   IFNULL(ORG.ADDR_1, '') || ',' ||
                   IFNULL(ORG.ADDR_2, '') || ',' ||
                   IFNULL(ORG.ADDR_3, '') || ',' ||
                   IFNULL(ORG.SUBURB, '') || ',' ||
                   IFNULL(ORG.STATE, '') || ',' ||
                   IFNULL(ORG.POSTCODE, '') || ',' ||
                   IFNULL(ORG.COUNTRY_CODE, '') || ',' ||
                   IFNULL(ORG.VALIDATION_STATUS, '') || ',' ||
                   IFNULL(ORG.DPID, 0) || ',' ||
                   IFNULL(ORG.LATITUDE, 0) || ',' ||
                   IFNULL(ORG.LONGITUDE, '') || ',' ||
                   IFNULL('N', '')
           )                                     HASH_MD5,
       ORG_UNIT_CD,
       EFFCT_DT,
       EXPIRY_DT,
       ORG_UNIT_NM,
       ORG_UNIT_SHORT_NM,
       ORG_UNIT_TYPE_CD,
       PARENT_ORG_UNIT_CD,
       AC_ORG_UNIT_CD,
       EXAM_ADMIN_FG,
       VERS,
       CRUSER,
       CRDATEI,
       CRTIMEI,
       CRTERM,
       CRWINDOW,
       LAST_MOD_USER,
       LAST_MOD_DATEI,
       LAST_MOD_TIMEI,
       LAST_MOD_TERM,
       LAST_MOD_WINDOW,
       EAP_SEARCH_FG,
       KEY_CONTACT_STAFF_ID,
       MANAGER_STAFF_ID,
       TRADING_NAME,
       ADDR_1,
       ADDR_2,
       ADDR_3,
       SUBURB,
       STATE,
       POSTCODE,
       COUNTRY_CODE,
       VALIDATION_STATUS,
       DPID,
       LATITUDE,
       LONGITUDE,
       'N'                                       IS_DELETED
FROM ODS.AMIS.S1ORG_UNIT ORG
WHERE NOT EXISTS(
        SELECT NULL
        FROM DATA_VAULT.CORE.SAT_ORG_UNIT_AMIS SAT
        WHERE SAT.ORG_UNIT_CD=ORG.ORG_UNIT_CD
          AND SAT.EFFCT_DT=ORG.EFFCT_DT
    )
;

