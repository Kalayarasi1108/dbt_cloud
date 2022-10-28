-- INSERT
INSERT INTO CORE.SAT_STUDENT_ADDRESS
(SAT_STUDENT_ADDRESS_SK,
 HUB_STUDENT_KEY,
 SOURCE,
 LOAD_DTS,
 ETL_JOB_ID,
 HASH_MD5,
 HOME_ADDRESS_LINE_1,
 HOME_ADDRESS_LINE_2,
 HOME_ADDRESS_LINE_3,
 HOME_SUBURB,
 HOME_STATE,
 HOME_POST_CODE,
 HOME_COUNTRY,
 HOME_PREF_MAIL_FG,
 CONTACT_ADDRESS_LINE_1,
 CONTACT_ADDRESS_LINE_2,
 CONTACT_ADDRESS_LINE_3,
 CONTACT_SUBURB,
 CONTACT_STATE,
 CONTACT_POST_CODE,
 CONTACT_COUNTRY,
 CONTACT_PREF_MAIL_FG,
 TEMPORARY_ADDRESS_LINE_1,
 TEMPORARY_ADDRESS_LINE_2,
 TEMPORARY_ADDRESS_LINE_3,
 TEMPORARY_SUBURB,
 TEMPORARY_STATE,
 TEMPORARY_POST_CODE,
 TEMPORARY_COUNTRY,
 TEMPORARY_PREF_MAIL_FG,
 IS_DELETED)
SELECT CORE.SEQ.NEXTVAL                          SAT_STUDENT_ADDRESS_SK,
       S.HUB_STUDENT_KEY,
       'AMIS'                                    SOURCE,
       CURRENT_TIMESTAMP::timestamp_ntz          LOAD_DTS,
       'SQL' || CURRENT_TIMESTAMP::TIMESTAMP_NTZ ETL_JOB_ID,
       MD5(IFNULL(S.HOME_ADDRESS_LINE_1, '') || ',' ||
           IFNULL(S.HOME_ADDRESS_LINE_2, '') || ',' ||
           IFNULL(S.HOME_ADDRESS_LINE_3, '') || ',' ||
           IFNULL(S.HOME_SUBURB, '') || ',' ||
           IFNULL(S.HOME_STATE, '') || ',' ||
           IFNULL(S.HOME_POST_CODE, '') || ',' ||
           IFNULL(S.HOME_COUNTRY, '') || ',' ||
           IFNULL(S.HOME_PREF_MAIL_FG, '') || ',' ||
           IFNULL(S.CONTACT_ADDRESS_LINE_1, '') || ',' ||
           IFNULL(S.CONTACT_ADDRESS_LINE_2, '') || ',' ||
           IFNULL(S.CONTACT_ADDRESS_LINE_3, '') || ',' ||
           IFNULL(S.CONTACT_SUBURB, '') || ',' ||
           IFNULL(S.CONTACT_STATE, '') || ',' ||
           IFNULL(S.CONTACT_POST_CODE, '') || ',' ||
           IFNULL(S.CONTACT_COUNTRY, '') || ',' ||
           IFNULL(S.CONTACT_PREF_MAIL_FG, '') || ',' ||
           IFNULL(S.TEMPORARY_ADDRESS_LINE_1, '') || ',' ||
           IFNULL(S.TEMPORARY_ADDRESS_LINE_2, '') || ',' ||
           IFNULL(S.TEMPORARY_ADDRESS_LINE_3, '') || ',' ||
           IFNULL(S.TEMPORARY_SUBURB, '') || ',' ||
           IFNULL(S.TEMPORARY_STATE, '') || ',' ||
           IFNULL(S.TEMPORARY_POST_CODE, '') || ',' ||
           IFNULL(S.TEMPORARY_COUNTRY, '') || ',' ||
           IFNULL(S.TEMPORARY_PREF_MAIL_FG, '') || ',' ||
           'N')                                  HASH_MD5,
       S.HOME_ADDRESS_LINE_1,
       S.HOME_ADDRESS_LINE_2,
       S.HOME_ADDRESS_LINE_3,
       S.HOME_SUBURB,
       S.HOME_STATE,
       S.HOME_POST_CODE,
       S.HOME_COUNTRY,
       S.HOME_PREF_MAIL_FG,
       S.CONTACT_ADDRESS_LINE_1,
       S.CONTACT_ADDRESS_LINE_2,
       S.CONTACT_ADDRESS_LINE_3,
       S.CONTACT_SUBURB,
       S.CONTACT_STATE,
       S.CONTACT_POST_CODE,
       S.CONTACT_COUNTRY,
       S.CONTACT_PREF_MAIL_FG,
       S.TEMPORARY_ADDRESS_LINE_1,
       S.TEMPORARY_ADDRESS_LINE_2,
       S.TEMPORARY_ADDRESS_LINE_3,
       S.TEMPORARY_SUBURB,
       S.TEMPORARY_STATE,
       S.TEMPORARY_POST_CODE,
       S.TEMPORARY_COUNTRY,
       S.TEMPORARY_PREF_MAIL_FG,
       'N'                                       IS_DELETED
FROM (
         SELECT MD5(STU_ID)                                                                HUB_STUDENT_KEY,
                MAX(CASE WHEN STU_ADDR_TYPE_CD = 'P' THEN STU_ADDR_1 ELSE NULL END)        HOME_ADDRESS_LINE_1,
                MAX(CASE WHEN STU_ADDR_TYPE_CD = 'P' THEN STU_ADDR_2 ELSE NULL END)        HOME_ADDRESS_LINE_2,
                MAX(CASE WHEN STU_ADDR_TYPE_CD = 'P' THEN STU_ADDR_3 ELSE NULL END)        HOME_ADDRESS_LINE_3,
                MAX(CASE WHEN STU_ADDR_TYPE_CD = 'P' THEN STU_SUBURB ELSE NULL END)        HOME_SUBURB,
                MAX(CASE WHEN STU_ADDR_TYPE_CD = 'P' THEN STU_STATE ELSE NULL END)         HOME_STATE,
                MAX(CASE WHEN STU_ADDR_TYPE_CD = 'P' THEN STU_PCODE ELSE NULL END)         HOME_POST_CODE,
                MAX(CASE WHEN STU_ADDR_TYPE_CD = 'P' THEN STU_COUNTRY_CD ELSE NULL END)    HOME_COUNTRY,
                MAX(CASE WHEN STU_ADDR_TYPE_CD = 'P' THEN STU_PREF_MAIL_FG ELSE NULL END)  HOME_PREF_MAIL_FG,
                MAX(CASE WHEN STU_ADDR_TYPE_CD = 'C' THEN STU_ADDR_1 ELSE NULL END)        CONTACT_ADDRESS_LINE_1,
                MAX(CASE WHEN STU_ADDR_TYPE_CD = 'C' THEN STU_ADDR_2 ELSE NULL END)        CONTACT_ADDRESS_LINE_2,
                MAX(CASE WHEN STU_ADDR_TYPE_CD = 'C' THEN STU_ADDR_3 ELSE NULL END)        CONTACT_ADDRESS_LINE_3,
                MAX(CASE WHEN STU_ADDR_TYPE_CD = 'C' THEN STU_SUBURB ELSE NULL END)        CONTACT_SUBURB,
                MAX(CASE WHEN STU_ADDR_TYPE_CD = 'C' THEN STU_STATE ELSE NULL END)         CONTACT_STATE,
                MAX(CASE WHEN STU_ADDR_TYPE_CD = 'C' THEN STU_PCODE ELSE NULL END)         CONTACT_POST_CODE,
                MAX(CASE WHEN STU_ADDR_TYPE_CD = 'C' THEN STU_COUNTRY_CD ELSE NULL END)    CONTACT_COUNTRY,
                MAX(CASE WHEN STU_ADDR_TYPE_CD = 'C' THEN STU_PREF_MAIL_FG ELSE NULL END)  CONTACT_PREF_MAIL_FG,
                MAX(CASE WHEN STU_ADDR_TYPE_CD = '$T' THEN STU_ADDR_1 ELSE NULL END)       TEMPORARY_ADDRESS_LINE_1,
                MAX(CASE WHEN STU_ADDR_TYPE_CD = '$T' THEN STU_ADDR_2 ELSE NULL END)       TEMPORARY_ADDRESS_LINE_2,
                MAX(CASE WHEN STU_ADDR_TYPE_CD = '$T' THEN STU_ADDR_3 ELSE NULL END)       TEMPORARY_ADDRESS_LINE_3,
                MAX(CASE WHEN STU_ADDR_TYPE_CD = '$T' THEN STU_SUBURB ELSE NULL END)       TEMPORARY_SUBURB,
                MAX(CASE WHEN STU_ADDR_TYPE_CD = '$T' THEN STU_STATE ELSE NULL END)        TEMPORARY_STATE,
                MAX(CASE WHEN STU_ADDR_TYPE_CD = '$T' THEN STU_PCODE ELSE NULL END)        TEMPORARY_POST_CODE,
                MAX(CASE WHEN STU_ADDR_TYPE_CD = '$T' THEN STU_COUNTRY_CD ELSE NULL END)   TEMPORARY_COUNTRY,
                MAX(CASE WHEN STU_ADDR_TYPE_CD = '$T' THEN STU_PREF_MAIL_FG ELSE NULL END) TEMPORARY_PREF_MAIL_FG
         FROM ODS.AMIS.S1STU_ADDR
         WHERE STU_ADDR_TYPE_CD IN ('P', 'C', '$T')
         GROUP BY STU_ID
     ) S
WHERE NOT EXISTS(
        SELECT 1
        FROM (SELECT HUB_STUDENT_KEY,
                     HASH_MD5,
                     LOAD_DTS,
                     LEAD(LOAD_DTS) OVER (PARTITION BY HUB_STUDENT_KEY ORDER BY LOAD_DTS ASC) EFFECTIVE_END_DTS
              FROM DATA_VAULT.CORE.SAT_STUDENT_ADDRESS
             ) SAT
        WHERE SAT.HUB_STUDENT_KEY = S.HUB_STUDENT_KEY
          AND SAT.EFFECTIVE_END_DTS IS NULL
          AND SAT.HASH_MD5 = MD5(IFNULL(S.HOME_ADDRESS_LINE_1, '') || ',' ||
                                 IFNULL(S.HOME_ADDRESS_LINE_2, '') || ',' ||
                                 IFNULL(S.HOME_ADDRESS_LINE_3, '') || ',' ||
                                 IFNULL(S.HOME_SUBURB, '') || ',' ||
                                 IFNULL(S.HOME_STATE, '') || ',' ||
                                 IFNULL(S.HOME_POST_CODE, '') || ',' ||
                                 IFNULL(S.HOME_COUNTRY, '') || ',' ||
                                 IFNULL(S.HOME_PREF_MAIL_FG, '') || ',' ||
                                 IFNULL(S.CONTACT_ADDRESS_LINE_1, '') || ',' ||
                                 IFNULL(S.CONTACT_ADDRESS_LINE_2, '') || ',' ||
                                 IFNULL(S.CONTACT_ADDRESS_LINE_3, '') || ',' ||
                                 IFNULL(S.CONTACT_SUBURB, '') || ',' ||
                                 IFNULL(S.CONTACT_STATE, '') || ',' ||
                                 IFNULL(S.CONTACT_POST_CODE, '') || ',' ||
                                 IFNULL(S.CONTACT_COUNTRY, '') || ',' ||
                                 IFNULL(S.CONTACT_PREF_MAIL_FG, '') || ',' ||
                                 IFNULL(S.TEMPORARY_ADDRESS_LINE_1, '') || ',' ||
                                 IFNULL(S.TEMPORARY_ADDRESS_LINE_2, '') || ',' ||
                                 IFNULL(S.TEMPORARY_ADDRESS_LINE_3, '') || ',' ||
                                 IFNULL(S.TEMPORARY_SUBURB, '') || ',' ||
                                 IFNULL(S.TEMPORARY_STATE, '') || ',' ||
                                 IFNULL(S.TEMPORARY_POST_CODE, '') || ',' ||
                                 IFNULL(S.TEMPORARY_COUNTRY, '') || ',' ||
                                 IFNULL(S.TEMPORARY_PREF_MAIL_FG, '') || ',' ||
                                 'N')
    );

