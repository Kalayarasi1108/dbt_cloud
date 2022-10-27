INSERT INTO DATA_VAULT.CORE.SAT_COURSE_ADMISSION_SANCTION(SAT_COURSE_ADMISSION_SANCTION_SK,
                                                          HUB_COURSE_ADMISSION_SANCTION_KEY, SOURCE, LOAD_DTS,
                                                          ETL_JOB_ID, HASH_MD5, SSP_NO, SEQ_NO, STU_ID, EFFECTIVE_DATE,
                                                          EXPIRY_DATE, SANCTION_TYPE_CD, SANCTION_REASON_CD,
                                                          SANCTION_DESC, ENDED_BY_USER_ID, SANCTION_AMOUNT,
                                                          AUTO_APPLY_FG, AUTO_LIFT_FG, EXPORTED_FG, VERS, CRUSER,
                                                          CRDATEI, CRTIMEI, CRTERM, CRWINDOW, LAST_MOD_USER,
                                                          LAST_MOD_DATEI, LAST_MOD_TIMEI, LAST_MOD_TERM,
                                                          LAST_MOD_WINDOW, AVAIL_YR, SPRD_CD, EP_YEAR, EP_NO,
                                                          SANCTION_NAME, SANCTION_SCOPE_CD,
                                                          SANCTION_DEFINITION_DESCRIPTION, IS_DELETED)
SELECT DATA_VAULT.CORE.SEQ.NEXTVAL               SAT_COURSE_ADMISSION_SANCTION_SK,
       MD5(
                   IFNULL(SSP_SCT_DTL.SSP_NO, 0) || ',' ||
                   IFNULL(SSP_SCT_DTL.SEQ_NO, 0) || ',' ||
                   IFNULL(SSP_SCT_DTL.STU_ID, '')
           )                                     HUB_COURSE_ADMISSION_SANCTION_KEY,
       'AMIS'                                    SOURCE,
       CURRENT_TIMESTAMP::TIMESTAMP_NTZ          LOAD_DTS,
       'SQL' || CURRENT_TIMESTAMP::TIMESTAMP_NTZ ETL_JOB_ID,
       MD5(IFNULL(SSP_SCT_DTL.SSP_NO, 0) || ',' ||
           IFNULL(SSP_SCT_DTL.SEQ_NO, 0) || ',' ||
           IFNULL(SSP_SCT_DTL.STU_ID, '') || ',' ||
           IFNULL(SSP_SCT_DTL.EFFCT_DT, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
           IFNULL(SSP_SCT_DTL.EXPIRY_DT, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
           IFNULL(SSP_SCT_DTL.SANCT_TYPE_CD, '') || ',' ||
           IFNULL(SSP_SCT_DTL.SANCT_REAS_CD, '') || ',' ||
           IFNULL(SSP_SCT_DTL.SANCT_DESC, '') || ',' ||
           IFNULL(SSP_SCT_DTL.ENDED_BY_USER_ID, '') || ',' ||
           IFNULL(SSP_SCT_DTL.SANCT_AMT, 0) || ',' ||
           IFNULL(SSP_SCT_DTL.AUTO_APPLY_FG, '') || ',' ||
           IFNULL(SSP_SCT_DTL.AUTO_LIFT_FG, '') || ',' ||
           IFNULL(SSP_SCT_DTL.EXPORTED_FG, '') || ',' ||
           IFNULL(SSP_SCT_DTL.VERS, 0) || ',' ||
           IFNULL(SSP_SCT_DTL.CRUSER, '') || ',' ||
           IFNULL(SSP_SCT_DTL.CRDATEI, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
           IFNULL(SSP_SCT_DTL.CRTIMEI, 0) || ',' ||
           IFNULL(SSP_SCT_DTL.CRTERM, '') || ',' ||
           IFNULL(SSP_SCT_DTL.CRWINDOW, '') || ',' ||
           IFNULL(SSP_SCT_DTL.LAST_MOD_USER, '') || ',' ||
           IFNULL(SSP_SCT_DTL.LAST_MOD_DATEI, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
           IFNULL(SSP_SCT_DTL.LAST_MOD_TIMEI, 0) || ',' ||
           IFNULL(SSP_SCT_DTL.LAST_MOD_TERM, '') || ',' ||
           IFNULL(SSP_SCT_DTL.LAST_MOD_WINDOW, '') || ',' ||
           IFNULL(SSP_SCT_DTL.AVAIL_YR, 0) || ',' ||
           IFNULL(SSP_SCT_DTL.SPRD_CD, '') || ',' ||
           IFNULL(SSP_SCT_DTL.EP_YEAR, 0) || ',' ||
           IFNULL(SSP_SCT_DTL.EP_NO, 0) || ',' ||
           IFNULL(SYS_SCT_DTL.SANCT_NM, '') || ',' ||
           IFNULL(SYS_SCT_DTL.SANCT_DEFN_DESC, '') || ',' ||
           IFNULL(SYS_SCT_DTL.SANCT_SCOPE_CD, '') || ',' ||
           IFNULL('N', '')
           )                                     HASH_MD5,

       SSP_SCT_DTL.SSP_NO           AS           SSP_NO,
       SSP_SCT_DTL.SEQ_NO           AS           SEQ_NO,
       SSP_SCT_DTL.STU_ID           AS           STU_ID,
       SSP_SCT_DTL.EFFCT_DT         AS           EFFECTIVE_DATE,
       SSP_SCT_DTL.EXPIRY_DT        AS           EXPIRY_DATE,
       SSP_SCT_DTL.SANCT_TYPE_CD    AS           SANCTION_TYPE_CD,
       SSP_SCT_DTL.SANCT_REAS_CD    AS           SANCTION_REASON_CD,
       SSP_SCT_DTL.SANCT_DESC       AS           SANCTION_DESC,
       SSP_SCT_DTL.ENDED_BY_USER_ID AS           ENDED_BY_USER_ID,
       SSP_SCT_DTL.SANCT_AMT        AS           SANCTION_AMOUNT,
       SSP_SCT_DTL.AUTO_APPLY_FG    AS           AUTO_APPLY_FG,
       SSP_SCT_DTL.AUTO_LIFT_FG     AS           AUTO_LIFT_FG,
       SSP_SCT_DTL.EXPORTED_FG      AS           EXPORTED_FG,
       SSP_SCT_DTL.VERS             AS           VERS,
       SSP_SCT_DTL.CRUSER           AS           CRUSER,
       SSP_SCT_DTL.CRDATEI          AS           CRDATEI,
       SSP_SCT_DTL.CRTIMEI          AS           CRTIMEI,
       SSP_SCT_DTL.CRTERM           AS           CRTERM,
       SSP_SCT_DTL.CRWINDOW         AS           CRWINDOW,
       SSP_SCT_DTL.LAST_MOD_USER    AS           LAST_MOD_USER,
       SSP_SCT_DTL.LAST_MOD_DATEI   AS           LAST_MOD_DATEI,
       SSP_SCT_DTL.LAST_MOD_TIMEI   AS           LAST_MOD_TIMEI,
       SSP_SCT_DTL.LAST_MOD_TERM    AS           LAST_MOD_TERM,
       SSP_SCT_DTL.LAST_MOD_WINDOW  AS           LAST_MOD_WINDOW,
       SSP_SCT_DTL.AVAIL_YR         AS           AVAIL_YR,
       SSP_SCT_DTL.SPRD_CD          AS           SPRD_CD,
       SSP_SCT_DTL.EP_YEAR          AS           EP_YEAR,
       SSP_SCT_DTL.EP_NO            AS           EP_NO,
       SYS_SCT_DTL.SANCT_NM         AS           SANCTION_NAME,
       SYS_SCT_DTL.SANCT_DEFN_DESC  AS           SANCTION_SCOPE_CD,
       SYS_SCT_DTL.SANCT_SCOPE_CD   AS           SANCTION_DEFINITION_DESCRIPTION,
       'N'                                       IS_DELETED

FROM ODS.AMIS.S1SSP_SCT_DTL SSP_SCT_DTL
         JOIN ODS.AMIS.S1SSP_STU_SPK STU_SPK
              ON STU_SPK.SSP_NO = SSP_SCT_DTL.SSP_NO
         JOIN ODS.AMIS.S1SPK_DET SPK_DET
              ON STU_SPK.SPK_NO = SPK_DET.SPK_NO
                  and STU_SPK.SPK_VER_NO = SPK_DET.SPK_VER_NO AND SPK_DET.SPK_CAT_CD = 'CS'
         LEFT OUTER JOIN ODS.AMIS.S1SYS_SCT_DTL SYS_SCT_DTL
                         ON SSP_SCT_DTL.SANCT_TYPE_CD = SYS_SCT_DTL.SANCT_TYPE_CD
         LEFT OUTER JOIN ODS.AMIS.S1STC_CODE SANCTION_REASON_CODE
                         ON SANCTION_REASON_CODE.CODE_TYPE = 'SANCTION_REASON'
                             AND SANCTION_REASON_CODE.CODE_ID = SYS_SCT_DTL.SANCT_REAS_CD
         LEFT OUTER JOIN ODS.AMIS.S1STC_CODE SANCTION_SCOPE_CODE
                         ON SANCTION_SCOPE_CODE.CODE_TYPE = 'S1_SANCTION_SCOPE'
                             AND SANCTION_SCOPE_CODE.CODE_ID = SYS_SCT_DTL.SANCT_SCOPE_CD
WHERE NOT EXISTS(
        SELECT NULL
        FROM (SELECT HUB_COURSE_ADMISSION_SANCTION_KEY,
                     HASH_MD5,
                     LOAD_DTS,
                     LEAD(LOAD_DTS)
                          OVER (PARTITION BY HUB_COURSE_ADMISSION_SANCTION_KEY ORDER BY LOAD_DTS ASC) EFFECTIVE_END_DTS
              FROM DATA_VAULT.CORE.SAT_COURSE_ADMISSION_SANCTION) S
        WHERE S.EFFECTIVE_END_DTS IS NULL
          AND S.HUB_COURSE_ADMISSION_SANCTION_KEY = MD5(
                    IFNULL(SSP_SCT_DTL.SSP_NO, 0) || ',' ||
                    IFNULL(SSP_SCT_DTL.SEQ_NO, 0) || ',' ||
                    IFNULL(SSP_SCT_DTL.STU_ID, '')
            )
          AND S.HASH_MD5 = MD5(IFNULL(SSP_SCT_DTL.SSP_NO, 0) || ',' ||
                               IFNULL(SSP_SCT_DTL.SEQ_NO, 0) || ',' ||
                               IFNULL(SSP_SCT_DTL.STU_ID, '') || ',' ||
                               IFNULL(SSP_SCT_DTL.EFFCT_DT, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
                               IFNULL(SSP_SCT_DTL.EXPIRY_DT, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
                               IFNULL(SSP_SCT_DTL.SANCT_TYPE_CD, '') || ',' ||
                               IFNULL(SSP_SCT_DTL.SANCT_REAS_CD, '') || ',' ||
                               IFNULL(SSP_SCT_DTL.SANCT_DESC, '') || ',' ||
                               IFNULL(SSP_SCT_DTL.ENDED_BY_USER_ID, '') || ',' ||
                               IFNULL(SSP_SCT_DTL.SANCT_AMT, 0) || ',' ||
                               IFNULL(SSP_SCT_DTL.AUTO_APPLY_FG, '') || ',' ||
                               IFNULL(SSP_SCT_DTL.AUTO_LIFT_FG, '') || ',' ||
                               IFNULL(SSP_SCT_DTL.EXPORTED_FG, '') || ',' ||
                               IFNULL(SSP_SCT_DTL.VERS, 0) || ',' ||
                               IFNULL(SSP_SCT_DTL.CRUSER, '') || ',' ||
                               IFNULL(SSP_SCT_DTL.CRDATEI, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
                               IFNULL(SSP_SCT_DTL.CRTIMEI, 0) || ',' ||
                               IFNULL(SSP_SCT_DTL.CRTERM, '') || ',' ||
                               IFNULL(SSP_SCT_DTL.CRWINDOW, '') || ',' ||
                               IFNULL(SSP_SCT_DTL.LAST_MOD_USER, '') || ',' ||
                               IFNULL(SSP_SCT_DTL.LAST_MOD_DATEI, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
                               IFNULL(SSP_SCT_DTL.LAST_MOD_TIMEI, 0) || ',' ||
                               IFNULL(SSP_SCT_DTL.LAST_MOD_TERM, '') || ',' ||
                               IFNULL(SSP_SCT_DTL.LAST_MOD_WINDOW, '') || ',' ||
                               IFNULL(SSP_SCT_DTL.AVAIL_YR, 0) || ',' ||
                               IFNULL(SSP_SCT_DTL.SPRD_CD, '') || ',' ||
                               IFNULL(SSP_SCT_DTL.EP_YEAR, 0) || ',' ||
                               IFNULL(SSP_SCT_DTL.EP_NO, 0) || ',' ||
                               IFNULL(SYS_SCT_DTL.SANCT_NM, '') || ',' ||
                               IFNULL(SYS_SCT_DTL.SANCT_DEFN_DESC, '') || ',' ||
                               IFNULL(SYS_SCT_DTL.SANCT_SCOPE_CD, '') || ',' ||
                               IFNULL('N', '')
            )
    );