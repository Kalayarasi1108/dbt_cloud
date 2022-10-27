INSERT INTO DATA_VAULT.CORE.SAT_STUDENT_SANCTION (SAT_STUDENT_SANCTION_SK, HUB_STUDENT_SANCTION_KEY, SOURCE, LOAD_DTS,
                                                  ETL_JOB_ID, HASH_MD5, STU_ID, SEQ_NO, STU_SANCTION_EFFECTIVE_DATE,
                                                  STU_SANCTION_END_DATE, SANCTION_TYPE_CD, SANCTION_TYPE,
                                                  SANCTION_REASON_CD, SANCTION_REASON, SPK_NO, SPK_VER_NO, SSP_ATT_NO,
                                                  SANCTION_AMOUNT, SANCTION_DESCRIPTION, AVAIL_YR, SPRD_CD,
                                                  ENDED_BY_USER_ID, VERS, CRUSER, CRDATEI, CRTIMEI, CRTERM, CRWINDOW,
                                                  LAST_MOD_USER, LAST_MOD_DATEI, LAST_MOD_TIMEI, LAST_MOD_TERM,
                                                  LAST_MOD_WINDOW, AUTO_APPLY_FG, AUTO_LIFT_FG, EXPORTED_FG, EP_YEAR,
                                                  EP_NO, STU_REWARD_ID, SANCTION_SCOPE_CD, SANCTION_SCOPE,
                                                  SANCTION_NAME, SANCTION_DEFINITION_DESCRIPTION, IS_DELETED)
SELECT DATA_VAULT.CORE.SEQ.NEXTVAL               SAT_STUDENT_SANCTION_SK,
       MD5(IFNULL(STU_SANCTION.STU_ID, '') || ',' ||
           IFNULL(STU_SANCTION.SEQ_NO, 0)
           )                                     HUB_STUDENT_SANCTION_KEY,
       'AMIS'                                    SOURCE,
       CURRENT_TIMESTAMP::TIMESTAMP_NTZ          LOAD_DTS,
       'SQL' || CURRENT_TIMESTAMP::TIMESTAMP_NTZ ETL_JOB_ID,
       MD5(IFNULL(STU_SANCTION.STU_ID, '') || ',' ||
           IFNULL(STU_SANCTION.SEQ_NO, 0) || ',' ||
           IFNULL(STU_SANCTION.STU_SANCT_EFFCT_DT, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
           IFNULL(STU_SANCTION.STU_SANCT_END_DT, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
           IFNULL(STU_SANCTION.SANCT_TYPE_CD, '') || ',' ||
           IFNULL(SCT_DTL.SANCT_NM, '') || ',' ||
           IFNULL(STU_SANCTION.SANCT_REAS_CD, '') || ',' ||
           IFNULL(SANCTION_REASON_CODE.CODE_DESCR, '') || ',' ||
           IFNULL(STU_SANCTION.SPK_NO, 0) || ',' ||
           IFNULL(STU_SANCTION.SPK_VER_NO, 0) || ',' ||
           IFNULL(STU_SANCTION.SSP_ATT_NO, 0) || ',' ||
           IFNULL(STU_SANCTION.SANCT_AMT, 0) || ',' ||
           IFNULL(STU_SANCTION.SANCT_DESC, '') || ',' ||
           IFNULL(STU_SANCTION.AVAIL_YR, 0) || ',' ||
           IFNULL(STU_SANCTION.SPRD_CD, '') || ',' ||
           IFNULL(STU_SANCTION.ENDED_BY_USER_ID, '') || ',' ||
           IFNULL(STU_SANCTION.VERS, 0) || ',' ||
           IFNULL(STU_SANCTION.CRUSER, '') || ',' ||
           IFNULL(STU_SANCTION.CRDATEI, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
           IFNULL(STU_SANCTION.CRTIMEI, 0) || ',' ||
           IFNULL(STU_SANCTION.CRTERM, '') || ',' ||
           IFNULL(STU_SANCTION.CRWINDOW, '') || ',' ||
           IFNULL(STU_SANCTION.LAST_MOD_USER, '') || ',' ||
           IFNULL(STU_SANCTION.LAST_MOD_DATEI, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
           IFNULL(STU_SANCTION.LAST_MOD_TIMEI, 0) || ',' ||
           IFNULL(STU_SANCTION.LAST_MOD_TERM, '') || ',' ||
           IFNULL(STU_SANCTION.LAST_MOD_WINDOW, '') || ',' ||
           IFNULL(STU_SANCTION.AUTO_APPLY_FG, '') || ',' ||
           IFNULL(STU_SANCTION.AUTO_LIFT_FG, '') || ',' ||
           IFNULL(STU_SANCTION.EXPORTED_FG, '') || ',' ||
           IFNULL(STU_SANCTION.EP_YEAR, 0) || ',' ||
           IFNULL(STU_SANCTION.EP_NO, 0) || ',' ||
           IFNULL(STU_SANCTION.STU_REWARD_ID, 0) || ',' ||
           IFNULL(SCT_DTL.SANCT_SCOPE_CD, '') || ',' ||
           IFNULL(SANCTION_SCOPE_CODE.CODE_DESCR, '') || ',' ||
           IFNULL(SCT_DTL.SANCT_NM, '') || ',' ||
           IFNULL(SCT_DTL.SANCT_DEFN_DESC, '') || ',' ||
           IFNULL('N', '')
           )                                     HASH_MD5,
       STU_SANCTION.STU_ID                   AS   STU_ID,
       STU_SANCTION.SEQ_NO                   AS   SEQ_NO,
       STU_SANCTION.STU_SANCT_EFFCT_DT       AS   STU_SANCTION_EFFECTIVE_DATE,
       STU_SANCTION.STU_SANCT_END_DT         AS   STU_SANCTION_END_DATE,
       STU_SANCTION.SANCT_TYPE_CD            AS   SANCTION_TYPE_CD,
       SCT_DTL.SANCT_NM                      AS   SANCTION_TYPE,
       STU_SANCTION.SANCT_REAS_CD            AS   SANCTION_REASON_CD,
       SANCTION_REASON_CODE.CODE_DESCR       AS   SANCTION_REASON,
       STU_SANCTION.SPK_NO                   AS   SPK_NO,
       STU_SANCTION.SPK_VER_NO               AS   SPK_VER_NO,
       STU_SANCTION.SSP_ATT_NO               AS   SSP_ATT_NO,
       STU_SANCTION.SANCT_AMT                AS   SANCTION_AMOUNT,
       STU_SANCTION.SANCT_DESC               AS   SANCTION_DESCRIPTION,
       STU_SANCTION.AVAIL_YR                 AS   AVAIL_YR,
       STU_SANCTION.SPRD_CD                  AS   SPRD_CD,
       STU_SANCTION.ENDED_BY_USER_ID         AS   ENDED_BY_USER_ID,
       STU_SANCTION.VERS                     AS   VERS,
       STU_SANCTION.CRUSER                   AS   CRUSER,
       STU_SANCTION.CRDATEI                  AS   CRDATEI,
       STU_SANCTION.CRTIMEI                  AS   CRTIMEI,
       STU_SANCTION.CRTERM                   AS   CRTERM,
       STU_SANCTION.CRWINDOW                 AS   CRWINDOW,
       STU_SANCTION.LAST_MOD_USER            AS   LAST_MOD_USER,
       STU_SANCTION.LAST_MOD_DATEI           AS   LAST_MOD_DATEI,
       STU_SANCTION.LAST_MOD_TIMEI           AS   LAST_MOD_TIMEI,
       STU_SANCTION.LAST_MOD_TERM            AS   LAST_MOD_TERM,
       STU_SANCTION.LAST_MOD_WINDOW          AS   LAST_MOD_WINDOW,
       STU_SANCTION.AUTO_APPLY_FG            AS   AUTO_APPLY_FG,
       STU_SANCTION.AUTO_LIFT_FG             AS   AUTO_LIFT_FG,
       STU_SANCTION.EXPORTED_FG              AS   EXPORTED_FG,
       STU_SANCTION.EP_YEAR                  AS   EP_YEAR,
       STU_SANCTION.EP_NO                    AS   EP_NO,
       STU_SANCTION.STU_REWARD_ID            AS   STU_REWARD_ID,
       SCT_DTL.SANCT_SCOPE_CD                AS   SANCTION_SCOPE_CD,
       SANCTION_SCOPE_CODE.CODE_DESCR        AS   SANCTION_SCOPE,
       SCT_DTL.SANCT_NM                      AS   SANCTION_NAME,
       SCT_DTL.SANCT_DEFN_DESC               AS   SANCTION_DEFINITION_DESCRIPTION,
       'N'                                        IS_DELETED
FROM ODS.AMIS.S1STU_SANCTION STU_SANCTION
         LEFT OUTER JOIN ODS.AMIS.S1SYS_SCT_DTL SCT_DTL
                         ON SCT_DTL.SANCT_TYPE_CD = STU_SANCTION.SANCT_TYPE_CD
         LEFT OUTER JOIN ODS.AMIS.S1STC_CODE SANCTION_REASON_CODE
                         ON SANCTION_REASON_CODE.CODE_TYPE = 'SANCTION_REASON'
                             AND SANCTION_REASON_CODE.CODE_ID = STU_SANCTION.SANCT_REAS_CD
         LEFT OUTER JOIN ODS.AMIS.S1STC_CODE SANCTION_SCOPE_CODE
                         ON SANCTION_SCOPE_CODE.CODE_TYPE = 'S1_SANCTION_SCOPE'
                             AND SANCTION_SCOPE_CODE.CODE_ID = SCT_DTL.SANCT_SCOPE_CD
WHERE NOT EXISTS(
        SELECT NULL
        FROM (SELECT HUB_STUDENT_SANCTION_KEY,
                     HASH_MD5,
                     LOAD_DTS,
                     LEAD(LOAD_DTS) OVER (PARTITION BY HUB_STUDENT_SANCTION_KEY ORDER BY LOAD_DTS ASC) EFFECTIVE_END_DTS
              FROM DATA_VAULT.CORE.SAT_STUDENT_SANCTION) S
        WHERE S.EFFECTIVE_END_DTS IS NULL
          AND S.HUB_STUDENT_SANCTION_KEY = MD5(IFNULL(STU_SANCTION.STU_ID, '') || ',' ||
                                               IFNULL(STU_SANCTION.SEQ_NO, 0)
            )
          AND S.HASH_MD5 = MD5(IFNULL(STU_SANCTION.STU_ID, '') || ',' ||
                               IFNULL(STU_SANCTION.SEQ_NO, 0) || ',' ||
                               IFNULL(STU_SANCTION.STU_SANCT_EFFCT_DT, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
                               IFNULL(STU_SANCTION.STU_SANCT_END_DT, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
                               IFNULL(STU_SANCTION.SANCT_TYPE_CD, '') || ',' ||
                               IFNULL(SCT_DTL.SANCT_NM, '') || ',' ||
                               IFNULL(STU_SANCTION.SANCT_REAS_CD, '') || ',' ||
                               IFNULL(SANCTION_REASON_CODE.CODE_DESCR, '') || ',' ||
                               IFNULL(STU_SANCTION.SPK_NO, 0) || ',' ||
                               IFNULL(STU_SANCTION.SPK_VER_NO, 0) || ',' ||
                               IFNULL(STU_SANCTION.SSP_ATT_NO, 0) || ',' ||
                               IFNULL(STU_SANCTION.SANCT_AMT, 0) || ',' ||
                               IFNULL(STU_SANCTION.SANCT_DESC, '') || ',' ||
                               IFNULL(STU_SANCTION.AVAIL_YR, 0) || ',' ||
                               IFNULL(STU_SANCTION.SPRD_CD, '') || ',' ||
                               IFNULL(STU_SANCTION.ENDED_BY_USER_ID, '') || ',' ||
                               IFNULL(STU_SANCTION.VERS, 0) || ',' ||
                               IFNULL(STU_SANCTION.CRUSER, '') || ',' ||
                               IFNULL(STU_SANCTION.CRDATEI, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
                               IFNULL(STU_SANCTION.CRTIMEI, 0) || ',' ||
                               IFNULL(STU_SANCTION.CRTERM, '') || ',' ||
                               IFNULL(STU_SANCTION.CRWINDOW, '') || ',' ||
                               IFNULL(STU_SANCTION.LAST_MOD_USER, '') || ',' ||
                               IFNULL(STU_SANCTION.LAST_MOD_DATEI, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
                               IFNULL(STU_SANCTION.LAST_MOD_TIMEI, 0) || ',' ||
                               IFNULL(STU_SANCTION.LAST_MOD_TERM, '') || ',' ||
                               IFNULL(STU_SANCTION.LAST_MOD_WINDOW, '') || ',' ||
                               IFNULL(STU_SANCTION.AUTO_APPLY_FG, '') || ',' ||
                               IFNULL(STU_SANCTION.AUTO_LIFT_FG, '') || ',' ||
                               IFNULL(STU_SANCTION.EXPORTED_FG, '') || ',' ||
                               IFNULL(STU_SANCTION.EP_YEAR, 0) || ',' ||
                               IFNULL(STU_SANCTION.EP_NO, 0) || ',' ||
                               IFNULL(STU_SANCTION.STU_REWARD_ID, 0) || ',' ||
                               IFNULL(SCT_DTL.SANCT_SCOPE_CD, '') || ',' ||
                               IFNULL(SANCTION_SCOPE_CODE.CODE_DESCR, '') || ',' ||
                               IFNULL(SCT_DTL.SANCT_NM, '') || ',' ||
                               IFNULL(SCT_DTL.SANCT_DEFN_DESC, '') || ',' ||
                               IFNULL('N', '')
            )
    );

