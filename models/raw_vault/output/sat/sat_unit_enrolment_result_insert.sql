INSERT INTO DATA_VAULT.CORE.SAT_UNIT_ENROLMENT_RESULT (SAT_UNIT_ENROLMENT_RESULT_SK, HUB_UNIT_ENROLMENT_RESULT_KEY,
                                                       SOURCE, LOAD_DTS, ETL_JOB_ID, HASH_MD5, SSP_NO, SEQ_NO,
                                                       STUDENT_ID, UNIT_CODE, UNIT_SPK_NO, UNIT_SPK_VER_NO,
                                                       UNIT_SSP_ATT_NO, RESULT_TYPE_CODE, EFFECT_DATE, GRADE_CODE,
                                                       GRADE_DESCRIPTION, GRADE_TYPE_CODE, GRADE_TYPE, PASS_FAIL_CODE,
                                                       PASS_FAIL, MARK, GRADE_EFFECTIVE_DATE, RESULT_REASON_CODE,
                                                       RESULT_REASON, MARKS_CODE, MARKS_CODE_DESC, VERIFIED_FG,
                                                       RATIFIED_FG, CERTIFIED_FG, VERS, CREATE_DTS, LAST_MODIFIED_DTS,
                                                       IS_DELETED)
SELECT DATA_VAULT.CORE.SEQ.NEXTVAL                  SAT_UNIT_ENROLMENT_RESULT_SK,
       MD5(
                   IFNULL(SSP_RSLT_HIST.SSP_NO, 0) || ',' ||
                   IFNULL(SSP_RSLT_HIST.SEQ_NO, 0)) HUB_UNIT_ENROLMENT_RESULT_KEY,
       'AMIS'                                       SOURCE,
       CURRENT_TIMESTAMP::TIMESTAMP_NTZ             LOAD_DTS,
       'SQL' || CURRENT_TIMESTAMP::TIMESTAMP_NTZ    ETL_JOB_ID,
       MD5(
                   IFNULL(SSP_RSLT_HIST.SSP_NO, 0) || ',' ||
                   IFNULL(SSP_RSLT_HIST.SEQ_NO, 0) || ',' ||
                   IFNULL(SSP_RSLT_HIST.STU_ID, '') || ',' ||
                   IFNULL(UN_SPK_DET.SPK_CD, '') || ',' ||
                   IFNULL(SSP_RSLT_HIST.SPK_NO, 0) || ',' ||
                   IFNULL(SSP_RSLT_HIST.SPK_VER_NO, 0) || ',' ||
                   IFNULL(SSP_RSLT_HIST.SSP_ATT_NO, 0) || ',' ||
                   IFNULL(SSP_RSLT_HIST.RSLT_TYPE_CD, '') || ',' ||
                   IFNULL(SSP_RSLT_HIST.EFFCT_DT, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
                   IFNULL(SSP_RSLT_HIST.GRADE_CD, '') || ',' ||
                   IFNULL(RSL_DET.GRADE_DESCRIPTION, '') || ',' ||
                   IFNULL(SSP_RSLT_HIST.GRADE_TYPE_CD, '') || ',' ||
                   IFNULL(GRADE_TYPE_CODE.CODE_DESCR, '') || ',' ||
                   IFNULL(SSP_RSLT_HIST.PASS_FAIL_CD, '') || ',' ||
                   IFNULL(PASS_FAIL_CODE.CODE_DESCR, '') || ',' ||
                   IFNULL(SSP_RSLT_HIST.MARK, 0) || ',' ||
                   IFNULL(SSP_RSLT_HIST.GRADE_EFFCT_DT, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
                   IFNULL(SSP_RSLT_HIST.RSLT_REASON_CD, '') || ',' ||
                   IFNULL(RSLT_RSN_CODE.CODE_DESCR, '') || ',' ||
                   IFNULL(RSL_DET.MARKS_CD, '') || ',' ||
                   IFNULL(MARKS_CODE.CODE_DESCR, '') || ',' ||
                   IFNULL(SSP_RSLT_HIST.VERIFIED_FG, '') || ',' ||
                   IFNULL(SSP_RSLT_HIST.RATIFIED_FG, '') || ',' ||
                   IFNULL(SSP_RSLT_HIST.CERTIFIED_FG, '') || ',' ||
                   IFNULL(SSP_RSLT_HIST.VERS, 0) || ',' ||
                   IFNULL(TO_TIMESTAMP_NTZ(
                                      TO_CHAR(SSP_RSLT_HIST.CRDATEI, 'YYYY-MM-DD') || ' ' ||
                                      LPAD(SSP_RSLT_HIST.CRTIMEI, 6, '0'),
                                      'YYYY-MM-DD HH24MISS'), TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
                   IFNULL(TO_TIMESTAMP_NTZ(
                                      TO_CHAR(SSP_RSLT_HIST.LAST_MOD_DATEI, 'YYYY-MM-DD') || ' ' ||
                                      LPAD(SSP_RSLT_HIST.LAST_MOD_TIMEI, 6, '0'),
                                      'YYYY-MM-DD HH24MISS'), TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
                   'N'
           )                                        HASH_MD5,
       SSP_RSLT_HIST.SSP_NO,
       SSP_RSLT_HIST.SEQ_NO,
       SSP_RSLT_HIST.STU_ID                         STUDENT_ID,
       UN_SPK_DET.SPK_CD                            UNIT_CODE,
       SSP_RSLT_HIST.SPK_NO                         UNIT_SPK_NO,
       SSP_RSLT_HIST.SPK_VER_NO                     UNIT_SPK_VER_NO,
       SSP_RSLT_HIST.SSP_ATT_NO                     UNIT_SSP_ATT_NO,
       SSP_RSLT_HIST.RSLT_TYPE_CD                   RESULT_TYPE_CODE,
       SSP_RSLT_HIST.EFFCT_DT                       EFFECT_DATE,
       SSP_RSLT_HIST.GRADE_CD                       GRADE_CODE,
       RSL_DET.GRADE_DESCRIPTION,
       SSP_RSLT_HIST.GRADE_TYPE_CD                  GRADE_TYPE_CODE,
       GRADE_TYPE_CODE.CODE_DESCR                   GRADE_TYPE,
       SSP_RSLT_HIST.PASS_FAIL_CD                   PASS_FAIL_CODE,
       PASS_FAIL_CODE.CODE_DESCR                    PASS_FAIL,
       SSP_RSLT_HIST.MARK,
       SSP_RSLT_HIST.GRADE_EFFCT_DT                 GRADE_EFFECTIVE_DATE,
       SSP_RSLT_HIST.RSLT_REASON_CD                 RESULT_REASON_CODE,
       RSLT_RSN_CODE.CODE_DESCR                     RESULT_REASON,
       RSL_DET.MARKS_CD                             MARKS_CODE,
       MARKS_CODE.CODE_DESCR                        MARKS_CODE_DESC,
       SSP_RSLT_HIST.VERIFIED_FG,
       SSP_RSLT_HIST.RATIFIED_FG,
       SSP_RSLT_HIST.CERTIFIED_FG,
       SSP_RSLT_HIST.VERS,
       TO_TIMESTAMP_NTZ(
                   TO_CHAR(SSP_RSLT_HIST.CRDATEI, 'YYYY-MM-DD') || ' ' || LPAD(SSP_RSLT_HIST.CRTIMEI, 6, '0'),
                   'YYYY-MM-DD HH24MISS')           CREATE_DTS,
       TO_TIMESTAMP_NTZ(
                   TO_CHAR(SSP_RSLT_HIST.LAST_MOD_DATEI, 'YYYY-MM-DD') || ' ' ||
                   LPAD(SSP_RSLT_HIST.LAST_MOD_TIMEI, 6, '0'),
                   'YYYY-MM-DD HH24MISS')           LAST_MODIFIED_DTS,
       'N'                                          IS_DELETED
FROM ODS.AMIS.S1SSP_RSLT_HIST SSP_RSLT_HIST
         JOIN ODS.AMIS.S1SPK_DET UN_SPK_DET
              ON UN_SPK_DET.SPK_NO = SSP_RSLT_HIST.SPK_NO
                  AND UN_SPK_DET.SPK_VER_NO = SSP_RSLT_HIST.SPK_VER_NO
         LEFT OUTER JOIN ODS.AMIS.S1STC_CODE RSLT_RSN_CODE
                         ON RSLT_RSN_CODE.CODE_ID = SSP_RSLT_HIST.RSLT_TYPE_CD
                             AND RSLT_RSN_CODE.CODE_TYPE = 'RSLT_CHG_RSN_CD'
         LEFT OUTER JOIN ODS.AMIS.S1RSL_DET RSL_DET
                         ON RSL_DET.RSLT_TYPE_CD = SSP_RSLT_HIST.RSLT_TYPE_CD
                             AND RSL_DET.GRADE_CD = SSP_RSLT_HIST.GRADE_CD
                             AND SSP_RSLT_HIST.EFFCT_DT >= RSL_DET.EFFCT_DT
                             AND SSP_RSLT_HIST.EFFCT_DT <=
                                 IFF(YEAR(RSL_DET.EXPIRY_DT) = 1900, TO_DATE('2050', 'YYYY'), RSL_DET.EXPIRY_DT)
         LEFT OUTER JOIN ODS.AMIS.S1STC_CODE MARKS_CODE
                         ON MARKS_CODE.CODE_ID = RSL_DET.MARKS_CD AND MARKS_CODE.CODE_TYPE = 'MARKS_CD'
         LEFT OUTER JOIN ODS.AMIS.S1STC_CODE GRADE_TYPE_CODE
                         ON GRADE_TYPE_CODE.CODE_ID = RSL_DET.GRADE_TYPE_CD AND
                            GRADE_TYPE_CODE.CODE_TYPE = 'GRADE_TYPE_CD'
         LEFT OUTER JOIN ODS.AMIS.S1STC_CODE PASS_FAIL_CODE
                         ON PASS_FAIL_CODE.CODE_ID = SSP_RSLT_HIST.PASS_FAIL_CD AND
                            PASS_FAIL_CODE.CODE_TYPE = 'PASS_FAIL_CD'
WHERE NOT EXISTS(
        SELECT NULL
        FROM (SELECT HUB_UNIT_ENROLMENT_RESULT_KEY,
                     HASH_MD5,
                     LOAD_DTS,
                     LEAD(LOAD_DTS)
                          OVER (PARTITION BY HUB_UNIT_ENROLMENT_RESULT_KEY ORDER BY LOAD_DTS ASC) EFFECTIVE_END_DTS
              FROM DATA_VAULT.CORE.SAT_UNIT_ENROLMENT_RESULT) S
        WHERE S.EFFECTIVE_END_DTS IS NULL
          AND S.HUB_UNIT_ENROLMENT_RESULT_KEY = MD5(
                    IFNULL(SSP_RSLT_HIST.SSP_NO, 0) || ',' ||
                    IFNULL(SSP_RSLT_HIST.SEQ_NO, 0)
            )
          AND S.HASH_MD5 = MD5(
                    IFNULL(SSP_RSLT_HIST.SSP_NO, 0) || ',' ||
                    IFNULL(SSP_RSLT_HIST.SEQ_NO, 0) || ',' ||
                    IFNULL(SSP_RSLT_HIST.STU_ID, '') || ',' ||
                    IFNULL(UN_SPK_DET.SPK_CD, '') || ',' ||
                    IFNULL(SSP_RSLT_HIST.SPK_NO, 0) || ',' ||
                    IFNULL(SSP_RSLT_HIST.SPK_VER_NO, 0) || ',' ||
                    IFNULL(SSP_RSLT_HIST.SSP_ATT_NO, 0) || ',' ||
                    IFNULL(SSP_RSLT_HIST.RSLT_TYPE_CD, '') || ',' ||
                    IFNULL(SSP_RSLT_HIST.EFFCT_DT, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
                    IFNULL(SSP_RSLT_HIST.GRADE_CD, '') || ',' ||
                    IFNULL(RSL_DET.GRADE_DESCRIPTION, '') || ',' ||
                    IFNULL(SSP_RSLT_HIST.GRADE_TYPE_CD, '') || ',' ||
                    IFNULL(GRADE_TYPE_CODE.CODE_DESCR, '') || ',' ||
                    IFNULL(SSP_RSLT_HIST.PASS_FAIL_CD, '') || ',' ||
                    IFNULL(PASS_FAIL_CODE.CODE_DESCR, '') || ',' ||
                    IFNULL(SSP_RSLT_HIST.MARK, 0) || ',' ||
                    IFNULL(SSP_RSLT_HIST.GRADE_EFFCT_DT, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
                    IFNULL(SSP_RSLT_HIST.RSLT_REASON_CD, '') || ',' ||
                    IFNULL(RSLT_RSN_CODE.CODE_DESCR, '') || ',' ||
                    IFNULL(RSL_DET.MARKS_CD, '') || ',' ||
                    IFNULL(MARKS_CODE.CODE_DESCR, '') || ',' ||
                    IFNULL(SSP_RSLT_HIST.VERIFIED_FG, '') || ',' ||
                    IFNULL(SSP_RSLT_HIST.RATIFIED_FG, '') || ',' ||
                    IFNULL(SSP_RSLT_HIST.CERTIFIED_FG, '') || ',' ||
                    IFNULL(SSP_RSLT_HIST.VERS, 0) || ',' ||
                    IFNULL(TO_TIMESTAMP_NTZ(
                                       TO_CHAR(SSP_RSLT_HIST.CRDATEI, 'YYYY-MM-DD') || ' ' ||
                                       LPAD(SSP_RSLT_HIST.CRTIMEI, 6, '0'),
                                       'YYYY-MM-DD HH24MISS'), TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
                    IFNULL(TO_TIMESTAMP_NTZ(
                                       TO_CHAR(SSP_RSLT_HIST.LAST_MOD_DATEI, 'YYYY-MM-DD') || ' ' ||
                                       LPAD(SSP_RSLT_HIST.LAST_MOD_TIMEI, 6, '0'),
                                       'YYYY-MM-DD HH24MISS'), TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
                    'N'
            )
    )
;
