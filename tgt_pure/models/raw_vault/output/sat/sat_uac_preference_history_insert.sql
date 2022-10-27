-- INSERT
INSERT INTO DATA_VAULT.CORE.SAT_UAC_PREFERENCE_HISTORY (SAT_UAC_PREFERENCE_HISTORY_SK,
                                                        HUB_UAC_PREFERENCE_HISTORY_KEY,
                                                        SOURCE,
                                                        LOAD_DTS,
                                                        ETL_JOB_ID,
                                                        HASH_MD5,
                                                        YEAR,
                                                        STAT_DATE,
                                                        STAT_GRP,
                                                        PREFERENCE_NUMBER,
                                                        COURSE,
                                                        PREFERENCE_COUNT,
                                                        INSTITUTION_CODE,
                                                        COURSE_TITLE,
                                                        COURSE_DESC,
                                                        INSTITUTION_SHORT_NAME,
                                                        INSTITUTION_LONG_NAME,
                                                        IS_DELETED)
SELECT DATA_VAULT.CORE.SEQ.nextval               SAT_UAC_PREFERENCE_HISTORY_SK,
       HUB_UAC_PREFERENCE_HISTORY_KEY,
       S.SOURCE,
       CURRENT_TIMESTAMP::TIMESTAMP_NTZ          LOAD_DTS,
       'SQL' || CURRENT_TIMESTAMP::TIMESTAMP_NTZ ETL_JOB_ID,
       MD5(
                   IFNULL(S.YEAR, '') || ',' ||
                   IFNULL(S.STAT_DATE::STRING, '') || ',' ||
                   IFNULL(S.STAT_GRP, '') || ',' ||
                   IFNULL(S.PREFERENCE_NUMBER, '') || ',' ||
                   IFNULL(S.COURSE, '') || ',' ||
                   IFNULL(S.PREFERENCE_COUNT, 0) || ',' ||
                   IFNULL(S.INSTITUTION_CODE, '') || ',' ||
                   IFNULL(S.COURSE_TITLE, '') || ',' ||
                   IFNULL(S.COURSE_DESC, '') || ',' ||
                   IFNULL(S.INSTITUTION_SHORT_NAME, '') || ',' ||
                   IFNULL(S.INSTITUTION_LONG_NAME, '') || ',' ||
                   IFNULL(S.IS_DELETED, '')
           )                                     HASH_MD5,
       S.YEAR,
       S.STAT_DATE,
       S.STAT_GRP,
       S.PREFERENCE_NUMBER,
       S.COURSE,
       S.PREFERENCE_COUNT,
       S.INSTITUTION_CODE,
       S.COURSE_TITLE,
       S.COURSE_DESC,
       S.INSTITUTION_SHORT_NAME,
       S.INSTITUTION_LONG_NAME,
       S.IS_DELETED
FROM (SELECT MD5(
                         IFNULL(TO_CHAR(GENSTAT.STATDATE::DATE, 'YYYY-MM-DD'), '') || ',' ||
                         IFNULL(GENSTAT.STATGRP, '') || ',' ||
                         IFNULL(GENSTAT.PREFNUM, '') || ',' ||
                         IFNULL(GENSTAT.COURSE, '') || ',' ||
                         GENSTAT.YEAR || ',' ||
                         GENSTAT.SOURCE) HUB_UAC_PREFERENCE_HISTORY_KEY,
             GENSTAT.YEAR,
             GENSTAT.SOURCE,
             GENSTAT.STATDATE::DATE      STAT_DATE,
             GENSTAT.STATGRP             STAT_GRP,
             GENSTAT.COURSE,
             GENSTAT.PREFNUM             PREFERENCE_NUMBER,
             GENSTAT.PREFCNT             PREFERENCE_COUNT,
             COURSE.INSTCODE             INSTITUTION_CODE,
             COURSE.CSTITLE              COURSE_TITLE,
             COURSE.CSDESC               COURSE_DESC,
             INST.SHORTNAM               INSTITUTION_SHORT_NAME,
             INST.LONGNAM                INSTITUTION_LONG_NAME,
             'N'                         IS_DELETED
      FROM ODS.UAC.VW_ALL_GENSTAT1 GENSTAT
               LEFT OUTER JOIN ODS.UAC.VW_ALL_COURSE COURSE
                               ON COURSE.YEAR = GENSTAT.YEAR
                                   AND COURSE.SOURCE = GENSTAT.SOURCE
                                   AND COURSE.COURSE = GENSTAT.COURSE
               LEFT OUTER JOIN ODS.UAC.VW_ALL_INST INST
                               ON INST.YEAR = GENSTAT.YEAR
                                   AND INST.SOURCE = GENSTAT.SOURCE
                                   AND INST.CODE = COURSE.INSTCODE
     ) S
WHERE NOT EXISTS(
        SELECT NULL
        FROM (SELECT HUB_UAC_PREFERENCE_HISTORY_KEY,
                     HASH_MD5,
                     LOAD_DTS,
                     LEAD(LOAD_DTS)
                          OVER (PARTITION BY HUB_UAC_PREFERENCE_HISTORY_KEY ORDER BY LOAD_DTS ASC) EFFECTIVE_END_DTS
              FROM DATA_VAULT.CORE.SAT_UAC_PREFERENCE_HISTORY) SAT
        WHERE SAT.EFFECTIVE_END_DTS IS NULL
          AND SAT.HUB_UAC_PREFERENCE_HISTORY_KEY = S.HUB_UAC_PREFERENCE_HISTORY_KEY
          AND SAT.HASH_MD5 = MD5(
                    IFNULL(S.YEAR, '') || ',' ||
                    IFNULL(S.STAT_DATE::STRING, '') || ',' ||
                    IFNULL(S.STAT_GRP, '') || ',' ||
                    IFNULL(S.PREFERENCE_NUMBER, '') || ',' ||
                    IFNULL(S.COURSE, '') || ',' ||
                    IFNULL(S.PREFERENCE_COUNT, 0) || ',' ||
                    IFNULL(S.INSTITUTION_CODE, '') || ',' ||
                    IFNULL(S.COURSE_TITLE, '') || ',' ||
                    IFNULL(S.COURSE_DESC, '') || ',' ||
                    IFNULL(S.INSTITUTION_SHORT_NAME, '') || ',' ||
                    IFNULL(S.INSTITUTION_LONG_NAME, '') || ',' ||
                    IFNULL(S.IS_DELETED, '')
            )
    )
;

