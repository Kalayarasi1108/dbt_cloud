INSERT INTO DATA_VAULT.CORE.LNK_UAC_PREFERENCE_TO_COURSE (LNK_UAC_PREFERENCE_TO_COURSE_KEY, HUB_UAC_PREFERENCE_KEY,
                                                          HUB_UAC_COURSE_KEY, REFNUM, SETNUM, PREFNUM, COURSE, YEAR,
                                                          SOURCE, LOAD_DTS, ETL_JOB_ID)
SELECT MD5(IFNULL(B.REFNUM, 0) || ',' ||
           IFNULL(B.SETNUM, 0) || ',' ||
           IFNULL(B.PREFNUM, 0) || ',' ||
           IFNULL(B.COURSE, 0) || ',' ||
           B.YEAR || ',' ||
           B.SOURCE) LNK_UAC_PREFERENCE_TO_COURSE_KEY,
       MD5(IFNULL(B.PREFNUM, 0) || ',' ||
           IFNULL(B.SETNUM, 0) || ',' ||
           IFNULL(B.REFNUM, 0) || ',' ||
           IFNULL(B.COURSE, 0) || ',' ||
           B.YEAR || ',' ||
           B.SOURCE)                             HUB_UAC_PREFERENCE_KEY,
       MD5(IFNULL(A.COURSE, 0) || ',' ||
           A.YEAR || ',' ||
           A.SOURCE)                             HUB_UAC_COURSE_KEY,
       B.REFNUM,
       B.SETNUM,
       B.PREFNUM,
       B.COURSE,
       B.YEAR,
       B.SOURCE,
       CURRENT_TIMESTAMP::TIMESTAMP_NTZ          LOAD_DTS,
       'SQL' || CURRENT_TIMESTAMP::TIMESTAMP_NTZ ETL_JOB_ID
FROM ODS.UAC.VW_ALL_COURSE A
         JOIN ODS.UAC.VW_ALL_PREF B
              ON A.SOURCE = b.SOURCE
                  AND a.COURSE = b.COURSE
                  AND A.YEAR = B.YEAR
                  AND NOT EXISTS(
                          SELECT NULL
                          FROM DATA_VAULT.CORE.LNK_UAC_PREFERENCE_TO_COURSE L
                          WHERE L.HUB_UAC_PREFERENCE_KEY = MD5(IFNULL(B.PREFNUM, 0) || ',' ||
                                                                IFNULL(B.SETNUM, 0) || ',' ||
                                                                IFNULL(B.REFNUM, 0) || ',' ||
                                                                IFNULL(B.COURSE, 0) || ',' ||
                                                                B.YEAR || ',' ||
                                                                B.SOURCE)

                            AND L.HUB_UAC_COURSE_KEY = MD5(IFNULL(A.COURSE, 0) || ',' ||
                                                           A.YEAR || ',' ||
                                                           A.SOURCE)
                      )
;