INSERT INTO DATA_VAULT.CORE.LNK_UAC_OFFER_TO_COURSE (LNK_UAC_OFFER_TO_COURSE_KEY, HUB_UAC_COURSE_KEY, HUB_UAC_OFFER_KEY,
                                                     REFNUM, ROUNDNUM, COURSE, YEAR, SOURCE, LOAD_DTS, ETL_JOB_ID)
SELECT MD5(IFNULL(B.REFNUM, 0) || ',' ||
           IFNULL(B.ROUNDNUM, 0) || ',' ||
           IFNULL(B.COURSE, 0) || ',' ||
           B.YEAR || ',' ||
           B.SOURCE) LNK_UAC_OFFER_TO_COURSE_KEY,
       MD5(IFNULL(A.COURSE, 0) || ',' ||
           A.YEAR || ',' ||
           A.SOURCE)                             HUB_UAC_COURSE_KEY,
       MD5(IFNULL(B.ROUNDNUM, 0) || ',' ||
           IFNULL(B.REFNUM, 0) || ',' ||
           IFNULL(B.COURSE, 0) || ',' ||
           B.YEAR || ',' ||
           B.SOURCE)                             HUB_UAC_OFFER_KEY,
       B.REFNUM,
       B.ROUNDNUM,
       B.COURSE,
       B.YEAR,
       B.SOURCE,
       CURRENT_TIMESTAMP::TIMESTAMP_NTZ          LOAD_DTS,
       'SQL' || CURRENT_TIMESTAMP::TIMESTAMP_NTZ ETL_JOB_ID
FROM ODS.UAC.VW_ALL_COURSE A
         JOIN ODS.UAC.VW_ALL_OFFER B
              ON A.SOURCE = B.SOURCE
                  AND A.COURSE = B.COURSE
                  AND A.YEAR = B.YEAR
                  AND NOT EXISTS(
                          SELECT NULL
                          FROM DATA_VAULT.CORE.LNK_UAC_OFFER_TO_COURSE L
                          WHERE L.HUB_UAC_OFFER_KEY = MD5(IFNULL(B.ROUNDNUM, 0) || ',' ||
                                                          IFNULL(B.REFNUM, 0) || ',' ||
                                                          IFNULL(B.COURSE, 0) || ',' ||
                                                          B.YEAR || ',' ||
                                                          B.SOURCE)

                            AND L.HUB_UAC_COURSE_KEY = MD5(IFNULL(A.COURSE, 0) || ',' ||
                                                           A.YEAR || ',' ||
                                                           A.SOURCE)
                      )
;
