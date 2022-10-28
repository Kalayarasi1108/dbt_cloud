INSERT INTO DATA_VAULT.CORE.LNK_UAC_APPLICANT_TO_QUALIFICATION(LNK_UAC_APPLICANT_TO_QUALIFICATION_KEY,
                                                               HUB_UAC_APPLICANT_KEY, HUB_UAC_QUALIFICATION_KEY, REFNUM,
                                                               QUALNUM, YEAR, SOURCE, LOAD_DTS, ETL_JOB_ID)
SELECT MD5(IFNULL(A.REFNUM, 0) || ',' ||
           IFNULL(B.QUALNUM, 0) || ',' ||
           A.YEAR || ',' ||
           A.SOURCE)                             LNK_UAC_APPLICANT_TO_QUALIFICATION_KEY,
       MD5(IFNULL(A.REFNUM, 0) || ',' ||
           A.YEAR || ',' ||
           A.SOURCE)                             HUB_UAC_APPLICANT_KEY,
       MD5(IFNULL(B.REFNUM, 0) || ',' ||
           IFNULL(B.QUALNUM, 0) || ',' ||
           B.YEAR || ',' ||
           B.SOURCE)                             HUB_UAC_QUALIFICATION_KEY,
       B.REFNUM,
       B.QUALNUM,
       B.YEAR,
       B.SOURCE,
       CURRENT_TIMESTAMP::TIMESTAMP_NTZ          LOAD_DTS,
       'SQL' || CURRENT_TIMESTAMP::TIMESTAMP_NTZ ETL_JOB_ID
FROM ODS.UAC.VW_ALL_APPLIC A
         JOIN ODS.UAC.VW_ALL_QUAL B
              ON A.SOURCE = b.SOURCE
                  AND A.REFNUM = B.REFNUM
                  AND A.YEAR = B.YEAR
                  AND NOT EXISTS(
                          SELECT NULL
                          FROM DATA_VAULT.CORE.LNK_UAC_APPLICANT_TO_QUALIFICATION L
                          WHERE L.HUB_UAC_QUALIFICATION_KEY = MD5(IFNULL(B.REFNUM, 0) || ',' ||
                                                                  IFNULL(B.QUALNUM, 0) || ',' ||
                                                                  B.YEAR || ',' ||
                                                                  B.SOURCE)

                            AND L.HUB_UAC_APPLICANT_KEY = MD5(IFNULL(A.REFNUM, 0) || ',' ||
                                                              A.YEAR || ',' ||
                                                              A.SOURCE)
                      );
