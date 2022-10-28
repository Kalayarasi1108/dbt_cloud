INSERT INTO DATA_VAULT.CORE.LNK_UAC_APPLICANT_TO_SECONDARY_SCHOOL(LNK_UAC_APPLICANT_TO_SECONDARY_SCHOOL,
                                                                  HUB_UAC_APPLICANT_KEY, HUB_UAC_SECONDARY_SCHOOL_KEY,
                                                                  APPLICANT_REFNUM, SCHOOL_CODE, SCHOOL_STATE, YEAR,
                                                                  SOURCE, LOAD_DTS,
                                                                  ETL_JOB_ID)
WITH VW_ALL_QUAL AS (
    SELECT SOURCE,
           YEAR,
           REFNUM,
           QUALNUM,
           LVEL,
           ORIGIN,
           WHENCE,
           Year_,
           SCHEDULE,
           RANK                                                                          ATAR,
           CSTITLE,
           RANK()
                   OVER (PARTITION BY SOURCE, YEAR, REFNUM ORDER BY YEAR_ DESC, QUALNUM) ORD
    FROM ODS.UAC.VW_ALL_QUAL
    where TRIM(SCHEDULE) <> 'na'
      and LVEL like 'S%'
)

SELECT MD5(IFNULL(A.REFNUM, 0) || ',' ||
           IFNULL(B.CODE, '') || ',' ||
           IFNULL(B.STATE, '') || ',' ||
           A.YEAR || ',' ||
           A.SOURCE)                             LNK_UAC_APPLICANT_TO_SECONDARY_SCHOOL,
    MD5(IFNULL(A.REFNUM, 0) || ',' ||
        A.YEAR || ',' ||
        A.SOURCE)                             HUB_UAC_APPLICANT_KEY,
     MD5(IFNULL(B.CODE, '') || ',' ||
         IFNULL(A.YEAR, '') || ',' ||
         IFNULL(A.SOURCE, '') || ',' ||
         IFNULL(B.STATE, '')
         )                                     HUB_UAC_SECONDARY_SCHOOL_KEY,
    A.REFNUM                                  APPLICANT_REFNUM,
       B.CODE                                    SCHOOL_CODE,
       B.STATE                                   SCHOOL_STATE,
    A.YEAR,
    A.SOURCE,
    CURRENT_TIMESTAMP::TIMESTAMP_NTZ          LOAD_DTS,
    'SQL' || CURRENT_TIMESTAMP::TIMESTAMP_NTZ ETL_JOB_ID

FROM ODS.UAC.VW_ALL_APPLIC A
         JOIN VW_ALL_QUAL Q
              ON A.SOURCE = Q.SOURCE
                  AND A.REFNUM = Q.REFNUM
                  AND A.YEAR = Q.YEAR
                  AND Q.ORD = 1
     JOIN ODS.UAC.VW_ALL_SCHOOL B
          ON B.CODE = Q.WHENCE
              AND B.STATE = Q.ORIGIN
              AND B.SOURCE = Q.SOURCE
              AND B.YEAR = Q.YEAR
WHERE NOT EXISTS(
        SELECT NULL
        FROM DATA_VAULT.CORE.LNK_UAC_APPLICANT_TO_SECONDARY_SCHOOL L
        WHERE L.HUB_UAC_APPLICANT_KEY = MD5(IFNULL(A.REFNUM, 0) || ',' ||
                                            A.YEAR || ',' ||
                                            A.SOURCE)

          AND L.HUB_UAC_SECONDARY_SCHOOL_KEY = MD5(IFNULL(B.CODE, '') || ',' ||
                                                   IFNULL(A.YEAR, '') || ',' ||
                                                   IFNULL(A.SOURCE, '') || ',' ||
                                                   IFNULL(B.STATE, '')
            )
    );