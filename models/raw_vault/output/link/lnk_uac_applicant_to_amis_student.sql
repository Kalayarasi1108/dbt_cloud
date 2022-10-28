INSERT INTO DATA_VAULT.CORE.LNK_UAC_APPLICANT_TO_AMIS_STUDENT (LNK_UAC_APPLICANT_TO_AMIS_STUDENT_KEY,
                                                                              HUB_UAC_APPLICANT_KEY, HUB_STUDENT_KEY,
                                                                              REFNUM, YEAR, STU_ID, SOURCE, LOAD_DTS,
                                                                              ETL_JOB_ID)


WITH STU_APPLICATION AS (
    SELECT appl.*
    FROM ODS.AMIS.S1APP_APPLICATION as a
             INNER JOIN ODS.AMIS.S1APP_APPLICATION_LINE as l on a.application_id = l.application_id
             INNER JOIN ODS.AMIS.S1STU_APPLICATION as appl on appl.application_id = l.application_id
        and appl.application_line_id = l.application_line_id and appl.application_id != 0 and
                                                              appl.application_line_id != 0
             INNER JOIN ODS.AMIS.S1APP_STUDY as s on l.application_id = s.application_id
        and s.application_line_id = l.application_line_id
             LEFT JOIN ODS.AMIS.S1APP_OFFER as o on o.application_id = l.application_id
        and l.application_line_id = o.application_line_id
)


SELECT MD5(
                   IFNULL(L.REFNUM, 0) || ',' ||
                   L.YEAR || ',' ||
                   L.SOURCE || ',' ||
                   L.STU_ID)                     LNK_UAC_APPLICANT_TO_AMIS_STUDENT_KEY,
       MD5(
                   IFNULL(L.REFNUM, 0) || ',' ||
                   L.YEAR || ',' ||
                   L.SOURCE)                     HUB_UAC_APPLICANT_KEY,
       MD5(L.STU_ID)                             HUB_STUDENT_KEY,
       L.REFNUM                                  REFNUM,
       L.YEAR                                    YEAR,
       L.STU_ID                                  STU_ID,
       L.SOURCE                                  SOURCE,
       CURRENT_TIMESTAMP::timestamp_ntz          LOAD_DTS,
       'SQL' || CURRENT_TIMESTAMP::TIMESTAMP_NTZ ETL_JOB_ID
FROM (
         SELECT DISTINCT STU_APPLICATION.STU_ID,
                         UAC_OFFER.REFNUM::NUMBER REFNUM,
                         UAC_OFFER.YEAR,
                         UAC_OFFER.SOURCE
         FROM STU_APPLICATION
                  JOIN ODS.UAC.VW_ALL_OFFER UAC_OFFER
                       ON UAC_OFFER.COURSE = STU_APPLICATION.ADMSN_CNTR_CRS_CD
                           AND UAC_OFFER.REFNUM = STU_APPLICATION.ADMSN_CNTR_APP_ID
                  JOIN ODS.AMIS.S1STU_DET STU_DET
                       ON STU_DET.STU_ID = STU_APPLICATION.STU_ID
                           AND STU_DET.STU_CONSOL_FG = 'N'
     ) L
WHERE NOT EXISTS(
        SELECT 1
        FROM DATA_VAULT.CORE.LNK_UAC_APPLICANT_TO_AMIS_STUDENT S
        WHERE S.HUB_UAC_APPLICANT_KEY = MD5(IFNULL(L.REFNUM, 0) || ',' ||
                                            L.YEAR || ',' ||
                                            L.SOURCE)
          AND S.HUB_STUDENT_KEY = MD5(L.STU_ID)
    );