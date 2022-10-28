-- INSERT
INSERT INTO DATA_VAULT.CORE.SAT_STUDYLINK_APPLICATION_COURSE_FILE (SAT_STUDYLINK_APPLICATION_COURSE_FILE_SK,
                                                                   HUB_STUDYLINK_APPLICATION_COURSE_KEY, SOURCE,
                                                                   LOAD_DTS, ETL_JOB_ID, HASH_MD5, APPLICATIONID,
                                                                   COURSE_CODE, COURSE_NAME, COURSE_TYPE, INTAKE_CODE,
                                                                   INTAKE_DESC, INTAKE_YEAR, FACULTY, CAMPUS, STUDY_MODE,
                                                                   STUDY_ONLINE, FILE_DATE,
                                                                   IS_DELETED)
SELECT DATA_VAULT.CORE.SEQ.NEXTVAL               SAT_STUDYLINK_APPLICATION_COURSE_FILE_SK,
       MD5(S.APPLICATIONID || ',' ||
           S.COURSE_CODE)                        HUB_STUDYLINK_APPLICATION_COURSE_KEY,
       'STUDYLINK'                               SOURCE,
       FILE_DATE                                 LOAD_DTS,
       'SQL' || CURRENT_TIMESTAMP::TIMESTAMP_NTZ ETL_JOB_ID,
       MD5(IFNULL(S.APPLICATIONID, '') || ',' ||
           IFNULL(S.COURSE_CODE, '') || ',' ||
           IFNULL(S.COURSE_NAME, '') || ',' ||
           IFNULL(S.COURSE_TYPE, '') || ',' ||
           IFNULL(S.INTAKE_CODE, '') || ',' ||
           IFNULL(S.INTAKE_DESC, '') || ',' ||
           IFNULL(S.INTAKE_YEAR, '') || ',' ||
           IFNULL(S.FACULTY, '') || ',' ||
           IFNULL(S.CAMPUS, '') || ',' ||
           IFNULL(CASE
           WHEN UPPER(S.CAMPUS) LIKE '%DISTANCE%' THEN 'DISTANCE'
           WHEN UPPER(S.CAMPUS) LIKE '%OVERSEAS CAMPUS%' THEN 'OFFSHORE'
           WHEN UPPER(S.CAMPUS) LIKE '%BEIJING%' THEN 'OFFSHORE'
           WHEN UPPER(S.CAMPUS) LIKE '%HONG KONG%' THEN 'OFFSHORE'
           WHEN UPPER(S.CAMPUS) LIKE '%DISTANCE%' THEN 'DISTANCE'
           WHEN UPPER(S.CAMPUS) LIKE '%OVERSEAS CAMPUS%' THEN 'OFFSHORE'
           WHEN UPPER(S.CAMPUS) LIKE '%BEIJING%' THEN 'OFFSHORE'
           WHEN UPPER(S.CAMPUS) LIKE '%HONG KONG%' THEN 'OFFSHORE'
           ELSE 'ONSHORE'
           END, '') || ',' ||
           IFNULL(CASE
           WHEN UPPER(S.INTAKE_CODE) LIKE '%ONLINE%' THEN 'ONLINE'
           WHEN UPPER(S.INTAKE_CODE) LIKE '%ONLINE%' THEN 'ONLINE'
           ELSE 'OFFLINE'
           END, '') || ',' ||
           IFNULL(s.FILE_DATE, TO_TIMESTAMP_NTZ('1900', 'YYYY')) || ',' ||
           'N'
           )                                     HASH_MD5,
       S.APPLICATIONID,
       S.COURSE_CODE,
       S.COURSE_NAME,
       S.COURSE_TYPE,
       S.INTAKE_CODE,
       S.INTAKE_DESC,
       S.INTAKE_YEAR,
       S.FACULTY,
       S.CAMPUS,
       CASE
           WHEN UPPER(S.CAMPUS) LIKE '%DISTANCE%' THEN 'DISTANCE'
           WHEN UPPER(S.CAMPUS) LIKE '%OVERSEAS CAMPUS%' THEN 'OFFSHORE'
           WHEN UPPER(S.CAMPUS) LIKE '%BEIJING%' THEN 'OFFSHORE'
           WHEN UPPER(S.CAMPUS) LIKE '%HONG KONG%' THEN 'OFFSHORE'
           WHEN UPPER(S.CAMPUS) LIKE '%DISTANCE%' THEN 'DISTANCE'
           WHEN UPPER(S.CAMPUS) LIKE '%OVERSEAS CAMPUS%' THEN 'OFFSHORE'
           WHEN UPPER(S.CAMPUS) LIKE '%BEIJING%' THEN 'OFFSHORE'
           WHEN UPPER(S.CAMPUS) LIKE '%HONG KONG%' THEN 'OFFSHORE'
           ELSE 'ONSHORE'
           END AS                                STUDY_MODE,
       CASE
           WHEN UPPER(S.INTAKE_CODE) LIKE '%ONLINE%' THEN 'ONLINE'
           WHEN UPPER(S.INTAKE_CODE) LIKE '%ONLINE%' THEN 'ONLINE'
           ELSE 'OFFLINE'
           END AS                                STUDY_ONLINE,
       S.FILE_DATE,
       'N'                                       IS_DELETED
FROM (
         SELECT A.APPLICATIONID,
                A.FILE_DATE,
                DECODE(R.SEQ, 0, A.DEG_S1_CODE,
                       1, A.FND_S1_CODE,
                       2, A.DIP_S1_CODE,
                       3, A.ENG_S1_CODE,
                       4, A.GCGD_S1_CODE,
                       5, A.SAE_S1_CODE)                              COURSE_CODE,
                DECODE(R.SEQ, 0, A.DEG_PROGRAM_NAME,
                       1, A.FND_PROGRAM_NAME,
                       2, A.DIP_PROGRAM_NAME,
                       3, A.ENG_PROGRAM_NAME,
                       4, A.GCGD_PROGRAM_NAME,
                       5, A.SAE_PROGRAM_NAME)                         COURSE_NAME,
                DECODE(R.SEQ, 0, A.DEG_COURSE_TYPE,
                       1, A.FND_COURSE_TYPE,
                       2, A.DIP_COURSE_TYPE,
                       3, A.ENG_COURSE_TYPE,
                       4, A.GCGD_COURSE_TYPE,
                       5, A.SAE_COURSE_TYPE)                          COURSE_TYPE,
                DECODE(R.SEQ, 0, A.DEG_INTAKE_CODE,
                       1, A.FND_INTAKE_CODE,
                       2, A.DIP_INTAKE_CODE,
                       3, A.ENG_INTAKE_CODE,
                       4, A.GCGD_INTAKE_CODE,
                       5, A.SAE_INTAKE_CODE)                          INTAKE_CODE,
                DECODE(R.SEQ, 0, A.DEG_INTAKE_DESC,
                       1, A.FND_INTAKE_DESC,
                       2, A.DIP_INTAKE_DESC,
                       3, A.ENG_INTAKE_DESC,
                       4, A.GCGD_INTAKE_DESC,
                       5, A.SAE_INTAKE_DESC)                          INTAKE_DESC,
                DECODE(R.SEQ, 0, REGEXP_SUBSTR(A.DEG_INTAKE_DESC, '\\d{4}'),
                       1, REGEXP_SUBSTR(A.FND_INTAKE_DESC, '\\d{4}'),
                       2, REGEXP_SUBSTR(A.DIP_INTAKE_DESC, '\\d{4}'),
                       3, REGEXP_SUBSTR(A.ENG_INTAKE_DESC, '\\d{4}'),
                       4, REGEXP_SUBSTR(A.GCGD_INTAKE_DESC, '\\d{4}'),
                       5, REGEXP_SUBSTR(A.SAE_INTAKE_DESC, '\\d{4}')) INTAKE_YEAR,
                DECODE(R.SEQ, 0, A.DEG_FACULTY,
                       1, A.FND_FACULTY,
                       2, A.DIP_FACULTY,
                       3, A.ENG_FACULTY,
                       4, A.GCGD_FACULTY,
                       5, A.SAE_FACULTY)                              FACULTY,
                DECODE(R.SEQ, 0, A.DEG_CAMPUS,
                       1, A.FND_CAMPUS,
                       2, A.DIP_CAMPUS,
                       3, A.ENG_CAMPUS,
                       4, A.GCGD_CAMPUS,
                       5, A.SAE_CAMPUS)                               CAMPUS
         FROM (
                  SELECT APPLICATIONID,
                         ENG_S1_CODE,
                         ENG_PROGRAM_NAME,
                         ENG_INTAKE_CODE,
                         ENG_INTAKE_DESC,
                         ENG_CAMPUS,
                         ENG_FACULTY,
                         ENG_COURSE_TYPE,
                         FND_S1_CODE,
                         FND_PROGRAM_NAME,
                         FND_INTAKE_CODE,
                         FND_INTAKE_DESC,
                         FND_CAMPUS,
                         FND_FACULTY,
                         FND_COURSE_TYPE,
                         DIP_S1_CODE,
                         DIP_PROGRAM_NAME,
                         DIP_INTAKE_CODE,
                         DIP_INTAKE_DESC,
                         DIP_CAMPUS,
                         DIP_FACULTY,
                         DIP_COURSE_TYPE,
                         GCGD_S1_CODE,
                         GCGD_PROGRAM_NAME,
                         GCGD_INTAKE_CODE,
                         GCGD_INTAKE_DESC,
                         GCGD_CAMPUS,
                         GCGD_FACULTY,
                         GCGD_COURSE_TYPE,
                         DEG_S1_CODE,
                         DEG_PROGRAM_NAME,
                         DEG_INTAKE_CODE,
                         DEG_INTAKE_DESC,
                         DEG_CAMPUS,
                         DEG_FACULTY,
                         DEG_COURSE_TYPE,
                         SAE_CAMPUS,
                         SAE_COURSE_TYPE,
                         SAE_FACULTY,
                         SAE_INTAKE_CODE,
                         SAE_INTAKE_DESC,
                         SAE_PROGRAM_NAME,
                         SAE_S1_CODE,
                         FILE_DATE
                  FROM (
                           SELECT APPLICATIONID,
                                  ENG_S1_CODE,
                                  ENG_PROGRAM_NAME,
                                  ENG_INTAKE_CODE,
                                  ENG_INTAKE_DESC,
                                  ENG_CAMPUS,
                                  ENG_FACULTY,
                                  ENG_COURSE_TYPE,
                                  FND_S1_CODE,
                                  FND_PROGRAM_NAME,
                                  FND_INTAKE_CODE,
                                  FND_INTAKE_DESC,
                                  FND_CAMPUS,
                                  FND_FACULTY,
                                  FND_COURSE_TYPE,
                                  DIP_S1_CODE,
                                  DIP_PROGRAM_NAME,
                                  DIP_INTAKE_CODE,
                                  DIP_INTAKE_DESC,
                                  DIP_CAMPUS,
                                  DIP_FACULTY,
                                  DIP_COURSE_TYPE,
                                  GCGD_S1_CODE,
                                  GCGD_PROGRAM_NAME,
                                  GCGD_INTAKE_CODE,
                                  GCGD_INTAKE_DESC,
                                  GCGD_CAMPUS,
                                  GCGD_FACULTY,
                                  GCGD_COURSE_TYPE,
                                  DEG_S1_CODE,
                                  DEG_PROGRAM_NAME,
                                  DEG_INTAKE_CODE,
                                  DEG_INTAKE_DESC,
                                  DEG_CAMPUS,
                                  DEG_FACULTY,
                                  DEG_COURSE_TYPE,
                                  SAE_CAMPUS,
                                  SAE_COURSE_TYPE,
                                  SAE_FACULTY,
                                  SAE_INTAKE_CODE,
                                  SAE_INTAKE_DESC,
                                  SAE_PROGRAM_NAME,
                                  SAE_S1_CODE,
                                  FILE_DATE,
                                  IFF(MD5_CHECKSUM = LAG(MD5_CHECKSUM) OVER (PARTITION BY APPLICATIONID ORDER BY FILE_DATE ASC), 'N',
           'Y')                                                                   HAS_CHANGE
                           FROM ODS.STUDYLINK.STUDYLINK_APPLICATION_CSV_EXTRACT
                      WHERE APP_STATUS_DESCRIPTION  IS NOT NULL
                       )
                  WHERE HAS_CHANGE='Y'
              ) A
                  JOIN (
             SELECT 0 SEQ
             UNION ALL
             SELECT 1
             UNION ALL
             SELECT 2
             UNION ALL
             SELECT 3
             UNION ALL
             SELECT 4
             UNION ALL
             SELECT 5
         ) R ON 1 = 1
         WHERE DECODE(R.SEQ, 0, DEG_S1_CODE,
                      1, FND_S1_CODE,
                      2, DIP_S1_CODE,
                      3, ENG_S1_CODE,
                      4, GCGD_S1_CODE,
                      5, SAE_S1_CODE) IS NOT NULL
     ) S
WHERE NOT EXISTS(
        SELECT NULL
        FROM DATA_VAULT.CORE.SAT_STUDYLINK_APPLICATION_COURSE_FILE FILE
        WHERE FILE.APPLICATIONID = S.APPLICATIONID
          AND FILE.COURSE_CODE = S.COURSE_CODE
          AND FILE.FILE_DATE = S.FILE_DATE
    )
;
