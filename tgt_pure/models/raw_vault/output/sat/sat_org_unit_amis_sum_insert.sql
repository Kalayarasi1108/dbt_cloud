-- INSERT
INSERT INTO DATA_VAULT.CORE.SAT_ORG_UNIT_AMIS_SUM(SAT_ORG_UNIT_AMIS_SUM_SK, HUB_ORG_UNIT_KEY, SOURCE, LOAD_DTS,
                                                  ETL_JOB_ID, HASH_MD5, ORG_UNIT_CD, EFFECTIVE_DATE, EXPIRY_DATE,
                                                  ORG_UNIT_NAME, ORG_UNIT_TYPE_CD, ORG_UNIT_TYPE,
                                                  DEPARTMENT_ORG_UNIT_CD, DEPARTMENT_ORG_UNIT_NAME,
                                                  DEPARTMENT_ORG_UNIT_SHORT_NAME, DEPARTMENT_ORG_UNIT_TYPE,
                                                  FACULTY_ORG_UNIT_CD, FACULT_ORG_UNIT_NAME, FACULT_ORG_UNIT_SHORT_NAME,
                                                  FACULT_ORG_UNIT_TYPE, REPORT_FACULTY_ORG_UNIT_CD,
                                                  REPORT_FACULT_ORG_UNIT_NAME, REPORT_FACULT_ORG_UNIT_SHORT_NAME,
                                                  REPORT_FACULT_ORG_UNIT_TYPE, IS_DELETED)
SELECT DATA_VAULT.CORE.SEQ.NEXTVAL               SAT_ORG_UNIT_AMIS_SUM_SK,
       MD5(IFNULL(S.ORG_UNIT_CD, '')
           )                                     HUB_ORG_UNIT_KEY,
       'AMIS'                                    SOURCE,
       S.LOAD_DTS,
       'SQL' || CURRENT_TIMESTAMP::TIMESTAMP_NTZ ETL_JOB_ID,
       MD5(IFNULL(S.ORG_UNIT_CD, '') || ',' ||
           IFNULL(TO_CHAR(S.EFFECTIVE_DATE, 'YYYY-MM-DD'), '') || ',' ||
           IFNULL(TO_CHAR(S.EXPIRY_DATE, 'YYYY-MM-DD'), '') || ',' ||
           IFNULL(S.ORG_UNIT_NAME, '') || ',' ||
           IFNULL(S.ORG_UNIT_TYPE_CD, '') || ',' ||
           IFNULL(S.ORG_UNIT_TYPE, '') || ',' ||
           IFNULL(S.DEPARTMENT_ORG_UNIT_CD, '') || ',' ||
           IFNULL(S.DEPARTMENT_ORG_UNIT_NAME, '') || ',' ||
           IFNULL(S.DEPARTMENT_ORG_UNIT_SHORT_NAME, '') || ',' ||
           IFNULL(S.DEPARTMENT_ORG_UNIT_TYPE, '') || ',' ||
           IFNULL(S.FACULTY_ORG_UNIT_CD, '') || ',' ||
           IFNULL(S.FACULT_ORG_UNIT_NAME, '') || ',' ||
           IFNULL(S.FACULT_ORG_UNIT_SHORT_NAME, '') || ',' ||
           IFNULL(S.FACULT_ORG_UNIT_TYPE, '') || ',' ||
           IFNULL(S.REPORT_FACULTY_ORG_UNIT_CD, '') || ',' ||
           IFNULL(S.REPORT_FACULT_ORG_UNIT_NAME, '') || ',' ||
           IFNULL(S.REPORT_FACULT_ORG_UNIT_SHORT_NAME, '') || ',' ||
           IFNULL(S.REPORT_FACULT_ORG_UNIT_TYPE, '') || ',' ||
           IFNULL(S.IS_DELETED, '')
           )                                     HASH_MD5,
       S.ORG_UNIT_CD,
       S.EFFECTIVE_DATE,
       IFF(LEAD(S.EFFECTIVE_DATE) OVER (PARTITION BY S.ORG_UNIT_CD ORDER BY S.EFFECTIVE_DATE) < DATEADD(DAY,1, S.EXPIRY_DATE), DATEADD(DAY, -1, LEAD(S.EFFECTIVE_DATE) OVER (PARTITION BY S.ORG_UNIT_CD ORDER BY S.EFFECTIVE_DATE)), S.EXPIRY_DATE) EXPIRY_DATE,
       --S.EXPIRY_DATE,
       S.ORG_UNIT_NAME,
       S.ORG_UNIT_TYPE_CD,
       S.ORG_UNIT_TYPE,
       S.DEPARTMENT_ORG_UNIT_CD,
       S.DEPARTMENT_ORG_UNIT_NAME,
       S.DEPARTMENT_ORG_UNIT_SHORT_NAME,
       S.DEPARTMENT_ORG_UNIT_TYPE,
       S.FACULTY_ORG_UNIT_CD,
       S.FACULT_ORG_UNIT_NAME,
       S.FACULT_ORG_UNIT_SHORT_NAME,
       S.FACULT_ORG_UNIT_TYPE,
       S.REPORT_FACULTY_ORG_UNIT_CD,
       S.REPORT_FACULT_ORG_UNIT_NAME,
       S.REPORT_FACULT_ORG_UNIT_SHORT_NAME,
       S.REPORT_FACULT_ORG_UNIT_TYPE,
       S.IS_DELETED
FROM (SELECT ORG_UNIT_CD_1                                    ORG_UNIT_CD,
             GREATEST(EFFCT_DT_1, IFNULL(EFFCT_DT_2, '1900-01-01'::date),
                      IFNULL(EFFCT_DT_3, '1900-01-01'::date)) LOAD_DTS,
             EFFCT_DT_1                                       EFFECTIVE_DATE,
             EXPIRY_DT_1                                      EXPIRY_DATE,
             ORG_UNIT_NM_1                                    ORG_UNIT_NAME,
             ORG_UNIT_TYPE_CD_1                               ORG_UNIT_TYPE_CD,
             ORG_UNIT_TYPE_1                                  ORG_UNIT_TYPE,
             CASE
                 WHEN ORG_UNIT_TYPE_CD_1 in ('DEP', 'ADM') AND RIGHT(ORG_UNIT_CD_1, 1) = '0'
                     THEN ORG_UNIT_CD_1
                 WHEN ORG_UNIT_TYPE_CD_2 in ('DEP', 'ADM') AND RIGHT(ORG_UNIT_CD_2, 1) = '0'
                     THEN ORG_UNIT_CD_2
                 WHEN ORG_UNIT_TYPE_CD_3 in ('DEP', 'ADM') AND RIGHT(ORG_UNIT_CD_3, 1) = '0'
                     THEN ORG_UNIT_CD_3
                 ELSE NULL
                 END                                          DEPARTMENT_ORG_UNIT_CD,
             CASE
                 WHEN ORG_UNIT_TYPE_CD_1 in ('DEP', 'ADM') AND RIGHT(ORG_UNIT_CD_1, 1) = '0'
                     THEN ORG_UNIT_NM_1
                 WHEN ORG_UNIT_TYPE_CD_2 in ('DEP', 'ADM') AND RIGHT(ORG_UNIT_CD_2, 1) = '0'
                     THEN ORG_UNIT_NM_2
                 WHEN ORG_UNIT_TYPE_CD_3 in ('DEP', 'ADM') AND RIGHT(ORG_UNIT_CD_3, 1) = '0'
                     THEN ORG_UNIT_NM_3
                 ELSE NULL
                 END                                          DEPARTMENT_ORG_UNIT_NAME,
             CASE
                 WHEN ORG_UNIT_TYPE_CD_1 in ('DEP', 'ADM') AND RIGHT(ORG_UNIT_CD_1, 1) = '0'
                     THEN ORG_UNIT_SHORT_NM_1
                 WHEN ORG_UNIT_TYPE_CD_2 in ('DEP', 'ADM') AND RIGHT(ORG_UNIT_CD_2, 1) = '0'
                     THEN ORG_UNIT_SHORT_NM_3
                 WHEN ORG_UNIT_TYPE_CD_3 in ('DEP', 'ADM') AND RIGHT(ORG_UNIT_CD_3, 1) = '0'
                     THEN ORG_UNIT_SHORT_NM_3
                 ELSE NULL
                 END                                          DEPARTMENT_ORG_UNIT_SHORT_NAME,
             CASE
                 WHEN ORG_UNIT_TYPE_CD_1 in ('DEP', 'ADM') AND RIGHT(ORG_UNIT_CD_1, 1) = '0'
                     THEN ORG_UNIT_TYPE_1
                 WHEN ORG_UNIT_TYPE_CD_2 in ('DEP', 'ADM') AND RIGHT(ORG_UNIT_CD_2, 1) = '0'
                     THEN ORG_UNIT_TYPE_2
                 WHEN ORG_UNIT_TYPE_CD_3 in ('DEP', 'ADM') AND RIGHT(ORG_UNIT_CD_3, 1) = '0'
                     THEN ORG_UNIT_TYPE_3
                 ELSE NULL
                 END                                          DEPARTMENT_ORG_UNIT_TYPE,
             CASE
                 WHEN ORG_UNIT_TYPE_CD_1 = 'FAC' OR ORG_UNIT_CD_1 = '8011'
                     THEN ORG_UNIT_CD_1
                 WHEN ORG_UNIT_TYPE_CD_2 = 'FAC' OR ORG_UNIT_CD_2 = '8011'
                     THEN ORG_UNIT_CD_2
                 WHEN ORG_UNIT_TYPE_CD_3 = 'FAC' OR ORG_UNIT_CD_3 = '8011'
                     THEN ORG_UNIT_CD_3
                 ELSE NULL
                 END                                          FACULTY_ORG_UNIT_CD,
             CASE
                 WHEN ORG_UNIT_TYPE_CD_1 = 'FAC' OR ORG_UNIT_CD_1 = '8011'
                     THEN ORG_UNIT_NM_1
                 WHEN ORG_UNIT_TYPE_CD_2 = 'FAC' OR ORG_UNIT_CD_2 = '8011'
                     THEN ORG_UNIT_NM_2
                 WHEN ORG_UNIT_TYPE_CD_3 = 'FAC' OR ORG_UNIT_CD_3 = '8011'
                     THEN ORG_UNIT_NM_3
                 ELSE NULL
                 END                                          FACULT_ORG_UNIT_NAME,
             CASE
                 WHEN ORG_UNIT_TYPE_CD_1 = 'FAC' OR ORG_UNIT_CD_1 = '8011'
                     THEN ORG_UNIT_SHORT_NM_1
                 WHEN ORG_UNIT_TYPE_CD_2 = 'FAC' OR ORG_UNIT_CD_2 = '8011'
                     THEN ORG_UNIT_SHORT_NM_2
                 WHEN ORG_UNIT_TYPE_CD_3 = 'FAC' OR ORG_UNIT_CD_3 = '8011'
                     THEN ORG_UNIT_SHORT_NM_3
                 ELSE NULL
                 END                                          FACULT_ORG_UNIT_SHORT_NAME,
             CASE
                 WHEN ORG_UNIT_TYPE_CD_1 = 'FAC' OR ORG_UNIT_CD_1 = '8011'
                     THEN ORG_UNIT_TYPE_1
                 WHEN ORG_UNIT_TYPE_CD_2 = 'FAC' OR ORG_UNIT_CD_2 = '8011'
                     THEN ORG_UNIT_TYPE_2
                 WHEN ORG_UNIT_TYPE_CD_3 = 'FAC' OR ORG_UNIT_CD_3 = '8011'
                     THEN ORG_UNIT_TYPE_3
                 ELSE NULL
                 END                                          FACULT_ORG_UNIT_TYPE,
             CASE
                 WHEN ORG_UNIT_TYPE_CD_1 = 'FAC' OR ORG_UNIT_CD_1 = '8011'
                     THEN IFF(ORG_UNIT_CD_1 = '8011', '9011', ORG_UNIT_CD_1)
                 WHEN ORG_UNIT_TYPE_CD_2 = 'FAC' OR ORG_UNIT_CD_2 = '8011'
                     THEN IFF(ORG_UNIT_CD_2 = '8011', '9011', ORG_UNIT_CD_2)
                 WHEN ORG_UNIT_TYPE_CD_3 = 'FAC' OR ORG_UNIT_CD_3 = '8011'
                     THEN IFF(ORG_UNIT_CD_3 = '8011', '9011', ORG_UNIT_CD_3)
                 WHEN ORG_UNIT_CD_1 LIKE '9%'
                     THEN ORG_UNIT_CD
                 ELSE NULL
                 END                                          REPORT_FACULTY_ORG_UNIT_CD,
             CASE
                 WHEN ORG_UNIT_TYPE_CD_1 = 'FAC' OR ORG_UNIT_CD_1 = '8011'
                     THEN IFF(ORG_UNIT_CD_1 = '8011', 'Macquarie University', ORG_UNIT_NM_1)
                 WHEN ORG_UNIT_TYPE_CD_2 = 'FAC' OR ORG_UNIT_CD_2 = '8011'
                     THEN IFF(ORG_UNIT_CD_2 = '8011', 'Macquarie University', ORG_UNIT_NM_2)
                 WHEN ORG_UNIT_TYPE_CD_3 = 'FAC' OR ORG_UNIT_CD_3 = '8011'
                     THEN IFF(ORG_UNIT_CD_3 = '8011', 'Macquarie University', ORG_UNIT_NM_3)
                 WHEN ORG_UNIT_CD_1 LIKE '9%'
                     THEN 'Macquarie University - Other'
                 ELSE NULL
                 END                                          REPORT_FACULT_ORG_UNIT_NAME,
             CASE
                 WHEN ORG_UNIT_TYPE_CD_1 = 'FAC' OR ORG_UNIT_CD_1 = '8011'
                     THEN IFF(ORG_UNIT_CD_1 = '8011', 'MQ', ORG_UNIT_SHORT_NM_1)
                 WHEN ORG_UNIT_TYPE_CD_2 = 'FAC' OR ORG_UNIT_CD_2 = '8011'
                     THEN IFF(ORG_UNIT_CD_2 = '8011', 'MQ', ORG_UNIT_SHORT_NM_2)
                 WHEN ORG_UNIT_TYPE_CD_3 = 'FAC' OR ORG_UNIT_CD_3 = '8011'
                     THEN IFF(ORG_UNIT_CD_3 = '8011', 'MQ', ORG_UNIT_SHORT_NM_3)
                 WHEN ORG_UNIT_CD_1 LIKE '9%'
                     THEN 'MQ - Other'
                 ELSE NULL
                 END                                          REPORT_FACULT_ORG_UNIT_SHORT_NAME,
             CASE
                 WHEN ORG_UNIT_TYPE_CD_1 = 'FAC' OR ORG_UNIT_CD_1 = '8011'
                     THEN IFF(ORG_UNIT_CD_1 = '8011', 'Organisation', ORG_UNIT_TYPE_1)
                 WHEN ORG_UNIT_TYPE_CD_2 = 'FAC' OR ORG_UNIT_CD_2 = '8011'
                     THEN IFF(ORG_UNIT_CD_2 = '8011', 'Organisation', ORG_UNIT_TYPE_2)
                 WHEN ORG_UNIT_TYPE_CD_3 = 'FAC' OR ORG_UNIT_CD_3 = '8011'
                     THEN IFF(ORG_UNIT_CD_3 = '8011', 'Organisation', ORG_UNIT_TYPE_3)
                 WHEN ORG_UNIT_CD_1 LIKE '9%'
                     THEN ORG_UNIT_TYPE_1
                 ELSE NULL
                 END                                          REPORT_FACULT_ORG_UNIT_TYPE,
             'N'                                              IS_DELETED
      FROM (
               WITH ORG_UNIT AS (SELECT ORG_UNIT_CD,
                                        min_effct_dt            EFFCT_DT,
                                        max_converted_expiry_dt EXPIRY_DT,
                                        ORG_UNIT_NM,
                                        ORG_UNIT_SHORT_NM,
                                        ORG_UNIT_TYPE_CD,
                                        PARENT_ORG_UNIT_CD,
                                        ORG_UNIT_TYPE
                                 FROM (SELECT ORG_UNIT_CD,
                                              EFFCT_DT,
                                              EXPIRY_DT,
                                              CONVERTED_EXPIRY_DT,
                                              ORG_UNIT_NM,
                                              ORG_UNIT_SHORT_NM,
                                              ORG_UNIT_TYPE_CD,
                                              PARENT_ORG_UNIT_CD,
                                              ORG_UNIT_TYPE,
                                              grp_start,
                                              grp,
                                              min(EFFCT_DT)
                                                  over (partition by ORG_UNIT_CD, ORG_UNIT_NM, ORG_UNIT_SHORT_NM, ORG_UNIT_TYPE_CD, ORG_UNIT_TYPE,PARENT_ORG_UNIT_CD, grp) min_effct_dt,
                                              max(CONVERTED_EXPIRY_DT)
                                                  over (partition by ORG_UNIT_CD, ORG_UNIT_NM, ORG_UNIT_SHORT_NM, ORG_UNIT_TYPE_CD, ORG_UNIT_TYPE,PARENT_ORG_UNIT_CD, grp) max_converted_expiry_dt,
                                              ROW_NUMBER()
                                                      over (partition by ORG_UNIT_CD, ORG_UNIT_NM, ORG_UNIT_SHORT_NM, ORG_UNIT_TYPE_CD,PARENT_ORG_UNIT_CD,
                                                          ORG_UNIT_TYPE, grp
                                                          order by
                                                              EFFCT_DT ASC, CONVERTED_EXPIRY_DT)                                                                           rn
                                       FROM (SELECT ORG_UNIT_CD,
                                                    EFFCT_DT,
                                                    EXPIRY_DT,
                                                    CONVERTED_EXPIRY_DT,
                                                    ORG_UNIT_NM,
                                                    ORG_UNIT_SHORT_NM,
                                                    ORG_UNIT_TYPE_CD,
                                                    PARENT_ORG_UNIT_CD,
                                                    ORG_UNIT_TYPE,
                                                    grp_start,
                                                    sum(grp_start) over (
                                                        partition by ORG_UNIT_CD, ORG_UNIT_NM, ORG_UNIT_SHORT_NM, ORG_UNIT_TYPE_CD, PARENT_ORG_UNIT_CD,
                                                            ORG_UNIT_TYPE order by EFFCT_DT ASC, CONVERTED_EXPIRY_DT ASC
                                                        ) grp
                                             FROM (SELECT ORG.ORG_UNIT_CD,
                                                          ORG.EFFCT_DT,
                                                          ORG.EXPIRY_DT,
                                                          IFF(ORG.EXPIRY_DT = TO_DATE('1900-01-01', 'YYYY-MM-DD'),
                                                              TO_DATE('2050', 'YYYY'),
                                                              ORG.EXPIRY_DT)                CONVERTED_EXPIRY_DT,
                                                          ORG.ORG_UNIT_NM,
                                                          ORG.ORG_UNIT_SHORT_NM,
                                                          ORG.ORG_UNIT_TYPE_CD,
                                                          IFF(TRIM(ORG.PARENT_ORG_UNIT_CD) = '', NULL,
                                                              TRIM(ORG.PARENT_ORG_UNIT_CD)) PARENT_ORG_UNIT_CD,
                                                          ORG_TYPE_CODE.CODE_DESCR          ORG_UNIT_TYPE,
                                                          case
                                                              when DATEADD(DAY, -1, ORG.EFFCT_DT) > max(
                                                                      IFF(
                                                                              ORG.EXPIRY_DT = TO_DATE('1900-01-01', 'YYYY-MM-DD'),
                                                                              TO_DATE('2050', 'YYYY'),
                                                                              ORG.EXPIRY_DT)) over (
                                                                          partition by ORG.ORG_UNIT_CD, ORG.ORG_UNIT_NM, ORG.ORG_UNIT_SHORT_NM, ORG.ORG_UNIT_TYPE_CD, IFF(
                                                                                  TRIM(ORG.PARENT_ORG_UNIT_CD) = '',
                                                                                  NULL,
                                                                                  TRIM(ORG.PARENT_ORG_UNIT_CD)) ,
                                                                              ORG_TYPE_CODE.CODE_DESCR order by ORG.EFFCT_DT ASC, IFF(
                                                                                  ORG.EXPIRY_DT = TO_DATE('1900-01-01', 'YYYY-MM-DD'),
                                                                                  TO_DATE('2050', 'YYYY'),
                                                                                  ORG.EXPIRY_DT) ASC
                                                                          rows between unbounded preceding and 1 preceding
                                                                          )
                                                                  then 1
                                                              else 0
                                                              end                           grp_start
                                                   FROM ODS.AMIS.S1ORG_UNIT ORG
                                                            JOIN ODS.AMIS.S1STC_CODE ORG_TYPE_CODE
                                                                 ON ORG_TYPE_CODE.CODE_ID = ORG.ORG_UNIT_TYPE_CD
                                                                     AND ORG_TYPE_CODE.CODE_TYPE = 'ORG_TYPE_CD'
                                                      -- WHERE ORG_UNIT_CD = '5030'
                                                  ) A) B) C
                                 WHERE C.RN = 1)
               SELECT ORG_UNIT_1.ORG_UNIT_CD       ORG_UNIT_CD_1,
                      ORG_UNIT_1.EFFCT_DT          EFFCT_DT_1,
                      ORG_UNIT_1.EXPIRY_DT         EXPIRY_DT_1,
                      ORG_UNIT_1.ORG_UNIT_NM       ORG_UNIT_NM_1,
                      ORG_UNIT_1.ORG_UNIT_SHORT_NM ORG_UNIT_SHORT_NM_1,
                      ORG_UNIT_1.ORG_UNIT_TYPE_CD  ORG_UNIT_TYPE_CD_1,
                      ORG_UNIT_1.ORG_UNIT_TYPE     ORG_UNIT_TYPE_1,
                      --  ORG_UNIT_1.PARENT_ORG_UNIT_CD,
                      ORG_UNIT_2.ORG_UNIT_CD       ORG_UNIT_CD_2,
                      ORG_UNIT_2.EFFCT_DT          EFFCT_DT_2,
                      ORG_UNIT_2.EXPIRY_DT         EXPIRY_DT_2,
                      ORG_UNIT_2.ORG_UNIT_NM       ORG_UNIT_NM_2,
                      ORG_UNIT_2.ORG_UNIT_SHORT_NM ORG_UNIT_SHORT_NM_2,
                      ORG_UNIT_2.ORG_UNIT_TYPE_CD  ORG_UNIT_TYPE_CD_2,
                      ORG_UNIT_2.ORG_UNIT_TYPE     ORG_UNIT_TYPE_2,
                      --  ORG_UNIT_2.PARENT_ORG_UNIT_CD,
                      ORG_UNIT_3.ORG_UNIT_CD       ORG_UNIT_CD_3,
                      ORG_UNIT_3.EFFCT_DT          EFFCT_DT_3,
                      ORG_UNIT_3.EXPIRY_DT         EXPIRY_DT_3,
                      ORG_UNIT_3.ORG_UNIT_NM       ORG_UNIT_NM_3,
                      ORG_UNIT_3.ORG_UNIT_SHORT_NM ORG_UNIT_SHORT_NM_3,
                      ORG_UNIT_3.ORG_UNIT_TYPE_CD  ORG_UNIT_TYPE_CD_3,
                      ORG_UNIT_3.ORG_UNIT_TYPE     ORG_UNIT_TYPE_3
               FROM ORG_UNIT ORG_UNIT_1
                        LEFT OUTER JOIN ORG_UNIT ORG_UNIT_2
                                        ON (ORG_UNIT_2.ORG_UNIT_CD = ORG_UNIT_1.PARENT_ORG_UNIT_CD
                                            OR
                                            (
                                                    (ORG_UNIT_1.PARENT_ORG_UNIT_CD IS NULL OR
                                                     TRIM(ORG_UNIT_1.PARENT_ORG_UNIT_CD) = '')
                                                    AND
                                                    LEFT(ORG_UNIT_1.ORG_UNIT_CD, 3) || '0' = ORG_UNIT_2.ORG_UNIT_CD
                                                    AND RIGHT(ORG_UNIT_1.ORG_UNIT_CD, 1) != '0'
                                                    AND LENGTH(ORG_UNIT_1.ORG_UNIT_CD) = 4
                                                    AND LENGTH(ORG_UNIT_2.ORG_UNIT_CD) = 4
                                                ))
                                            AND NOT (ORG_UNIT_2.EXPIRY_DT < ORG_UNIT_1.EFFCT_DT OR
                                                     ORG_UNIT_2.EFFCT_DT > ORG_UNIT_1.EXPIRY_DT)
                        LEFT OUTER JOIN ORG_UNIT ORG_UNIT_3
                                        ON (ORG_UNIT_3.ORG_UNIT_CD = ORG_UNIT_2.PARENT_ORG_UNIT_CD
                                            OR
                                            (
                                                    (ORG_UNIT_2.PARENT_ORG_UNIT_CD IS NULL OR
                                                     TRIM(ORG_UNIT_2.PARENT_ORG_UNIT_CD) = '')
                                                    AND
                                                    LEFT(ORG_UNIT_2.ORG_UNIT_CD, 1) || '011' = ORG_UNIT_3.ORG_UNIT_CD
                                                    AND RIGHT(ORG_UNIT_2.ORG_UNIT_CD, 3) != '011'
                                                    AND LENGTH(ORG_UNIT_2.ORG_UNIT_CD) = 4
                                                    AND LENGTH(ORG_UNIT_3.ORG_UNIT_CD) = 4
                                                ))
                                            AND
                                           (NOT (ORG_UNIT_3.EXPIRY_DT < ORG_UNIT_1.EFFCT_DT OR
                                                 ORG_UNIT_3.EFFCT_DT > ORG_UNIT_1.EXPIRY_DT)
                                               OR
                                            NOT (ORG_UNIT_3.EXPIRY_DT < ORG_UNIT_2.EFFCT_DT OR
                                                 ORG_UNIT_3.EFFCT_DT > ORG_UNIT_2.EXPIRY_DT)
                                               )
           ) A) S
WHERE  NOT EXISTS(
        SELECT NULL
        FROM DATA_VAULT.CORE.SAT_ORG_UNIT_AMIS_SUM SAT
        WHERE SAT.ORG_UNIT_CD = S.ORG_UNIT_CD
          AND SAT.EFFECTIVE_DATE = S.EFFECTIVE_DATE
    )
;

