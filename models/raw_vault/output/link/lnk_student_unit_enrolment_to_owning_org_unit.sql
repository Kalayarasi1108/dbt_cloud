INSERT INTO DATA_VAULT.CORE.LNK_STUDENT_UNIT_ENROLMENT_TO_OWNING_ORG_UNIT (LNK_STUDENT_UNIT_ENROLMENT_TO_OWNING_ORG_UNIT_KEY,
                                                                           HUB_UNIT_ENROLMENT_KEY, HUB_ORG_UNIT_KEY,
                                                                           STU_ID, UNIT_SPK_CD, UNIT_SPK_VER_NO,
                                                                           UNIT_AVAIL_YR, UNIT_LOCATION_CD,
                                                                           UNIT_SPRD_CD, UNIT_AVAIL_KEY_NO, UNIT_SSP_NO,
                                                                           UNIT_PARENT_SSP_NO, ORG_UNIT_CD, SOURCE,
                                                                           LOAD_DTS, ETL_JOB_ID)
SELECT MD5(IFNULL(UN_SSP.STU_ID, '') || ',' ||
           IFNULL(UN_SPK_DET.SPK_CD, '') || ',' ||
           IFNULL(UN_SPK_DET.SPK_VER_NO, 0) || ',' ||
           IFNULL(UN_SSP.AVAIL_YR, 0) || ',' ||
           IFNULL(UN_SSP.LOCATION_CD, '') || ',' ||
           IFNULL(UN_SSP.SPRD_CD, '') || ',' ||
           IFNULL(UN_SSP.AVAIL_KEY_NO, 0) || ',' ||
           IFNULL(UN_SSP.SSP_NO, 0) || ',' ||
           IFNULL(UN_SSP.PARENT_SSP_NO, 0) || ',' ||
           IFNULL(COALESCE(UN_OWNING_AVAIL_ORG_UNIT.ORG_UNIT_CD, UN_OWNING_ORG_UNIT.ORG_UNIT_CD), '')
           )                                     LNK_STUDENT_UNIT_ENROLMENT_TO_OWNING_ORG_UNIT_KEY,
       MD5(IFNULL(UN_SSP.STU_ID, '') || ',' ||
           IFNULL(UN_SPK_DET.SPK_CD, '') || ',' ||
           IFNULL(UN_SPK_DET.SPK_VER_NO, 0) || ',' ||
           IFNULL(UN_SSP.AVAIL_YR, 0) || ',' ||
           IFNULL(UN_SSP.LOCATION_CD, '') || ',' ||
           IFNULL(UN_SSP.SPRD_CD, '') || ',' ||
           IFNULL(UN_SSP.AVAIL_KEY_NO, 0) || ',' ||
           IFNULL(UN_SSP.SSP_NO, 0) || ',' ||
           IFNULL(UN_SSP.PARENT_SSP_NO, 0)
           )                                     HUB_UNIT_ENROLMENT_KEY,
       MD5(IFNULL(COALESCE(UN_OWNING_AVAIL_ORG_UNIT.ORG_UNIT_CD, UN_OWNING_ORG_UNIT.ORG_UNIT_CD), '')) HUB_ORG_UNIT_KEY,
       UN_SSP.STU_ID,
       UN_SPK_DET.SPK_CD,
       UN_SPK_DET.SPK_VER_NO,
       UN_SSP.AVAIL_YR,
       UN_SSP.LOCATION_CD,
       UN_SSP.SPRD_CD,
       IFNULL(UN_SSP.AVAIL_KEY_NO, 0)            AVAIL_KEY_NO,
       UN_SSP.SSP_NO,
       UN_SSP.PARENT_SSP_NO,
       COALESCE(UN_OWNING_AVAIL_ORG_UNIT.ORG_UNIT_CD, UN_OWNING_ORG_UNIT.ORG_UNIT_CD) ORG_UNIT_CD,
       'AMIS'                                    SOURCE,
       CURRENT_TIMESTAMP::TIMESTAMP_NTZ          LOAD_DTS,
       'SQL' || CURRENT_TIMESTAMP::TIMESTAMP_NTZ ETL_JOB_ID
FROM ODS.AMIS.S1SSP_STU_SPK UN_SSP
         JOIN ODS.AMIS.S1SPK_DET UN_SPK_DET
              ON UN_SPK_DET.SPK_NO = UN_SSP.SPK_NO
                  AND UN_SPK_DET.SPK_VER_NO = UN_SSP.SPK_VER_NO
                  AND UN_SPK_DET.SPK_CAT_CD = 'UN'
         JOIN (SELECT SPK_NO, SPK_VER_NO, MAX(ORG_UNIT_CD) ORG_UNIT_CD
               FROM ODS.AMIS.S1SPK_ORG_UNIT SPK_ORG_UNIT
                WHERE RESP_CAT_CD='O'
             GROUP BY SPK_NO, SPK_VER_NO) UN_OWNING_ORG_UNIT
              ON UN_OWNING_ORG_UNIT.SPK_NO = UN_SSP.SPK_NO AND UN_OWNING_ORG_UNIT.SPK_VER_NO = UN_SSP.SPK_VER_NO
         LEFT OUTER JOIN (
             SELECT AVAIL_KEY_NO, MAX(ORG_UNIT_CD) ORG_UNIT_CD
             FROM ODS.AMIS.S1SPK_AVAIL_ORG UN_SPK_AVAIL_ORG
             WHERE RESP_CAT_CD='O'
             GROUP BY AVAIL_KEY_NO
    ) UN_OWNING_AVAIL_ORG_UNIT
     ON UN_OWNING_AVAIL_ORG_UNIT.AVAIL_KEY_NO=UN_SSP.AVAIL_KEY_NO
WHERE UN_SSP.SSP_NO <> UN_SSP.PARENT_SSP_NO
  AND NOT EXISTS(
        SELECT NULL
        FROM DATA_VAULT.CORE.LNK_STUDENT_UNIT_ENROLMENT_TO_OWNING_ORG_UNIT L
        WHERE L.HUB_UNIT_ENROLMENT_KEY = MD5(IFNULL(UN_SSP.STU_ID, '') || ',' ||
                                             IFNULL(UN_SPK_DET.SPK_CD, '') || ',' ||
                                             IFNULL(UN_SPK_DET.SPK_VER_NO, 0) || ',' ||
                                             IFNULL(UN_SSP.AVAIL_YR, 0) || ',' ||
                                             IFNULL(UN_SSP.LOCATION_CD, '') || ',' ||
                                             IFNULL(UN_SSP.SPRD_CD, '') || ',' ||
                                             IFNULL(UN_SSP.AVAIL_KEY_NO, 0) || ',' ||
                                             IFNULL(UN_SSP.SSP_NO, 0) || ',' ||
                                             IFNULL(UN_SSP.PARENT_SSP_NO, 0)
            )
          AND L.HUB_ORG_UNIT_KEY = MD5(IFNULL(COALESCE(UN_OWNING_AVAIL_ORG_UNIT.ORG_UNIT_CD, UN_OWNING_ORG_UNIT.ORG_UNIT_CD), ''))
    )
;
