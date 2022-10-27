INSERT INTO DATA_VAULT.CORE.LNK_STUDENT_UNIT_ENROLMENT_EP_TO_COURSE_FEE (LNK_STUDENT_UNIT_ENROLMENT_EP_TO_COURSE_FEE_KEY,
                                                                         HUB_UNIT_ENROLMENT_EP_KEY, HUB_COURSE_FEE_KEY,
                                                                         STU_ID, SPK_CD, SPK_VER_NO, AVAIL_YR,
                                                                         LOCATION_CD, SPRD_CD, AVAIL_KEY_NO, SSP_NO,
                                                                         PARENT_SSP_NO, EP_YEAR, EP_NO, CS_SPK_CD,
                                                                         CS_SPK_VER_NO, FEE_LIAB_NO, FEE_YR, SOURCE,
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
           IFNULL(SSP_EP_DTL.EP_YEAR, 0) || ',' ||
           IFNULL(SSP_EP_DTL.EP_NO, 0) || ',' ||
           IFNULL(CS_SPK_DET.SPK_CD, '') || ',' ||
           IFNULL(CS_SPK_DET.SPK_VER_NO, 0) || ',' ||
           IFNULL(HDR_COURSE_FEE.FEE_LIAB_NO, 0) || ',' ||
           IFNULL(HDR_COURSE_FEE.FEE_YR, 0)
           )                                     LNK_STUDENT_UNIT_ENROLMENT_EP_FEE_KEY,
       MD5(IFNULL(UN_SSP.STU_ID, '') || ',' ||
           IFNULL(UN_SPK_DET.SPK_CD, '') || ',' ||
           IFNULL(UN_SPK_DET.SPK_VER_NO, 0) || ',' ||
           IFNULL(UN_SSP.AVAIL_YR, 0) || ',' ||
           IFNULL(UN_SSP.LOCATION_CD, '') || ',' ||
           IFNULL(UN_SSP.SPRD_CD, '') || ',' ||
           IFNULL(UN_SSP.AVAIL_KEY_NO, 0) || ',' ||
           IFNULL(UN_SSP.SSP_NO, 0) || ',' ||
           IFNULL(UN_SSP.PARENT_SSP_NO, 0) || ',' ||
           IFNULL(SSP_EP_DTL.EP_YEAR, 0) || ',' ||
           IFNULL(SSP_EP_DTL.EP_NO, 0)
           )                                     HUB_UNIT_ENROLMENT_EP_KEY,
       MD5(
                   IFNULL(CS_SPK_DET.SPK_CD, '') || ',' ||
                   IFNULL(CS_SPK_DET.SPK_VER_NO, 0) || ',' ||
                   IFNULL(HDR_COURSE_FEE.FEE_LIAB_NO, 0) || ',' ||
                   IFNULL(HDR_COURSE_FEE.FEE_YR, 0)
           )                                     HUB_COURSE_FEE_KEY,
       UN_SSP.STU_ID,
       UN_SPK_DET.SPK_CD,
       UN_SPK_DET.SPK_VER_NO,
       UN_SSP.AVAIL_YR,
       UN_SSP.LOCATION_CD,
       UN_SSP.SPRD_CD,
       IFNULL(UN_SSP.AVAIL_KEY_NO, 0)            AVAIL_KEY_NO,
       UN_SSP.SSP_NO,
       UN_SSP.PARENT_SSP_NO,
       SSP_EP_DTL.EP_YEAR,
       SSP_EP_DTL.EP_NO,
       CS_SPK_DET.SPK_CD                         CS_SPK_CD,
       CS_SPK_DET.SPK_VER_NO                     CS_SPK_VER_NO,
       HDR_COURSE_FEE.FEE_LIAB_NO,
       HDR_COURSE_FEE.FEE_YR,
       'AMIS'                                    SOURCE,
       CURRENT_TIMESTAMP::TIMESTAMP_NTZ          LOAD_DTS,
       'SQL' || CURRENT_TIMESTAMP::TIMESTAMP_NTZ ETL_JOB_ID
FROM ODS.AMIS.S1SSP_STU_SPK UN_SSP
         JOIN ODS.AMIS.S1SPK_DET UN_SPK_DET
              ON UN_SPK_DET.SPK_NO = UN_SSP.SPK_NO
                  AND UN_SPK_DET.SPK_VER_NO = UN_SSP.SPK_VER_NO
                  AND UN_SPK_DET.SPK_CAT_CD = 'UN'
         JOIN ODS.AMIS.S1SSP_EP_DTL SSP_EP_DTL
              ON SSP_EP_DTL.SSP_NO = UN_SSP.SSP_NO
         JOIN ODS.AMIS.S1SPK_DET CS_SPK_DET
              ON CS_SPK_DET.SPK_NO = UN_SSP.PARENT_SPK_NO
                  AND CS_SPK_DET.SPK_VER_NO = UN_SSP.PARENT_SPK_VER_NO
                  AND CS_SPK_DET.SPK_CAT_CD = 'CS'
         JOIN
     (
         SELECT SPK_DET.SPK_CD,
                SPK_DET.SPK_VER_NO,
                FEE_DET_1.FEE_LIAB_NO,
                FEE_DET_1.FEE_YR,
                FEE_DET_1.FORM_NM,
                IFF(FEE_DET_1.FORM_NM LIKE 'RR%',
                    REGEXP_REPLACE(FEE_DET_1.FORM_NM, '[A-Za-z]')::FLOAT, NULL) HDR_COURSE_BASE_FEE_AMOUNT,
                IFF(FEE_DET_1.FORM_NM LIKE '%X', 'EXT', 'INT')                  ATTENDENCE_MODE_CD,
                ROW_NUMBER() OVER (PARTITION BY SPK_DET.SPK_CD, FEE_DET_1.FEE_YR
                    ORDER BY REGEXP_REPLACE(FEE_DET_1.FORM_NM, '[A-Za-z]')::FLOAT DESC,
                        FEE_DET_1.FEE_LIAB_NO DESC,
                        SPK_DET.SPK_VER_NO DESC,
                        FEE_DET_1.FORM_NM ASC)                                  RN
         FROM ODS.AMIS.S1SPK_FEE SPK_FEE
                  JOIN ODS.AMIS.S1SPK_DET SPK_DET
                       ON SPK_DET.SPK_NO = SPK_FEE.SPK_NO
                           AND SPK_DET.SPK_VER_NO = SPK_FEE.SPK_VER_NO
                           AND SPK_DET.SPK_CAT_CD = 'CS'
                  JOIN ODS.AMIS.S1FEE_DET FEE_DET
                       ON SPK_FEE.FEE_LIAB_NO = FEE_DET.FEE_LIAB_NO
                           AND SPK_FEE.FEE_YR = FEE_DET.FEE_YR
                  JOIN ODS.AMIS.S1FEE_ASSOC_FEE FEE_ASSOC_FEE
                       ON FEE_ASSOC_FEE.FEE_LIAB_NO = FEE_DET.FEE_LIAB_NO
                           AND FEE_ASSOC_FEE.FEE_YR = FEE_DET.FEE_YR
                  JOIN ODS.AMIS.S1FEE_DET FEE_DET_1
                       ON FEE_DET_1.FEE_LIAB_NO = FEE_ASSOC_FEE.ASSOC_FEE_LIAB_NO
                           AND FEE_DET_1.FEE_YR = FEE_ASSOC_FEE.ASSOC_FEE_YR
         WHERE IFF(FEE_DET_1.FORM_NM LIKE 'RR%',
                   REGEXP_REPLACE(FEE_DET_1.FORM_NM, '[A-Za-z]')::FLOAT, NULL) IS NOT NULL
           AND FEE_DET_1.FORM_NM NOT LIKE '%X'
     ) HDR_COURSE_FEE
     ON HDR_COURSE_FEE.RN = 1
         AND HDR_COURSE_FEE.SPK_CD = CS_SPK_DET.SPK_CD
         AND HDR_COURSE_FEE.SPK_VER_NO = CS_SPK_DET.SPK_VER_NO
         AND HDR_COURSE_FEE.FEE_YR = YEAR(SSP_EP_DTL.CTL_CENSUS_DT)
WHERE UN_SSP.SSP_NO <> UN_SSP.PARENT_SSP_NO
  AND UN_SSP.STUDY_BASIS_CD = '$TIME'
  AND NOT EXISTS(
        SELECT NULL
        FROM DATA_VAULT.CORE.LNK_STUDENT_UNIT_ENROLMENT_EP_TO_COURSE_FEE L
        WHERE L.HUB_UNIT_ENROLMENT_EP_KEY = MD5(IFNULL(UN_SSP.STU_ID, '') || ',' ||
                                                IFNULL(UN_SPK_DET.SPK_CD, '') || ',' ||
                                                IFNULL(UN_SPK_DET.SPK_VER_NO, 0) || ',' ||
                                                IFNULL(UN_SSP.AVAIL_YR, 0) || ',' ||
                                                IFNULL(UN_SSP.LOCATION_CD, '') || ',' ||
                                                IFNULL(UN_SSP.SPRD_CD, '') || ',' ||
                                                IFNULL(UN_SSP.AVAIL_KEY_NO, 0) || ',' ||
                                                IFNULL(UN_SSP.SSP_NO, 0) || ',' ||
                                                IFNULL(UN_SSP.PARENT_SSP_NO, 0) || ',' ||
                                                IFNULL(SSP_EP_DTL.EP_YEAR, 0) || ',' ||
                                                IFNULL(SSP_EP_DTL.EP_NO, 0)
            )
          AND L.HUB_COURSE_FEE_KEY = MD5(
                    IFNULL(CS_SPK_DET.SPK_CD, '') || ',' ||
                    IFNULL(CS_SPK_DET.SPK_VER_NO, 0) || ',' ||
                    IFNULL(HDR_COURSE_FEE.FEE_LIAB_NO, 0) || ',' ||
                    IFNULL(HDR_COURSE_FEE.FEE_YR, 0)
            )
    )
;