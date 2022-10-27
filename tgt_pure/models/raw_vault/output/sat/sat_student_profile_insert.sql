INSERT INTO DATA_VAULT.CORE.SAT_STUDENT_PROFILE (SAT_STUDENT_PROFILE_SK,
                                                 HUB_STUDENT_KEY,
                                                 SOURCE,
                                                 LOAD_DTS,
                                                 ETL_JOB_ID,
                                                 HASH_MD5,
                                                 STUDENT_ID,
                                                 FULL_NAME,
                                                 FAMILY_NAME,
                                                 GIVEN_NAME,
                                                 MIDDLE_NAME,
                                                 PREFERRED_NAME,
                                                 INITIALS,
                                                 TITLE,
                                                 BIRTH_DATE,
                                                 GENDER,
                                                 DECEASED,
                                                 ABORIGINAL_TSI,
                                                 DISABILITY,
                                                 BIRTH_COUNTRY,
                                                 RESIDENCY_COUNTRY,
                                                 RESIDENCY_SUBURB,
                                                 RESIDENCY_STATE,
                                                 RESIDENCY_POST_CODE,
                                                 HOME_LANGUAGE,
                                                 CITIZENSHIP,
                                                 PERMANENT_RESIDENT,
                                                 ONE_ID,
                                                 CONSOLIDATED,
                                                 CONSOLIDATED_TO,
                                                 STUDENT_RECORD_CREATION,
                                                 PARTY_ID,
                                                 DOMESTIC_OR_INTERNATIONAL,
                                                 LAST_COMPLETED_COURSE,
                                                 LAST_COURSE_COMPLETION_DATE,
                                                 ADMITTED_STUDENT,
                                                 ENROLLED_STUDENT,
                                                 CURRENT_MQ_STAFF,
                                                 CURRENT_ON_SCHOLARSHIP,
                                                 GUARDIAN_HIGHEST_EDUCATION,
                                                 FIRST_ADMISSION_YEAR,
                                                 HIGHEST_QUALIFICATION,
                                                 HIGHEST_QUALIFICATION_YEAR,
                                                 SES_INDICATOR,
                                                 CURRENT_YEAR_ENROLMENT,
                                                 YEAR_LEFT_SCHOOL,
                                                 YEAR_OF_ARRIVAL_IN_AUSTRALIA,
                                                 BIRTH_COUNTRY_CODE,
                                                 CITIZENSHIP_COUNTRY_CODE,
                                                 CITIZENSHIP_COUNTRY,
                                                 APPLICATION_EMAIL,
                                                 ALUMNI_EMAIL,
                                                 HDR_EMAIL,
                                                 MGSM_EMAIL,
                                                 MQE_EMAIL,
                                                 PREFERRED_EMAIL,
                                                 CHESSN,
                                                 Tertiary_Entrance_Score,
                                                 Term_Address,
                                                 Permanent_Address,
                                                 Guardian_One,
                                                 Guardian_Two,
                                                 guardian_one_level_of_education,
                                                 guardian_two_level_of_education,
                                                 DISABILITY_TYPE,
                                                 LOCATION_CODE_OF_PERMANENT_HOME_RESIDENCE,
                                                 WORK_EXPERIENCE_INDICATOR,
                                                 VET_STUDY,
                                                 IS_DELETED)


WITH DISABILITY AS (
    SELECT STU_ID,
           listagg(C.CODE_DESCR, '|') as DISABILITY_TYPE
    FROM ODS.AMIS.S1STU_DISABILITY_TYPE as d
             INNER JOIN ODS.AMIS.S1STC_CODE as C
    WHERE CODE_TYPE = 'S1_DISABILITY_TYPE_CD'
      AND D.DISABILITY_TYPE_CD = C.CODE_ID
    GROUP BY STU_ID
)
   , WORK_EXPER AS (
    SELECT STU_ID,
           WORK_EXP_TYPE,
           SC.CODE_DESCR                                                         as WORK_EXPERIENCE_INDICATOR,
           ROW_NUMBER() OVER ( PARTITION BY STU_ID ORDER BY WORK_EXP_TYPE DESC ) as RN
    FROM ODS.AMIS.S1SSP_STU_SPK
             INNER JOIN ODS.AMIS.S1SPK_AVAIL_DET
                        ON ODS.AMIS.S1SSP_STU_SPK.AVAIL_KEY_NO = ODS.AMIS.S1SPK_AVAIL_DET.AVAIL_KEY_NO
             LEFT JOIN ODS.AMIS.S1STC_CODE as SC
                       on SC.CODE_ID = WORK_EXP_TYPE
    WHERE SSP_STTS_CD = 'ENR'
      AND CODE_TYPE = 'S1_WORK_EXPERIENCE'
)
   , VET AS (
    SELECT ODS.AMIS.S1SSP_ASTG_DET.EXT_ORG_ID,
           EXT_ORG_TYPE_CD,
           STU_ID,
           ROW_NUMBER() OVER (PARTITION BY STU_ID ORDER BY EXT_ORG_TYPE_CD DESC) as RN,
           CODE_DESCR                                                            as VET_STUDY
    FROM ODS.AMIS.S1SSP_ASTG_DET
             INNER JOIN ODS.AMIS.S1EXT_ORG
                        ON ODS.AMIS.S1SSP_ASTG_DET.EXT_ORG_ID = ODS.AMIS.S1EXT_ORG.EXT_ORG_ID
             LEFT JOIN ODS.AMIS.S1STC_CODE as CS
                       on CS.CODE_ID = EXT_ORG_TYPE_CD
                           AND CODE_TYPE = 'EXT_ORG_TYPE'
)
   , CS_SSP_ADM AS (
    SELECT DISTINCT STU_ID
    FROM ODS.AMIS.S1SSP_STU_SPK
    WHERE SSP_STTS_CD = 'ADM'
      AND SSP_NO = PARENT_SSP_NO
)
   , UN_SSP_ENR AS (
    SELECT DISTINCT STU_ID
    FROM ODS.AMIS.S1SSP_STU_SPK
    WHERE SSP_STTS_CD IN ('ENR', 'UX')
)
   , UN_SSP_ENR_CURR_YR AS (
    -- COURSE WORK
    SELECT DISTINCT CS_SSP.STU_ID
    from ODS.AMIS.S1SSP_STU_SPK CS_SSP
             JOIN ODS.AMIS.S1SSP_STU_SPK UN_SSP
                  ON UN_SSP.PARENT_SSP_NO = CS_SSP.SSP_NO
    WHERE CS_SSP.SSP_NO = CS_SSP.PARENT_SSP_NO
      AND (
            (TO_CHAR(UN_SSP.AVAIL_YR) = TO_CHAR(CURRENT_TIMESTAMP::TIMESTAMP_NTZ, 'YYYY') AND
             UN_SSP.SSP_STTS_CD IN ('ENR', 'UX'))
            OR (TO_CHAR(UN_SSP.CENSUS_DT, 'YYYY') = TO_CHAR(CURRENT_TIMESTAMP::TIMESTAMP_NTZ, 'YYYY'))
        )
      AND UN_SSP.STUDY_BASIS_CD != '$TIME'
    UNION
    -- TIME BASED
    select DISTINCT CS_SSP.STU_ID
    from ODS.AMIS.S1SSP_STU_SPK CS_SSP
             JOIN ODS.AMIS.S1SSP_STU_SPK UN_SSP
                  ON UN_SSP.PARENT_SSP_NO = CS_SSP.SSP_NO
                      AND UN_SSP.SSP_NO <> UN_SSP.PARENT_SSP_NO
             JOIN ODS.AMIS.S1SSP_EP_DTL SSP_EP_DTL
                  ON SSP_EP_DTL.SSP_NO = UN_SSP.SSP_NO
                      AND TO_CHAR(SSP_EP_DTL.EP_YEAR) = TO_CHAR(CURRENT_TIMESTAMP ::TIMESTAMP_NTZ, 'YYYY')
    WHERE CS_SSP.SSP_NO = CS_SSP.PARENT_SSP_NO
      AND UN_SSP.STUDY_BASIS_CD = '$TIME'
      AND (
                (SSP_EP_DTL.GROSS_NORM_EFTSU + SSP_EP_DTL.GROSS_FEC_EFTSU) > 0
            OR (TO_CHAR(UN_SSP.CENSUS_DT, 'YYYY') = TO_CHAR(CURRENT_TIMESTAMP ::TIMESTAMP_NTZ, 'YYYY'))
        )
)
   , FIRST_ADMISSION_YEAR AS (
    SELECT CS_SSP.STU_ID,
           MIN(CS_SSP.AVAIL_YR) MIN_AVAIL_YR
    FROM ODS.AMIS.S1SSP_STU_SPK CS_SSP
             JOIN ODS.AMIS.S1SSP_STTS_HIST SSP_STTS_HIST
                  ON SSP_STTS_HIST.SSP_NO = CS_SSP.SSP_NO
    WHERE SSP_STTS_HIST.SSP_STTS_CD = 'ADM'
    GROUP BY CS_SSP.STU_ID
)
   , LAST_COMPLETE AS (
    SELECT A.STU_ID,
           A.LAST_COMPLETED_COURSE,
           A.LAST_COURSE_COMPLETION_DATE
    FROM (
             SELECT CS_SSP.STU_ID,
                    CS_SSP.EFFCT_START_DT,
                    AWD_DET.CONFERRAL_DT,
                    CASE
                        WHEN TO_CHAR(AWD_DET.CONFERRAL_DT, 'YYYY') = '1900' THEN CS_SSP.EFFCT_START_DT
                        WHEN AWD_DET.CONFERRAL_DT IS NULL THEN CS_SSP.EFFCT_START_DT
                        ELSE AWD_DET.CONFERRAL_DT
                        END                                                          LAST_COURSE_COMPLETION_DATE,
                    SPK_DET.SPK_FULL_TITLE                                           LAST_COMPLETED_COURSE,
                    ROW_NUMBER() OVER (PARTITION BY CS_SSP.STU_ID
                        ORDER BY CS_SSP.EFFCT_START_DT DESC,
                            AWD_DET.CONFERRAL_DT NULLS LAST,SPK_DET.SPK_FULL_TITLE ) RN
             FROM ODS.AMIS.S1SSP_STU_SPK CS_SSP
                      JOIN ODS.AMIS.S1SPK_DET SPK_DET
                           ON SPK_DET.SPK_NO = CS_SSP.SPK_NO
                               AND SPK_DET.SPK_VER_NO = CS_SSP.SPK_VER_NO
                      LEFT JOIN ODS.AMIS.S1AWD_DET AWD_DET
                                ON AWD_DET.SSP_NO = CS_SSP.SSP_NO
             WHERE CS_SSP.SSP_NO = CS_SSP.PARENT_SSP_NO
               AND CS_SSP.SSP_STTS_CD = 'PASS'
         ) A
    WHERE A.RN = 1
)
   , EMP AS (
    SELECT DISTINCT EMPLOYEE."ID#" EMPLOYEE_ONE_ID
    FROM ODS.HRIS.EMPLOYEE EMPLOYEE
             JOIN ODS.HRIS.SUBSTANTIVE SUB
                  ON SUB."EMPLOYEE#" = EMPLOYEE."EMPLOYEE#"
                      AND CURRENT_DATE BETWEEN SUB.COMMENCE_DATE AND SUB.OCCUP_TERM_DATE
                      AND SUB.EMP_STATUS NOT IN ('SCHL')
)
   , STU_EMAIL AS (
    SELECT STU_ID,
           MAX(IFF(EMAIL_TYPE_CD = '$PRF', EMAIL_ADDR, NULL)) APPLICATION_EMAIL,
           MAX(IFF(EMAIL_TYPE_CD = 'ALUM', EMAIL_ADDR, NULL)) ALUMNI_EMAIL,
           MAX(IFF(EMAIL_TYPE_CD = 'HDR', EMAIL_ADDR, NULL))  HDR_EMAIL,
           MAX(IFF(EMAIL_TYPE_CD = 'MGSM', EMAIL_ADDR, NULL)) MGSM_EMAIL,
           MAX(IFF(EMAIL_TYPE_CD = 'MQE', EMAIL_ADDR, NULL))  MQE_EMAIL,
           MAX(IFF(PREF_EMAIL_FG = 'Y', EMAIL_ADDR, NULL))    PREFERRED_EMAIL
    FROM ODS.AMIS.S1STU_EMAIL
    GROUP BY STU_ID
)
   , STUDENT_PROFILE AS (
    SELECT STU_DET.STU_ID                                                           STUDENT_ID,
           STU_DET.STU_FORMAL_NM_1                                                  FULL_NAME,
           STU_DET.STU_FAMILY_NM                                                    FAMILY_NAME,
           STU_DET.STU_GVN_NM                                                       GIVEN_NAME,
           STU_DET.STU_OTH_NM                                                       MIDDLE_NAME,
           STU_DET.STU_PREF_NM                                                      PREFERRED_NAME,
           STU_DET.STU_INITS                                                        INITIALS,
           TITLE_CODE.CODE_DESCR                                                    TITLE,
           STU_DET.STU_BIRTH_DT                                                     BIRTH_DATE,
           GENDER_CODE.CODE_DESCR                                                   GENDER,
           STU_DET.STU_DECD_FG                                                      DECEASED,
           ABOR_TSI_CODE.CODE_DESCR                                                 ABORIGINAL_TSI,
           STU_DISABILITY.STU_DISAB_FG                                              DISABILITY,
           BIRTH_COUNTRY_CODE.CODE_DESCR                                            BIRTH_COUNTRY,
           STU_DET.STU_BIRTH_CNTRY_CD                                               BIRTH_COUNTRY_CODE,
           CITIZENSHIP_COUNTRY_CODE.CODE_DESCR                                      CITIZENSHIP_COUNTRY,
           STU_DET.STU_CTZN_CNTRY_CD                                                CITIZENSHIP_COUNTRY_CODE,
           RESIDENCY_COUNTRY_CODE.CODE_DESCR                                        RESIDENCY_COUNTRY,
           STU_ADDR.STU_SUBURB                                                      RESIDENCY_SUBURB,
           STU_ADDR.STU_STATE                                                       RESIDENCY_STATE,
           STU_ADDR.STU_PCODE                                                       RESIDENCY_POST_CODE,
           HOME_LANG_CODE.CODE_DESCR                                                HOME_LANGUAGE,
           CITIZENSHIP_CODE.CODE_DESCR                                              CITIZENSHIP,
           PRM_RSD_CODE.CODE_DESCR                                                  PERMANENT_RESIDENT,
           STU_DET.STU_ID                                                           ONE_ID,
           STU_DET.STU_CONSOL_FG                                                    CONSOLICATED,
           STU_DET.CONSOL_TO_STU_ID                                                 CONSOLICATED_TO,
           TO_TIMESTAMP_NTZ(TO_CHAR(STU_DET.CRDATEI, 'YYYYMMDD') || LPAD(STU_DET.CRTIMEI, 6, '0'),
                            'YYYYMMDDHH24MISS')                                     STUDENT_RECORD_CREATION,
           ''                                                                       PARTY_ID,
           CASE
               WHEN CITIZENSHIP_CODE.code_id in ('1', '2', '3', '8') THEN 'Domestic'
               WHEN CITIZENSHIP_CODE.code_id in ('4', '5') THEN 'International'
               ELSE null
               END                                                                  DOMESTIC_OR_INTERNATIONAL,
           LAST_COMPLETE.LAST_COMPLETED_COURSE                                      LAST_COMPLETED_COURSE,
           LAST_COMPLETE.LAST_COURSE_COMPLETION_DATE                                LAST_COURSE_COMPLETION_DATE,
           IFF(CS_SSP_ADM.STU_ID IS NOT NULL, 'Y', 'N')                             ADMITTED_STUDENT,
           IFF(UN_SSP_ENR.STU_ID IS NOT NULL, 'Y', 'N')                             ENROLLED_STUDENT,
           IFF(EMP.EMPLOYEE_ONE_ID IS NOT NULL, 'Y', 'N')                           CURRENT_MQ_STAFF,
           ''                                                                       CURRENT_ON_SCHOLARSHIP,
           ''                                                                       GUARDIAN_HIGHEST_EDUCATION,
           FIRST_ADMISSION_YEAR.MIN_AVAIL_YR                                        FIRST_ADMISSION_YEAR,
           ''                                                                       HIGHEST_QUALIFICATION,
           NULL                                                                     HIGHEST_QUALIFICATION_YEAR,
           IFF(
                       CITIZENSHIP_CODE.code_id in ('1', '2', '3', '8'),
                       SES.SES_INDICATOR,
                       NULL
               )                                                                    SES_INDICATOR,
           IFF(UN_SSP_ENR_CURR_YR.STU_ID IS NOT NULL, 'Y', 'N')                     CURRENT_YEAR_ENROLMENT,
           IFF(TRIM(STU_DET.YR12_YEAR) = '', NULL, TRIM(STU_DET.YR12_YEAR))::number YEAR_LEFT_SCHOOL,
           STU_DET.STU_ENTRY_YR                                                     YEAR_OF_ARRIVAL_IN_AUSTRALIA,
           STU_EMAIL.APPLICATION_EMAIL,
           STU_EMAIL.ALUMNI_EMAIL,
           STU_EMAIL.HDR_EMAIL,
           STU_EMAIL.MGSM_EMAIL,
           STU_EMAIL.MQE_EMAIL,
           STU_EMAIL.PREFERRED_EMAIL,
           STU_DET.GSN_NO       as                                                  CHESSN,
           PREV_SCORE_RSLT      as                                                  Tertiary_Entrance_Score,
           ADDR.STU_PCODE       as                                                  Term_Address,
           ADDRP.STU_PCODE      as                                                  Permanent_Address,
           G1.GUARDIAN_HEA_CD   as                                                  Guardian_One,
           G2.GUARDIAN_HEA_CD   as                                                  Guardian_Two,
           G1DESC.CODE_DESCR    as                                                  Guardian_one_level_of_education,
           G2DESC.CODE_DESCR    as                                                  Guardian_two_level_of_education,
           DISABILITY_TYPE,
           IFF(LEN(ADDRP.STU_PCODE) != 4, RESIDENCY_COUNTRY_CODE.CODE_DESCR,
               ADDRP.STU_PCODE) as                                                  LOCATION_CODE_OF_PERMANENT_HOME_RESIDENCE,
           WORK_EXPER.WORK_EXPERIENCE_INDICATOR,
           VET_STUDY,
           'N'                                                                      IS_DELETED,
           MD5(
                       IFNULL(STUDENT_ID, '') || ',' ||
                       IFNULL(FULL_NAME, '') || ',' ||
                       IFNULL(FAMILY_NAME, '') || ',' ||
                       IFNULL(GIVEN_NAME, '') || ',' ||
                       IFNULL(MIDDLE_NAME, '') || ',' ||
                       IFNULL(PREFERRED_NAME, '') || ',' ||
                       IFNULL(INITIALS, '') || ',' ||
                       IFNULL(TITLE, '') || ',' ||
                       IFNULL(TO_CHAR(BIRTH_DATE, 'YYYYMMDD'), '') || ',' ||
                       IFNULL(GENDER, '') || ',' ||
                       IFNULL(DECEASED, '') || ',' ||
                       IFNULL(ABORIGINAL_TSI, '') || ',' ||
                       IFNULL(DISABILITY, '') || ',' ||
                       IFNULL(BIRTH_COUNTRY, '') || ',' ||
                       IFNULL(RESIDENCY_COUNTRY, '') || ',' ||
                       IFNULL(RESIDENCY_SUBURB, '') || ',' ||
                       IFNULL(RESIDENCY_STATE, '') || ',' ||
                       IFNULL(RESIDENCY_POST_CODE, '') || ',' ||
                       IFNULL(HOME_LANGUAGE, '') || ',' ||
                       IFNULL(CITIZENSHIP, '') || ',' ||
                       IFNULL(PERMANENT_RESIDENT, '') || ',' ||
                       IFNULL(ONE_ID, '') || ',' ||
                       IFNULL(CONSOLICATED, '') || ',' ||
                       IFNULL(CONSOLICATED_TO, '') || ',' ||
                       IFNULL(TO_CHAR(STUDENT_RECORD_CREATION, 'YYYYMMDD HH24:MI:SS'), '') || ',' ||
                       IFNULL(PARTY_ID, '') || ',' ||
                       IFNULL(DOMESTIC_OR_INTERNATIONAL, '') || ',' ||
                       IFNULL(LAST_COMPLETED_COURSE, '') || ',' ||
                       IFNULL(TO_CHAR(LAST_COURSE_COMPLETION_DATE, 'YYYYMMDD HH24:MI:SS'), '') || ',' ||
                       IFNULL(ADMITTED_STUDENT, '') || ',' ||
                       IFNULL(ENROLLED_STUDENT, '') || ',' ||
                       IFNULL(CURRENT_MQ_STAFF, '') || ',' ||
                       IFNULL(CURRENT_ON_SCHOLARSHIP, '') || ',' ||
                       IFNULL(GUARDIAN_HIGHEST_EDUCATION, '') || ',' ||
                       IFNULL(TO_CHAR(FIRST_ADMISSION_YEAR), '') || ',' ||
                       IFNULL(HIGHEST_QUALIFICATION, '') || ',' ||
                       IFNULL(TO_CHAR(HIGHEST_QUALIFICATION_YEAR), '') || ',' ||
                       IFNULL(SES_INDICATOR, '') || ',' ||
                       IFNULL(CURRENT_YEAR_ENROLMENT, '') || ',' ||
                       IFNULL(TO_CHAR(YEAR_LEFT_SCHOOL), '') || ',' ||
                       IFNULL(TO_CHAR(YEAR_OF_ARRIVAL_IN_AUSTRALIA), '') || ',' ||
                       IFNULL(TO_CHAR(BIRTH_COUNTRY_CODE), '') || ',' ||
                       IFNULL(TO_CHAR(CITIZENSHIP_COUNTRY_CODE), '') || ',' ||
                       IFNULL(TO_CHAR(CITIZENSHIP_COUNTRY), '') || ',' ||
                       IFNULL(TO_CHAR(APPLICATION_EMAIL), '') || ',' ||
                       IFNULL(TO_CHAR(ALUMNI_EMAIL), '') || ',' ||
                       IFNULL(TO_CHAR(HDR_EMAIL), '') || ',' ||
                       IFNULL(TO_CHAR(MGSM_EMAIL), '') || ',' ||
                       IFNULL(TO_CHAR(MQE_EMAIL), '') || ',' ||
                       IFNULL(TO_CHAR(PREFERRED_EMAIL), '') || ',' ||
                       IFNULL(TO_CHAR(CHESSN), '') || ',' ||
                       IFNULL(TO_CHAR(TERTIARY_ENTRANCE_SCORE), '') || ',' ||
                       IFNULL(TO_CHAR(Term_Address), '') || ',' ||
                       IFNULL(TO_CHAR(Permanent_Address), '') || ',' ||
                       IFNULL(TO_CHAR(Guardian_One), '') || ',' ||
                       IFNULL(TO_CHAR(Guardian_Two), '') || ',' ||
                       IFNULL(TO_CHAR(guardian_one_level_of_education), '') || ',' ||
                       IFNULL(TO_CHAR(guardian_two_level_of_education), '') || ',' ||
                       IFNULL(TO_CHAR(DISABILITY_TYPE), '') || ',' ||
                       IFNULL(TO_CHAR(LOCATION_CODE_OF_PERMANENT_HOME_RESIDENCE), '') || ',' ||
                       IFNULL(TO_CHAR(WORK_EXPERIENCE_INDICATOR), '') || ',' ||
                       IFNULL(TO_CHAR(VET_STUDY), '') || ',' ||
                       IFNULL(IS_DELETED, '')
               )                                                                    HASH_MD5
    FROM ODS.AMIS.S1STU_DET STU_DET
             LEFT JOIN ODS.AMIS.S1STC_CODE TITLE_CODE
                       ON TITLE_CODE.CODE_ID = STU_DET.STU_TITLE_CD
                           AND TITLE_CODE.CODE_TYPE = 'TITLE'
             LEFT JOIN ODS.AMIS.S1STC_CODE GENDER_CODE
                       ON GENDER_CODE.CODE_ID = STU_DET.STU_GENDER_CD
                           AND GENDER_CODE.CODE_TYPE = 'GENDER'
             LEFT JOIN ODS.AMIS.S1STC_CODE ABOR_TSI_CODE
                       ON ABOR_TSI_CODE.CODE_ID = STU_DET.STU_ABOR_TSI_CD
                           AND ABOR_TSI_CODE.CODE_TYPE = 'STU_ABOR_TSI_CD'
             LEFT JOIN ODS.AMIS.S1STU_DISABILITY STU_DISABILITY
                       ON STU_DISABILITY.STU_ID = STU_DET.STU_ID
             LEFT JOIN ODS.AMIS.S1STC_CODE BIRTH_COUNTRY_CODE
                       ON BIRTH_COUNTRY_CODE.CODE_ID = STU_DET.STU_BIRTH_CNTRY_CD
                           AND BIRTH_COUNTRY_CODE.CODE_TYPE = 'COUNTRY'
             LEFT JOIN ODS.AMIS.S1STU_ADDR STU_ADDR
                       ON STU_ADDR.STU_ID = STU_DET.STU_ID
                           AND STU_ADDR.STU_ADDR_TYPE_CD = 'C'
             LEFT JOIN ODS.AMIS.S1STC_CODE RESIDENCY_COUNTRY_CODE
                       ON RESIDENCY_COUNTRY_CODE.CODE_ID = STU_ADDR.STU_COUNTRY_CD
                           AND RESIDENCY_COUNTRY_CODE.CODE_TYPE = 'COUNTRY'
             LEFT JOIN ODS.AMIS.S1STC_CODE CITIZENSHIP_COUNTRY_CODE
                       ON CITIZENSHIP_COUNTRY_CODE.CODE_ID = STU_DET.STU_CTZN_CNTRY_CD
                           AND CITIZENSHIP_COUNTRY_CODE.CODE_TYPE = 'COUNTRY'
             LEFT JOIN ODS.AMIS.S1STC_CODE HOME_LANG_CODE
                       ON HOME_LANG_CODE.CODE_ID = STU_DET.STU_HOME_LANG_CD
                           AND HOME_LANG_CODE.CODE_TYPE = 'HOME_LANGUAGE'
             LEFT JOIN ODS.AMIS.S1STC_CODE CITIZENSHIP_CODE
                       ON CITIZENSHIP_CODE.CODE_ID = STU_DET.STU_CITIZEN_CD
                           AND CITIZENSHIP_CODE.CODE_TYPE = 'CITIZENSHIP'
             LEFT JOIN ODS.AMIS.S1STC_CODE PRM_RSD_CODE
                       ON PRM_RSD_CODE.CODE_ID = STU_DET.STU_PRM_RSD_CD
                           AND PRM_RSD_CODE.CODE_TYPE = 'YES_NO_CODE'
             LEFT JOIN ODS.AMIS.S1STU_OTH_SCORE as ATAR
                       ON ATAR.STU_ID = STU_DET.STU_ID
                           AND PREV_SCORE_TYPE_CD = 'ATR'
             LEFT JOIN ODS.AMIS.S1STU_ADDR as ADDR
                       ON ADDR.STU_ID = STU_DET.STU_ID
                           AND ADDR.STU_ADDR_TYPE_CD = 'C'
             LEFT JOIN ODS.AMIS.S1STU_ADDR as ADDRP
                       ON ADDRP.STU_ID = STU_DET.STU_ID
                           AND ADDRP.STU_ADDR_TYPE_CD = 'P'
             LEFT JOIN ODS.AMIS.S1STU_GUARDIAN as G1
                       ON G1.STU_ID = STU_DET.STU_ID
                           AND G1.GUARDIAN_SEQ_NO = 1
             LEFT JOIN ODS.AMIS.S1STU_GUARDIAN as G2
                       ON G2.STU_ID = STU_DET.STU_ID
                           AND G2.GUARDIAN_SEQ_NO = 2
             LEFT JOIN ODS.AMIS.S1STC_CODE as G1DESC
                       on G1.GUARDIAN_HEA_CD = G1DESC.CODE_ID
                           AND G1DESC.CODE_TYPE = 'S1_PAR_HEA_CD'
             LEFT JOIN ODS.AMIS.S1STC_CODE as G2DESC
                       on G2.GUARDIAN_HEA_CD = G2DESC.CODE_ID
                           AND G2DESC.CODE_TYPE = 'S1_PAR_HEA_CD'
             LEFT JOIN DISABILITY
                       ON DISABILITY.STU_ID = STU_DET.STU_ID
             LEFT JOIN WORK_EXPER
                       ON WORK_EXPER.STU_ID = STU_DET.STU_ID
                           AND WORK_EXPER.RN = 1
             LEFT JOIN VET
                       on VET.STU_ID = STU_DET.STU_ID
                           AND VET.RN = 1
             LEFT JOIN CS_SSP_ADM
                       ON CS_SSP_ADM.STU_ID = STU_DET.STU_ID
             LEFT JOIN UN_SSP_ENR
                       ON UN_SSP_ENR.STU_ID = STU_DET.STU_ID
             LEFT JOIN UN_SSP_ENR_CURR_YR ON UN_SSP_ENR_CURR_YR.STU_ID = STU_DET.STU_ID
             LEFT JOIN FIRST_ADMISSION_YEAR ON FIRST_ADMISSION_YEAR.STU_ID = STU_DET.STU_ID
             LEFT JOIN DATA_VAULT.CORE.REF_SUBURB_SES SES ON SES.POST_CODE = STU_ADDR.STU_PCODE
             LEFT JOIN LAST_COMPLETE ON LAST_COMPLETE.STU_ID = STU_DET.STU_ID
             LEFT JOIN EMP ON EMP.EMPLOYEE_ONE_ID = 'MQ' || STU_DET.STU_ID
             LEFT JOIN STU_EMAIL ON STU_EMAIL.STU_ID = STU_DET.STU_ID
)
   , SAT_STUDENT_PROFILE AS (
    SELECT HUB_STUDENT_KEY,
           HASH_MD5,
           LOAD_DTS,
           LEAD(LOAD_DTS) OVER (PARTITION BY HUB_STUDENT_KEY ORDER BY LOAD_DTS ) EFFECTIVE_END_DTS
    FROM DATA_VAULT.CORE.SAT_STUDENT_PROFILE
)
SELECT DATA_VAULT.CORE.SEQ.NEXTVAL               SAT_STUDENT_PROFILE_SK,
       MD5(STUDENT_ID)                           HUB_STUDENT_KEY,
       'AMIS'                                    SOURCE,
       CURRENT_TIMESTAMP::timestamp_ntz          LOAD_DTS,
       'SQL' || CURRENT_TIMESTAMP::TIMESTAMP_NTZ ETL_JOB_ID,
       HASH_MD5,
       STUDENT_ID,
       FULL_NAME,
       FAMILY_NAME,
       GIVEN_NAME,
       MIDDLE_NAME,
       PREFERRED_NAME,
       INITIALS,
       TITLE,
       BIRTH_DATE,
       GENDER,
       DECEASED,
       ABORIGINAL_TSI,
       DISABILITY,
       BIRTH_COUNTRY,
       RESIDENCY_COUNTRY,
       RESIDENCY_SUBURB,
       RESIDENCY_STATE,
       RESIDENCY_POST_CODE,
       HOME_LANGUAGE,
       CITIZENSHIP,
       PERMANENT_RESIDENT,
       ONE_ID,
       CONSOLICATED,
       CONSOLICATED_TO,
       STUDENT_RECORD_CREATION,
       PARTY_ID,
       DOMESTIC_OR_INTERNATIONAL,
       LAST_COMPLETED_COURSE,
       LAST_COURSE_COMPLETION_DATE,
       ADMITTED_STUDENT,
       ENROLLED_STUDENT,
       CURRENT_MQ_STAFF,
       CURRENT_ON_SCHOLARSHIP,
       GUARDIAN_HIGHEST_EDUCATION,
       FIRST_ADMISSION_YEAR,
       HIGHEST_QUALIFICATION,
       HIGHEST_QUALIFICATION_YEAR,
       SES_INDICATOR,
       CURRENT_YEAR_ENROLMENT,
       YEAR_LEFT_SCHOOL,
       YEAR_OF_ARRIVAL_IN_AUSTRALIA,
       BIRTH_COUNTRY_CODE,
       CITIZENSHIP_COUNTRY_CODE,
       CITIZENSHIP_COUNTRY,
       APPLICATION_EMAIL,
       ALUMNI_EMAIL,
       HDR_EMAIL,
       MGSM_EMAIL,
       MQE_EMAIL,
       PREFERRED_EMAIL,
       CHESSN,
       Tertiary_Entrance_Score,
       Term_Address,
       Permanent_Address,
       Guardian_One,
       Guardian_Two,
       guardian_one_level_of_education,
       guardian_two_level_of_education,
       DISABILITY_TYPE,
       LOCATION_CODE_OF_PERMANENT_HOME_RESIDENCE,
       WORK_EXPERIENCE_INDICATOR,
       VET_STUDY,
       IS_DELETED
FROM STUDENT_PROFILE S
WHERE NOT EXISTS(
        SELECT NULL
        FROM SAT_STUDENT_PROFILE SAT
        WHERE SAT.EFFECTIVE_END_DTS IS NULL
          AND SAT.HUB_STUDENT_KEY = MD5(S.STUDENT_ID)
          AND SAT.HASH_MD5 = S.HASH_MD5
    );