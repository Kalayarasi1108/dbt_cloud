INSERT INTO DATA_VAULT.CORE.LNK_STUDYLINK_APPLICANT_TO_AMIS_STUDENT (LNK_STUDYLINK_APPLICANT_TO_AMIS_STUDENT_KEY,
                                                                     HUB_STUDENT_KEY,
                                                                     HUB_STUDYLINK_APPLICANT_KEY,
                                                                     STU_ID,
                                                                     DF_APPLICATIONDETAILS_PORTALPROVIDERAPPLICANTID,
                                                                     SOURCE,
                                                                     LOAD_DTS,
                                                                     ETL_JOB_ID)
SELECT MD5(IFNULL(DET.STU_ID, '') ||
           IFNULL(DET.DF_APPLICATIONDETAILS_PORTALPROVIDERAPPLICANTID, '')
           )                                                                LNK_STUDYLINK_APPLICANT_TO_AMIS_STUDENT_KEY,
       MD5(IFNULL(DET.STU_ID, ''))                                          HUB_STUDENT_KEY,
       MD5(IFNULL(DET.DF_APPLICATIONDETAILS_PORTALPROVIDERAPPLICANTID, '')) HUB_STUDYLINK_APPLICANT_KEY,
       DET.STU_ID,
       DET.DF_APPLICATIONDETAILS_PORTALPROVIDERAPPLICANTID,
       'AMIS-STUDYLINK'                                                     SOURCE,
       CURRENT_TIMESTAMP :: TIMESTAMP_NTZ                                   LOAD_DTS,
       'SQL' || CURRENT_TIMESTAMP :: TIMESTAMP_NTZ                          ETL_JOB_ID
FROM (
         SELECT IFF(STU_CONSOL_FG = 'Y', CONSOL_TO_STU_ID, STU_ID) STU_ID,
                DF_APPLICATIONDETAILS_PORTALPROVIDERAPPLICANTID
         FROM (
                  SELECT AMIS_DET.STU_ID,
                         AMIS_DET.CONSOL_TO_STU_ID,
                         AMIS_DET.STU_CONSOL_FG,
                         STUDYLINK_DET.DF_APPLICATIONDETAILS_PORTALPROVIDERAPPLICANTID
                  FROM (
                           SELECT DISTINCT STU_ID, CONSOL_TO_STU_ID, STU_CONSOL_FG
                           FROM ODS.AMIS.S1STU_DET
                       ) AMIS_DET
                           JOIN
                       (
                           SELECT DISTINCT DF_APPLICATIONDETAILS_PORTALPROVIDERAPPLICANTID
                           FROM ODS.STUDYLINK.STUDYLINK_APPLICATION_API_LATEST
                           WHERE DF_APPLICATIONDETAILS_PORTALPROVIDERAPPLICANTID IS NOT NULL
                       ) STUDYLINK_DET
                       ON AMIS_DET.STU_ID = STUDYLINK_DET.DF_APPLICATIONDETAILS_PORTALPROVIDERAPPLICANTID
              )) DET
         WHERE NOT EXISTS
             (
                 SELECT NULL
                 FROM DATA_VAULT.CORE.LNK_STUDYLINK_APPLICANT_TO_AMIS_STUDENT SLA
                 WHERE SLA.HUB_STUDENT_KEY = MD5(IFNULL(DET.STU_ID, ''))
                   AND SLA.HUB_STUDYLINK_APPLICANT_KEY =
                       MD5(IFNULL(DET.DF_APPLICATIONDETAILS_PORTALPROVIDERAPPLICANTID, ''))
             )
     ;
