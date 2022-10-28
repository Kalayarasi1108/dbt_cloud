insert into DATA_VAULT.CORE.HUB_LL_HISTORICAL (HUB_LL_HISTORICAL_KEY,
                                               STUDENT_IDENTIFICATION_CODE,
                                               COURSE_CODE,
                                               UNIT_OF_STUDY_CENSUS_DATE,
                                               UNIT_OF_STUDY_CODE,
                                               ACADEMIC_ORGANISATIONAL_UNIT_CODE,
                                               SOURCE,
                                               LOAD_DTS,
                                               ETL_JOB_ID)
select distinct MD5(IFNULL(ll.STUDENT_IDENTIFICATION_CODE, '') || ',' ||
                    IFNULL(ll.COURSE_CODE, '') || ',' ||
                    IFNULL(ll.UNIT_OF_STUDY_CENSUS_DATE, 0) || ',' ||
                    IFNULL(ll.UNIT_OF_STUDY_CODE, '') || ',' ||
                    IFNULL(ll.ACADEMIC_ORGANISATIONAL_UNIT_CODE, '')
                    )                                     as HUB_EN_HISTORICAL_KEY,
                ll.STUDENT_IDENTIFICATION_CODE,
                ll.COURSE_CODE,
                ll.UNIT_OF_STUDY_CENSUS_DATE,
                ll.UNIT_OF_STUDY_CODE,
                ll.ACADEMIC_ORGANISATIONAL_UNIT_CODE,
                'AMIS'                                    as SOURCE,
                CURRENT_TIMESTAMP::TIMESTAMP_NTZ          as LOAD_DTS,
                'SQL' || CURRENT_TIMESTAMP::TIMESTAMP_NTZ as ETL_JOB_ID
FROM ods.EN_LL.staging_ll as ll
         left join DATA_VAULT.CORE.HUB_LL_HISTORICAL as target on
            MD5(IFNULL(ll.STUDENT_IDENTIFICATION_CODE, '') || ',' ||
                IFNULL(ll.COURSE_CODE, '') || ',' ||
                IFNULL(ll.UNIT_OF_STUDY_CENSUS_DATE, 0) || ',' ||
                IFNULL(ll.UNIT_OF_STUDY_CODE, '') || ',' ||
                IFNULL(ll.ACADEMIC_ORGANISATIONAL_UNIT_CODE, '')
                ) = target.HUB_LL_HISTORICAL_KEY
where target.HUB_LL_HISTORICAL_KEY is null;


