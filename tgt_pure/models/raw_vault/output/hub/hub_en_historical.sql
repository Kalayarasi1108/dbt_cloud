SELECT distinct MD5(IFNULL(en.STUDENT_IDENTIFICATION_CODE, '') || ',' ||
                    IFNULL(en.COURSE_CODE, '')
                    )                                     as HUB_EN_HISTORICAL_KEY,
                en.STUDENT_IDENTIFICATION_CODE,
                en.COURSE_CODE,
                'AMIS'                                    as SOURCE,
                CURRENT_TIMESTAMP::TIMESTAMP_NTZ          as LOAD_DTS,
                'SQL' || CURRENT_TIMESTAMP::TIMESTAMP_NTZ as ETL_JOB_ID
FROM {{source('en_ll','staging_en')}}as en
         left join DATA_VAULT.CORE.HUB_EN_HISTORICAL as target on
            MD5(IFNULL(en.STUDENT_IDENTIFICATION_CODE, '') || ',' ||
                IFNULL(en.COURSE_CODE, '')
                ) = target.HUB_EN_HISTORICAL_KEY
where target.HUB_EN_HISTORICAL_KEY is null;
