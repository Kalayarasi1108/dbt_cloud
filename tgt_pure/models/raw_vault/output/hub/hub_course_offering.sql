insert into data_vault.core.hub_course_offering(hub_course_offering_key, spk_cd, spk_ver_no, avail_key_no, avail_yr,
                                                sprd_cd, location_cd, source, load_dts, etl_job_id)
with course_offering as (
    select spk_cd,
           sd.spk_ver_no,
           avail_key_no,
           avail_yr,
           sprd_cd,
           location_cd
    from ods.amis.s1spk_det sd
             join ods.amis.s1spk_avail_det sad on sd.spk_no = sad.spk_no
        and sd.spk_ver_no = sad.spk_ver_no
    where spk_cat_cd = 'CS'
),
     ssp_additional_course_offering as (
         select distinct cs_spk_det.spk_cd,
                         cs_spk_det.spk_ver_no,
                         cs_ssp.avail_key_no,
                         cs_ssp.avail_yr,
                         cs_ssp.sprd_cd,
                         cs_ssp.location_cd
         from ods.amis.s1ssp_stu_spk cs_ssp
                  join ods.amis.s1spk_det cs_spk_det
                       on cs_spk_det.spk_no = cs_ssp.spk_no
                           and cs_spk_det.spk_ver_no = cs_ssp.spk_ver_no
                           and cs_spk_det.spk_cat_cd = 'CS'
     ),
     combined_course_offering as (
         select *
         from course_offering
         union
         select *
         from ssp_additional_course_offering
     ),
     final as (
         select md5(
                        ifnull(combined_course_offering.spk_cd, '') || ',' ||
                        ifnull(combined_course_offering.spk_ver_no, 0) || ',' ||
                        ifnull(combined_course_offering.avail_key_no, 0) || ',' ||
                        ifnull(combined_course_offering.avail_yr, 0) || ',' ||
                        ifnull(combined_course_offering.sprd_cd, '') || ',' ||
                        ifnull(combined_course_offering.location_cd, '')
                    )                                       as hub_course_offering_key,
                combined_course_offering.spk_cd,
                combined_course_offering.spk_ver_no,
                combined_course_offering.avail_key_no,
                combined_course_offering.avail_yr,
                combined_course_offering.sprd_cd,
                combined_course_offering.location_cd,
                'AMIS'                                      as source,
                current_timestamp :: timestamp_ntz          as load_dts,
                'SQL' || current_timestamp :: timestamp_ntz as etl_job_id
         from combined_course_offering
         where not exists(
             select null
             from data_vault.core.hub_course_offering h
             where h.hub_course_offering_key = md5(
                     ifnull(combined_course_offering.spk_cd, '') || ',' ||
                     ifnull(combined_course_offering.spk_ver_no, 0) || ',' ||
                     ifnull(combined_course_offering.avail_key_no, 0) || ',' ||
                     ifnull(combined_course_offering.avail_yr, 0) || ',' ||
                     ifnull(combined_course_offering.sprd_cd, '') || ',' ||
                     ifnull(combined_course_offering.location_cd, '')
                 )
             )
     )
select *
from final
;