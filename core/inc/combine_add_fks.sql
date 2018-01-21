
/* 
  Add forgeign keys for `core_record` and `core_indexmappingfailure`
*/

ALTER TABLE core_record ADD FOREIGN KEY (job_id) REFERENCES core_job(id) ON DELETE CASCADE;
ALTER TABLE core_indexmappingfailure ADD FOREIGN KEY (job_id) REFERENCES core_job(id) ON DELETE CASCADE;
