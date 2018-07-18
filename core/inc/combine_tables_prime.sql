
/* 
	Table creation for `core_record` and `core_indexmappingfailure`

	These are managed outside of Django due to high INSERT/DELETE demands these tables present.
	Deleting rows through Django was prohibitively slow, where using InnoDB's internal
	index for deleting related FKs is quick.

  NOTE: Due to order of operations for initial Django migrations, these are created without 
  foreign keys.  These FKs are added with combine_add_fks.sql after initial migrations are complete.
*/


CREATE TABLE `core_record` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `combine_id` varchar(1024) DEFAULT NULL,
  `record_id` varchar(1024) DEFAULT NULL,
  `document` longtext,
  `error` longtext,
  `unique` tinyint(1) NOT NULL,
  `unique_published` tinyint(1) DEFAULT NULL,
  `job_id` int(11) NOT NULL,
  `published` tinyint(1) DEFAULT 0,
  `publish_set_id` varchar(128) DEFAULT NULL,
  `oai_set` varchar(255) DEFAULT NULL,
  `success` tinyint(1) DEFAULT 1,
  `valid` tinyint(1) DEFAULT 1,
  `fingerprint` bigint(64) DEFAULT NULL,
  `transformed` tinyint(1) DEFAULT 0,  
  PRIMARY KEY (`id`),
  INDEX `core_record_job_id_idx` (`job_id`),
  INDEX `core_record_job_success_idx` (`success`),
  INDEX `core_record_job_valid_idx` (`valid`),
  INDEX `core_record_job_published_idx` (`published`),
  INDEX `core_record_job_fingerprint_idx` (`fingerprint`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;


CREATE TABLE `core_indexmappingfailure` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `combine_id` varchar(1024) DEFAULT NULL,
  `mapping_error` longtext,
  `job_id` int(11) NOT NULL,
  PRIMARY KEY (`id`),
  INDEX `core_indexmappingfailure_job_id_idx` (`job_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
