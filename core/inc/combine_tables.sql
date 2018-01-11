
/* 
	Table creation for `core_record` and `core_indexmappingfailure`

	These are managed outside of Django due to high INSERT/DELETE demands these tables present.
	Deleting rows through Django was prohibitively slow, where using InnoDB's internal
	index for deleting related FKs is quick.
*/


CREATE TABLE `core_record` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `record_id` varchar(1024) DEFAULT NULL,
  `oai_id` varchar(1024) DEFAULT NULL,
  `document` longtext,
  `error` longtext,
  `unique` tinyint(1) NOT NULL,
  `job_id` int(11) NOT NULL,
  `oai_set` varchar(255) DEFAULT NULL,
  `success` tinyint(1) DEFAULT 1 NOT NULL,
  PRIMARY KEY (`id`),
  INDEX `core_record_job_id_idx` (`job_id`),
  INDEX `core_record_job_success_idx` (`success`),
  FOREIGN KEY (job_id) REFERENCES core_job(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8;


CREATE TABLE `core_indexmappingfailure` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `record_id` varchar(1024) DEFAULT NULL,
  `mapping_error` longtext,
  `job_id` int(11) NOT NULL,
  PRIMARY KEY (`id`),
  INDEX `core_indexmappingfailure_job_id_idx` (`job_id`),
  FOREIGN KEY (job_id) REFERENCES core_job(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
