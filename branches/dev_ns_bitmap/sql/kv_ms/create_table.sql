CREATE TABLE `tfsmeta_1` (
  `meta_key` varbinary(750) NOT NULL DEFAULT '',
  `meta_value` blob NOT NULL,
  `version` int(10) unsigned DEFAULT NULL,
  PRIMARY KEY (`meta_key`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1
