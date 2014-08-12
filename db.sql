CREATE TABLE `domains` (
  `name` varchar(20) NOT NULL,
  `username` varchar(255) NOT NULL,
  `password` varchar(255) NOT NULL,
  `uid` varchar(64) NOT NULL,
  PRIMARY KEY (`name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE `project_invitations` (
  `pid` char(32) NOT NULL,
  `sender` varchar(255) NOT NULL,
  `created_time` datetime NOT NULL,
  `accepted_time` datetime DEFAULT NULL,
  `status` varchar(10) DEFAULT NULL,
  `error` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`pid`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE `projects` (
  `pid` char(32) NOT NULL,
  `project_id` int(10) unsigned NOT NULL,
  `writer_id` varchar(50) NOT NULL,
  `created_time` datetime NOT NULL,
  `access_token` varchar(64) DEFAULT NULL,
  `removal_time` datetime DEFAULT NULL,
  `keep_on_removal` tinyint(1) unsigned DEFAULT '0',
  `deleted_time` datetime DEFAULT NULL,
  PRIMARY KEY (`pid`),
  KEY `project_id` (`project_id`,`writer_id`,`removal_time`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE `users` (
  `uid` char(50) NOT NULL,
  `email` varchar(255) NOT NULL,
  `project_id` int(10) unsigned NOT NULL,
  `writer_id` varchar(50) NOT NULL,
  `created_time` datetime NOT NULL,
  `removal_time` datetime DEFAULT NULL,
  `deleted_time` datetime DEFAULT NULL,
  PRIMARY KEY (`uid`),
  KEY `email` (`email`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE `writers` (
  `project_id` int(11) unsigned NOT NULL,
  `writer_id` varchar(50) NOT NULL DEFAULT '',
  `bucket` varchar(100) DEFAULT NULL,
  `status` enum('preparing','ready','error','maintenance','deleted') NOT NULL DEFAULT 'preparing',
  `token_id` int(10) unsigned NOT NULL,
  `token_desc` varchar(255) NOT NULL DEFAULT '',
  `created_time` datetime NOT NULL,
  `deleted_time` datetime DEFAULT NULL,
  `info` text,
  `cl_tool` tinyint(1) NOT NULL DEFAULT '0',
  `date_facts` tinyint(1) NOT NULL DEFAULT '0',
  PRIMARY KEY (`project_id`,`writer_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;