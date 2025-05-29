# https://stackoverflow.com/questions/14633547/complex-sql-join-with-5-tables
CREATE TABLE `address` (
  `person_id` int(11) NOT NULL,
  `type_id` int(11) NOT NULL,
  `country_id` int(11) NOT NULL,
  UNIQUE KEY `apt` (`person_id`,`type_id`),
  KEY `apid` (`person_id`),
  KEY `atid` (`type_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

INSERT INTO `address` (`person_id`, `type_id`, `country_id`) VALUES
(1, 1, 1),
(2, 2, 1),
(3, 1, 1),
(3, 2, 2),
(5, 1, 2),
(6, 2, 1),
(7, 1, 1),
(7, 2, 2),
(8, 1, 1),
(9, 2, 1);

CREATE TABLE `address_type` (
  `id` int(11) NOT NULL,
  UNIQUE KEY `tid` (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

INSERT INTO `address_type` (`id`) VALUES
(1),
(2);

CREATE TABLE `option` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `name_id` int(11) NOT NULL,
  `person_id` int(11) NOT NULL,
  UNIQUE KEY `oid` (`id`),
  UNIQUE KEY `onp` (`name_id`,`person_id`),
  KEY `opid` (`person_id`),
  KEY `on` (`name_id`)
) ENGINE=InnoDB  DEFAULT CHARSET=utf8 AUTO_INCREMENT=9 ;

INSERT INTO `option` (`id`, `name_id`, `person_id`) VALUES
(1, 1, 1),
(2, 1, 2),
(3, 1, 3),
(4, 1, 5),
(5, 1, 6),
(6, 1, 7),
(7, 1, 8),
(8, 1, 9);

CREATE TABLE `option_address_type` (
  `option_id` int(11) NOT NULL,
  `type_id` int(11) NOT NULL,
  UNIQUE KEY `ot` (`option_id`,`type_id`),
  KEY `ooid` (`option_id`),
  KEY `otid` (`type_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

INSERT INTO `option_address_type` (`option_id`, `type_id`) VALUES
(1, 1),
(2, 2),
(3, 1),
(3, 2),
(4, 2),
(5, 1),
(6, 1),
(7, 1),
(7, 2),
(8, 1),
(8, 2);

CREATE TABLE `person` (
  `id` int(11) NOT NULL,
  UNIQUE KEY `pid` (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

INSERT INTO `person` (`id`) VALUES
(1),
(2),
(3),
(4),
(5),
(6),
(7),
(8),
(9);


ALTER TABLE `address`
  ADD CONSTRAINT `address_ibfk_1` FOREIGN KEY (`person_id`) REFERENCES `person` (`id`) ON DELETE CASCADE ON UPDATE CASCADE,
  ADD CONSTRAINT `address_ibfk_2` FOREIGN KEY (`type_id`) REFERENCES `address_type` (`id`) ON DELETE CASCADE ON UPDATE CASCADE;

ALTER TABLE `option`
  ADD CONSTRAINT `option_ibfk_1` FOREIGN KEY (`person_id`) REFERENCES `person` (`id`) ON DELETE CASCADE ON UPDATE CASCADE;

ALTER TABLE `option_address_type`
  ADD CONSTRAINT `option_address_type_ibfk_1` FOREIGN KEY (`option_id`) REFERENCES `option` (`id`) ON DELETE CASCADE ON UPDATE CASCADE,
  ADD CONSTRAINT `option_address_type_ibfk_2` FOREIGN KEY (`type_id`) REFERENCES `address_type` (`id`) ON DELETE CASCADE ON UPDATE CASCADE;
