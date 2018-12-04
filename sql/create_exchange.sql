CREATE DATABASE exchange;
USE exchange;

CREATE TABLE `assets` (
  `id` int(5) unsigned NOT NULL AUTO_INCREMENT,
  `name` varchar(8) NOT NULL DEFAULT '',
  `prec_save` tinyint(4) unsigned NOT NULL DEFAULT '8',
  `prec_show` tinyint(4) unsigned NOT NULL DEFAULT '4',
  `min_amount` decimal(10,8) unsigned NOT NULL DEFAULT '0.0001',
  `is_listed` tinyint(1) unsigned NOT NULL DEFAULT '0',
  `listing_date` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `IDX_UNIQUE_ASSET_NAME` (`name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE `market` (
  `id` int(5) unsigned NOT NULL AUTO_INCREMENT,
  `stock` int(5) unsigned NOT NULL,
  `currency` int(5) unsigned NOT NULL DEFAULT '1',
  `fee_prec` tinyint(1) unsigned NOT NULL DEFAULT '2',
  `min_amount` decimal(10,8) unsigned NOT NULL DEFAULT '0.0001',
  `init_price` decimal(10,8) unsigned NOT NULL DEFAULT '0',
  `listing_date` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `delisting_date` timestamp NULL DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE `wallet` (
  `user_id` int(11) unsigned NOT NULL,
  `asset_id` int(5) unsigned NOT NULL DEFAULT '0',
  `blended` decimal(30,8) unsigned NOT NULL DEFAULT '0',
  `purchased` decimal(30,8) unsigned NOT NULL DEFAULT '0',
  `update_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`user_id`,`asset_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

INSERT INTO assets (name, prec_save, prec_show, min_amount, is_listed) VALUES ('CHU', 8, 4, 0, 0);