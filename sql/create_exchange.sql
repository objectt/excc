CREATE DATABASE exchange;
USE exchange;

CREATE TABLE `assets` (
  `id` int(8) unsigned NOT NULL AUTO_INCREMENT,
  `symbol` varchar(10) NOT NULL,
  `name` varchar(25) NOT NULL,
  `prec_save` tinyint(4) unsigned NOT NULL DEFAULT '8',
  `prec_show` tinyint(4) unsigned NOT NULL DEFAULT '4',
  `tick_size` decimal(18,8) unsigned NOT NULL DEFAULT '100',
  `is_listed` tinyint(1) unsigned NOT NULL DEFAULT '0',
  PRIMARY KEY (`id`),
  UNIQUE KEY `IDX_UNIQUE_ASSET_symbol` (`symbol`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE `market` (
  `id` int(8) unsigned NOT NULL AUTO_INCREMENT,
  `symbol` varchar(10) NOT NULL,
  `name` varchar(25) NOT NULL,
  `stock` int(8) unsigned NOT NULL,
  `currency` int(8) unsigned NOT NULL DEFAULT '1',
  `fee` int(8) unsigned NOT NULL DEFAULT '1',
  `fee_prec` tinyint(1) unsigned NOT NULL DEFAULT '4',
  `min_amount` decimal(18,8) unsigned NOT NULL DEFAULT '1',
  `max_amount` decimal(18,8) unsigned NOT NULL DEFAULT '100',
  `min_price` decimal(18,8) unsigned NOT NULL DEFAULT '500',
  `max_price` decimal(18,8) unsigned NOT NULL DEFAULT '10000000',
  `min_total` decimal(18,8) unsigned NOT NULL DEFAULT '500',
  `delisting_ts` int(11) unsigned NOT NULL,
  `init_price` decimal(18,8) unsigned NOT NULL DEFAULT '100',
  `is_listed` tinyint(1) unsigned NOT NULL DEFAULT '0',
  `closing_price` decimal(18,8) unsigned NOT NULL DEFAULT '100',
  `update_time` timestamp NULL DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `IDX_UNIQUE_MARKET_symbol` (`symbol`),
  UNIQUE KEY `IDX_UNIQUE_MARKET_PAIR` (`stock`, `currency`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

INSERT INTO assets (symbol, name)
    VALUES ('KRW', 'Korean Won');
INSERT INTO assets (symbol, name, prec_save, prec_show, tick_size, is_listed)
    VALUES ('BTC', 'Bitcoin', 8, 4, 0.0001, 1);
INSERT INTO assets (symbol, name, prec_save, prec_show, tick_size, is_listed)
    VALUES ('ETH', 'Ethereum', 8, 4, 0.0001, 1);
INSERT INTO market (symbol, name, stock, currency, min_amount, max_amount, min_price, max_price, delisting_ts, init_price, closing_price)
    VALUES ('BTC', 'BTC/KRW', 2, 1, 0.0001, 0, 0, 0, 0, 5000000, 5000000);
