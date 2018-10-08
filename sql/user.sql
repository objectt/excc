create user 'exchange'@'localhost' identified by 'PASSWORD';
grant create, insert, select on trade_history.* to 'exchange'@'localhost';
grant create, insert, select on trade_log.* to 'exchange'@'localhost';
grant select on exchange.* to 'exchange'@'localhost';
grant insert, update on exchange.wallet to 'exchange'@'localhost';
grant update on exchange.assets to 'exchange'@'localhost';

flush privileges;
