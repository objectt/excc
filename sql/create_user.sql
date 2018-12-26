--create user 'exchange'@'localhost' identified by 'PASSWORD';
grant create, insert, select on trade_history.* to 'exchange'@'localhost';
grant create, drop, insert, select on trade_log.* to 'exchange'@'localhost';
grant select, insert, update on exchange.* to 'exchange'@'localhost';
grant delete on trade_log.slice_history to 'exchange'@'localhost';

flush privileges;
