create database crypto_ohlcv;

create user '*'@'%' identified by '**';
grant all privileges on crypto_ohlcv.* to '*'@'%';

create table crypto (cid INT not null AUTO_INCREMENT, 
                     coin varchar(25) not null, 
                     coin_abbr char(3) not null, 
                     market varchar(4) not null,
                     primary key (cid));

create table ohlcv_5 (cid int not null,
                      unix_time int(11) not null,
                      c_open double not null,
                      high double not null,
                      low double  not null,
                      c_close double not null,
                      volume float not null,
                      primary key (cid, unix_time),
                      foreign key (cid) references crypto(cid));

create table ohlcv_15 (cid int not null,
                       unix_time int(11) not null,
                       c_open double not null,
                       high double not null,
                       low double  not null,
                       c_close double not null,
                       volume float not null,
                       primary key (cid, unix_time),
                       foreign key (cid) references crypto(cid));
