create table adm_landing.kaggle_csv_data 
(
time datetime,
open double(10,8),
close double(10,8),
high double(10,8),
low double(10,8),
volume double(20,5),
cc_id int(3)
);

create table adm_landing.coin_currency_mapping
(
coin_symbol varchar(5),
currency varchar(5),
cc_id int(5) NOT NULL,
PRIMARY KEY (cc_id)
);

LOAD DATA LOCAL INFILE '/home/centos/adm/data/cmc_csv_data/coin_currency_mapping.csv' INTO TABLE adm_landing.coin_currency_mapping FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' IGNORE 1 LINES;


create table adm_landing.cmc_url_data
(
cmc_id int(5) NOT NULL,
coin_logo varchar(50),
coin_website varchar(50),
PRIMARY KEY (cmc_id)
);
LOAD DATA LOCAL INFILE '/home/centos/adm/data/cmc_csv_data/urldata.csv' INTO TABLE adm_landing.cmc_url_data FIELDS TERMINATED BY ',' LINES TERMINATED BY '\r\n' ;

create table adm_landing.cmc_listing_data 
(
cmc_id int(5) NOT NULL,
cmc_rank int(5),
num_market_pairs int(5),
circulating_supply bigint(15),
total_supply bigint(15),
max_supply bigint(15),
price  decimal(20,6),
volume_24h decimal(20,5),
volume_24h_reported decimal(20,5),
volume_7d decimal(20,5),
volume_7d_reported decimal(20,5),
volume_30d decimal(20,5),
volume_30d_reported decimal(20,5),
percent_change_1h decimal(10,5),
percent_change_24h decimal(10,5),
percent_change_7d decimal(10,5),
market_cap bigint(15),
last_updated varchar(35),
PRIMARY KEY (cmc_id)
);
LOAD DATA LOCAL INFILE '/home/centos/adm/data/cmc_csv_data/quotedata.csv' INTO TABLE adm_landing.cmc_listing_data FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' ;

create table adm_landing.cmc_meta_api_data
(
cmc_id int(5) NOT NULL,
name varchar(30),
symbol varchar(5),
slug varchar(30),
is_active varchar(5),
first_historical_data varchar(35),
last_historical_data varchar(35),
PRIMARY KEY (cmc_id)
);
LOAD DATA LOCAL INFILE '/home/centos/adm/data/cmc_csv_data/meta.csv' INTO TABLE adm_landing.cmc_meta_api_data  FIELDS TERMINATED BY ',' LINES TERMINATED BY '\r\n' ;


select a.cc_id,a.coin_symbol,a.currency,b.cmc_id,b.name from adm_landing.coin_currency_mapping a left join adm_landing.cmc_meta_api_data b on upper(a.coin_symbol) = upper(b.symbol);

select time, cast(time  as from_unixtime(floor(time/1000)) from kaggle_csv_data limit 10;


create table adm_working.kaggle_final_data like adm_landing.kaggle_csv_data;
insert into adm_working.kaggle_final_data select * from adm_landing.kaggle_csv_data  where time >= "2019-07-01 00:00:00";

create table adm_landing.git_commits(
name varchar(35),
git_commits int(8)
);
LOAD DATA LOCAL INFILE '/home/centos/adm/data/cmc_csv_data/git_commit_data.csv' INTO TABLE adm_landing.git_commits FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' IGNORE 1 LINES;

create table adm_working.coin_git_commits
(
cmc_id int(5) NOT NULL,
name varchar(30),
symbol varchar(5),
git_commits int (8)
);

INSERT INTO adm_working.coin_git_commits
SELECT a.cmc_id, b.name, a.symbol, b.git_commits
FROM adm_landing.cmc_meta_api_data a
LEFT JOIN adm_landing.git_commits b ON lower(a.name)=lower(b.name);

create table adm_working.twitter_data (
cmc_id int(5) NOT NULL,
name varchar (30),
symbol varchar(5),
twitter_url varchar(50),
twitter_followers int(11),
twitter_rem varchar(30),
PRIMARY KEY (cmc_id));


LOAD DATA LOCAL INFILE "/home/centos/adm/data/twitter_followers.csv" INTO TABLE adm_working.twitter_data FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n';

create table adm_working.weights
(
volatality int(5),
volume int(5),
market_cap int(5),
7d_price_change int(5),
git_updates int(5),
cmc_rank int(5),
twitter_followers int(5)
);

create table adm_working.normalized_data
(
market_cap decimal(10,7),
volume_7d decimal(10,7),
volatility decimal(10,7),
git_commits decimal(10,7),
twitter_followers decimal(10,7),
cmc_rank int(5),
local_rank int(5),
rank_normalized decimal(10,7),
cmc_id int(5) not null,
name varchar(35),
symbol varchar(10)
);

LOAD DATA LOCAL INFILE "/home/centos/adm/data/normalized_data.csv" INTO TABLE adm_working.normalized_data FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' IGNORE 1 LINES;

create table adm_working.recommendation as 
select 
a.cmc_id, 
a.name,
a.symbol,
(a.rank_normalized*(b.cmc_rank)/100) + (a.market_cap*(b.market_cap)/100) + (a.volume_7d*(b.volume)/100) + (a.volatility*(b.volatality)/100) + (a.git_commits*(b.git_updates)/100) + (a.twitter_followers*(b.twitter_followers)/100) as score 
from adm_working.normalized_data a 
inner join 
adm_working.weights b
order by score desc; 

CREATE TABLE adm_working.volatility_data as 
(Select cc_id, stddev(m.close)/avg(m.close) as volatility 
from (select cc_id,DATE(time),max(close) as close from kaggle_final_data 
group by 1,2 
order by 1 asc, 2 desc) m  
group by 1);


CREATE TABLE adm_working.volatility_final_data as(
select cc_id, coin_symbol, volatility
from volatility_data vd
join adm_landing.coin_currency_mapping cc
on vd.cc_id = cc.cc_id
where cc.currency = 'usd'
);

CREATE TABLE crypto_stage_data as (
select cc.cmc_id,cc.name,cc.symbol,cl.cmc_rank, market_cap, volume_7d,volatility,git_commits,twitter_followers 
from cmc_listing_data cl 
join cmc_meta_api_data cc on cl.cmc_id = cc.cmc_id 
join git_commits gc  on cc.name = gc.name 
join adm_working.volatility_final_data vf on cc.symbol = vf.coin_symbol 
join adm_working.twitter_data td on cc.cmc_id = td.cmc_id
);