
-- 1. Transform ADA-BUSD.csv
crypto = LOAD 'gs://assignment1_resources/crypto/ADA-BUSD.csv' using PigStorage(',') AS (open_time:long,open:double,high:double,low:double,close:double,volume:double,close_time:long,quote_asset_volume:double,number_of_trades:int);

crypto1 = FOREACH crypto GENERATE
	'ADA' as currency, 
	ToDate(open_time) as (open_date_time:DateTime),
	open,
	high,
	low,
	close,
	volume,
	ToDate(close_time) as (close_date_time:DateTime),
	number_of_trades;
	
	
	
crypto2 = FOREACH crypto1 GENERATE currency,
SUBSTRING(ToString(open_date_time),0,10),
SUBSTRING(ToString(open_date_time),11,19),
SUBSTRING(ToString(close_date_time),11,19),
open,high,low,close,volume,number_of_trades;

STORE crypto2 INTO 'gs://assignment1_resources/crypto/data/ADA' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',','NO_MULTILINE','WINDOWS');



	
-- 2. Transform BCH-BUSD.csv
crypto = LOAD 'gs://assignment1_resources/crypto/BCH-BUSD.csv' using PigStorage(',') AS (open_time:long,open:double,high:double,low:double,close:double,volume:double,close_time:long,quote_asset_volume:double,number_of_trades:int);

crypto1 = FOREACH crypto GENERATE
	'BCH' as currency, 
	ToDate(open_time) as (open_date_time:DateTime),
	open,
	high,
	low,
	close,
	volume,
	ToDate(close_time) as (close_date_time:DateTime),
	number_of_trades;
	
	
	
crypto2 = FOREACH crypto1 GENERATE currency,
SUBSTRING(ToString(open_date_time),0,10),
SUBSTRING(ToString(open_date_time),11,19),
SUBSTRING(ToString(close_date_time),11,19),
open,high,low,close,volume,number_of_trades;

STORE crypto2 INTO 'gs://assignment1_resources/crypto/data/BCH' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',','NO_MULTILINE','WINDOWS');






-- 3. Transform BNB-BUSD.csv
crypto = LOAD 'gs://assignment1_resources/crypto/BNB-BUSD.csv' using PigStorage(',') AS (open_time:long,open:double,high:double,low:double,close:double,volume:double,close_time:long,quote_asset_volume:double,number_of_trades:int);

crypto1 = FOREACH crypto GENERATE
	'BNB' as currency, 
	ToDate(open_time) as (open_date_time:DateTime),
	open,
	high,
	low,
	close,
	volume,
	ToDate(close_time) as (close_date_time:DateTime),
	number_of_trades;
	
	
	
crypto2 = FOREACH crypto1 GENERATE currency,
SUBSTRING(ToString(open_date_time),0,10),
SUBSTRING(ToString(open_date_time),11,19),
SUBSTRING(ToString(close_date_time),11,19),
open,high,low,close,volume,number_of_trades;

STORE crypto2 INTO 'gs://assignment1_resources/crypto/data/BNB' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',','NO_MULTILINE','WINDOWS');




-- 4. Transform BTC-BUSD.csv
crypto = LOAD 'gs://assignment1_resources/crypto/BTC-BUSD.csv' using PigStorage(',') AS (open_time:long,open:double,high:double,low:double,close:double,volume:double,close_time:long,quote_asset_volume:double,number_of_trades:int);

crypto1 = FOREACH crypto GENERATE
	'BTC' as currency, 
	ToDate(open_time) as (open_date_time:DateTime),
	open,
	high,
	low,
	close,
	volume,
	ToDate(close_time) as (close_date_time:DateTime),
	number_of_trades;
	
	
	
crypto2 = FOREACH crypto1 GENERATE currency,
SUBSTRING(ToString(open_date_time),0,10),
SUBSTRING(ToString(open_date_time),11,19),
SUBSTRING(ToString(close_date_time),11,19),
open,high,low,close,volume,number_of_trades;

STORE crypto2 INTO 'gs://assignment1_resources/crypto/data/BTC' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',','NO_MULTILINE','WINDOWS');


-- 5. Transform DOGE-BUSD.csv
crypto = LOAD 'gs://assignment1_resources/crypto/DOGE-BUSD.csv' using PigStorage(',') AS (open_time:long,open:double,high:double,low:double,close:double,volume:double,close_time:long,quote_asset_volume:double,number_of_trades:int);

crypto1 = FOREACH crypto GENERATE
	'DOGE' as currency, 
	ToDate(open_time) as (open_date_time:DateTime),
	open,
	high,
	low,
	close,
	volume,
	ToDate(close_time) as (close_date_time:DateTime),
	number_of_trades;
	
	
	
crypto2 = FOREACH crypto1 GENERATE currency,
SUBSTRING(ToString(open_date_time),0,10),
SUBSTRING(ToString(open_date_time),11,19),
SUBSTRING(ToString(close_date_time),11,19),
open,high,low,close,volume,number_of_trades;

STORE crypto2 INTO 'gs://assignment1_resources/crypto/data/DOGE' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',','NO_MULTILINE','WINDOWS');



-- 6. Transform EOS-BUSD.csv
crypto = LOAD 'gs://assignment1_resources/crypto/EOS-BUSD.csv' using PigStorage(',') AS (open_time:long,open:double,high:double,low:double,close:double,volume:double,close_time:long,quote_asset_volume:double,number_of_trades:int);

crypto1 = FOREACH crypto GENERATE
	'EOS' as currency, 
	ToDate(open_time) as (open_date_time:DateTime),
	open,
	high,
	low,
	close,
	volume,
	ToDate(close_time) as (close_date_time:DateTime),
	number_of_trades;
	
	
	
crypto2 = FOREACH crypto1 GENERATE currency,
SUBSTRING(ToString(open_date_time),0,10),
SUBSTRING(ToString(open_date_time),11,19),
SUBSTRING(ToString(close_date_time),11,19),
open,high,low,close,volume,number_of_trades;

STORE crypto2 INTO 'gs://assignment1_resources/crypto/data/EOS' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',','NO_MULTILINE','WINDOWS');




-- 7. Transform ETC-BUSD.csv
crypto = LOAD 'gs://assignment1_resources/crypto/ETC-BUSD.csv' using PigStorage(',') AS (open_time:long,open:double,high:double,low:double,close:double,volume:double,close_time:long,quote_asset_volume:double,number_of_trades:int);

crypto1 = FOREACH crypto GENERATE
	'ETC' as currency, 
	ToDate(open_time) as (open_date_time:DateTime),
	open,
	high,
	low,
	close,
	volume,
	ToDate(close_time) as (close_date_time:DateTime),
	number_of_trades;
	
	
	
crypto2 = FOREACH crypto1 GENERATE currency,
SUBSTRING(ToString(open_date_time),0,10),
SUBSTRING(ToString(open_date_time),11,19),
SUBSTRING(ToString(close_date_time),11,19),
open,high,low,close,volume,number_of_trades;

STORE crypto2 INTO 'gs://assignment1_resources/crypto/data/ETC' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',','NO_MULTILINE','WINDOWS');
	


-- 8. Transform ETH-BUSD.csv
crypto = LOAD 'gs://assignment1_resources/crypto/ETH-BUSD.csv' using PigStorage(',') AS (open_time:long,open:double,high:double,low:double,close:double,volume:double,close_time:long,quote_asset_volume:double,number_of_trades:int);

crypto1 = FOREACH crypto GENERATE
	'ETH' as currency, 
	ToDate(open_time) as (open_date_time:DateTime),
	open,
	high,
	low,
	close,
	volume,
	ToDate(close_time) as (close_date_time:DateTime),
	number_of_trades;
	
	
	
crypto2 = FOREACH crypto1 GENERATE currency,
SUBSTRING(ToString(open_date_time),0,10),
SUBSTRING(ToString(open_date_time),11,19),
SUBSTRING(ToString(close_date_time),11,19),
open,high,low,close,volume,number_of_trades;

STORE crypto2 INTO 'gs://assignment1_resources/crypto/data/ETH' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',','NO_MULTILINE','WINDOWS');


-- 9. Transform IOTA-BUSD.csv
crypto = LOAD 'gs://assignment1_resources/crypto/IOTA-BUSD.csv' using PigStorage(',') AS (open_time:long,open:double,high:double,low:double,close:double,volume:double,close_time:long,quote_asset_volume:double,number_of_trades:int);

crypto1 = FOREACH crypto GENERATE
	'IOTA' as currency, 
	ToDate(open_time) as (open_date_time:DateTime),
	open,
	high,
	low,
	close,
	volume,
	ToDate(close_time) as (close_date_time:DateTime),
	number_of_trades;
	
	
	
crypto2 = FOREACH crypto1 GENERATE currency,
SUBSTRING(ToString(open_date_time),0,10),
SUBSTRING(ToString(open_date_time),11,19),
SUBSTRING(ToString(close_date_time),11,19),
open,high,low,close,volume,number_of_trades;

STORE crypto2 INTO 'gs://assignment1_resources/crypto/data/IOTA' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',','NO_MULTILINE','WINDOWS');




-- 10. Transform LTC-BUSD.csv
crypto = LOAD 'gs://assignment1_resources/crypto/LTC-BUSD.csv' using PigStorage(',') AS (open_time:long,open:double,high:double,low:double,close:double,volume:double,close_time:long,quote_asset_volume:double,number_of_trades:int);

crypto1 = FOREACH crypto GENERATE
	'LTC' as currency, 
	ToDate(open_time) as (open_date_time:DateTime),
	open,
	high,
	low,
	close,
	volume,
	ToDate(close_time) as (close_date_time:DateTime),
	number_of_trades;
	
	
	
crypto2 = FOREACH crypto1 GENERATE currency,
SUBSTRING(ToString(open_date_time),0,10),
SUBSTRING(ToString(open_date_time),11,19),
SUBSTRING(ToString(close_date_time),11,19),
open,high,low,close,volume,number_of_trades;

STORE crypto2 INTO 'gs://assignment1_resources/crypto/data/LTC' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',','NO_MULTILINE','WINDOWS');




-- 11. Transform MKR-BUSD.csv
crypto = LOAD 'gs://assignment1_resources/crypto/MKR-BUSD.csv' using PigStorage(',') AS (open_time:long,open:double,high:double,low:double,close:double,volume:double,close_time:long,quote_asset_volume:double,number_of_trades:int);

crypto1 = FOREACH crypto GENERATE
	'MKR' as currency, 
	ToDate(open_time) as (open_date_time:DateTime),
	open,
	high,
	low,
	close,
	volume,
	ToDate(close_time) as (close_date_time:DateTime),
	number_of_trades;
	
	
	
crypto2 = FOREACH crypto1 GENERATE currency,
SUBSTRING(ToString(open_date_time),0,10),
SUBSTRING(ToString(open_date_time),11,19),
SUBSTRING(ToString(close_date_time),11,19),
open,high,low,close,volume,number_of_trades;

STORE crypto2 INTO 'gs://assignment1_resources/crypto/data/MKR' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',','NO_MULTILINE','WINDOWS');



-- 12. Transform TRX-BUSD.csv
crypto = LOAD 'gs://assignment1_resources/crypto/TRX-BUSD.csv' using PigStorage(',') AS (open_time:long,open:double,high:double,low:double,close:double,volume:double,close_time:long,quote_asset_volume:double,number_of_trades:int);

crypto1 = FOREACH crypto GENERATE
	'TRX' as currency, 
	ToDate(open_time) as (open_date_time:DateTime),
	open,
	high,
	low,
	close,
	volume,
	ToDate(close_time) as (close_date_time:DateTime),
	number_of_trades;
	
	
	
crypto2 = FOREACH crypto1 GENERATE currency,
SUBSTRING(ToString(open_date_time),0,10),
SUBSTRING(ToString(open_date_time),11,19),
SUBSTRING(ToString(close_date_time),11,19),
open,high,low,close,volume,number_of_trades;

STORE crypto2 INTO 'gs://assignment1_resources/crypto/data/TRX' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',','NO_MULTILINE','WINDOWS');




-- 13. Transform XLM-BUSD.csv
crypto = LOAD 'gs://assignment1_resources/crypto/XLM-BUSD.csv' using PigStorage(',') AS (open_time:long,open:double,high:double,low:double,close:double,volume:double,close_time:long,quote_asset_volume:double,number_of_trades:int);

crypto1 = FOREACH crypto GENERATE
	'XLM' as currency, 
	ToDate(open_time) as (open_date_time:DateTime),
	open,
	high,
	low,
	close,
	volume,
	ToDate(close_time) as (close_date_time:DateTime),
	number_of_trades;
	
	
	
crypto2 = FOREACH crypto1 GENERATE currency,
SUBSTRING(ToString(open_date_time),0,10),
SUBSTRING(ToString(open_date_time),11,19),
SUBSTRING(ToString(close_date_time),11,19),
open,high,low,close,volume,number_of_trades;

STORE crypto2 INTO 'gs://assignment1_resources/crypto/data/XLM' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',','NO_MULTILINE','WINDOWS');



-- 14. Transform XMR-BUSD.csv
crypto = LOAD 'gs://assignment1_resources/crypto/XMR-BUSD.csv' using PigStorage(',') AS (open_time:long,open:double,high:double,low:double,close:double,volume:double,close_time:long,quote_asset_volume:double,number_of_trades:int);

crypto1 = FOREACH crypto GENERATE
	'XMR' as currency, 
	ToDate(open_time) as (open_date_time:DateTime),
	open,
	high,
	low,
	close,
	volume,
	ToDate(close_time) as (close_date_time:DateTime),
	number_of_trades;
	
	
	
crypto2 = FOREACH crypto1 GENERATE currency,
SUBSTRING(ToString(open_date_time),0,10),
SUBSTRING(ToString(open_date_time),11,19),
SUBSTRING(ToString(close_date_time),11,19),
open,high,low,close,volume,number_of_trades;

STORE crypto2 INTO 'gs://assignment1_resources/crypto/data/XMR' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',','NO_MULTILINE','WINDOWS');


