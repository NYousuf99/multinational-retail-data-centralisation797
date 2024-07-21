
--milestone 3 task 10

SELECT MAX(LENGTH(card_number::varchar)) AS max_card_number_length,
       MAX(LENGTH(store_code::varchar)) AS max_store_code_length,
       MAX(LENGTH(product_code::varchar)) AS max_product_code_length
FROM public.orders_table;

ALTER TABLE public.orders_table 
ALTER COLUMN date_uuid TYPE uuid using date_uuid::uuid,
ALTER COLUMN user_uuid TYPE uuid using user_uuid::uuid,
ALTER COLUMN card_number TYPE varchar(19),
ALTER COLUMN store_code TYPE varchar(12),
ALTER COLUMN product_code TYPE varchar(11),
ALTER COLUMN product_quantity TYPE smallint;

