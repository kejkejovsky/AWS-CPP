# Databricks notebook source
# MAGIC %sql
# MAGIC DROP DATABASE IF EXISTS itversity_retail CASCADE

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE itversity_retail

# COMMAND ----------

# MAGIC %sql
# MAGIC USE itversity_retail

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS users

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE users(
# MAGIC   user_id INT,
# MAGIC   user_first_name VARCHAR(30),
# MAGIC   user_last_name VARCHAR(30),
# MAGIC   user_email_id VARCHAR(50),
# MAGIC   user_gender VARCHAR(1),
# MAGIC   user_unique_id VARCHAR(15),
# MAGIC   user_phone_no VARCHAR(20),
# MAGIC   user_dob DATE,
# MAGIC   created_ts TIMESTAMP
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO users
# MAGIC VALUES
# MAGIC     (1, 'Giuseppe', 'Bode', 'gbode0@imgur.com', 'M', '88833-8759', 
# MAGIC      '+86 (764) 443-1967', '1973-05-31', '2018-04-15 12:13:38'),
# MAGIC     (2, 'Lexy', 'Gisbey', 'lgisbey1@mail.ru', 'F', '262501-029', 
# MAGIC      '+86 (751) 160-3742', '2003-05-31', '2020-12-29 06:44:09'),
# MAGIC     (3, 'Karel', 'Claringbold', 'kclaringbold2@yale.edu', 'F', '391-33-2823', 
# MAGIC      '+62 (445) 471-2682', '1985-11-28', '2018-11-19 00:04:08'),
# MAGIC     (4, 'Marv', 'Tanswill', 'mtanswill3@dedecms.com', 'F', '1195413-80', 
# MAGIC      '+62 (497) 736-6802', '1998-05-24', '2018-11-19 16:29:43'),
# MAGIC     (5, 'Gertie', 'Espinoza', 'gespinoza4@nationalgeographic.com', 'M', '471-24-6869', 
# MAGIC      '+249 (687) 506-2960', '1997-10-30', '2020-01-25 21:31:10'),
# MAGIC     (6, 'Saleem', 'Danneil', 'sdanneil5@guardian.co.uk', 'F', '192374-933', 
# MAGIC      '+63 (810) 321-0331', '1992-03-08', '2020-11-07 19:01:14'),
# MAGIC     (7, 'Rickert', 'O''Shiels', 'roshiels6@wikispaces.com', 'M', '749-27-47-52', 
# MAGIC      '+86 (184) 759-3933', '1972-11-01', '2018-03-20 10:53:24'),
# MAGIC     (8, 'Cybil', 'Lissimore', 'clissimore7@pinterest.com', 'M', '461-75-4198', 
# MAGIC      '+54 (613) 939-6976', '1978-03-03', '2019-12-09 14:08:30'),
# MAGIC     (9, 'Melita', 'Rimington', 'mrimington8@mozilla.org', 'F', '892-36-676-2', 
# MAGIC      '+48 (322) 829-8638', '1995-12-15', '2018-04-03 04:21:33'),
# MAGIC     (10, 'Benetta', 'Nana', 'bnana9@google.com', 'M', '197-54-1646', 
# MAGIC      '+420 (934) 611-0020', '1971-12-07', '2018-10-17 21:02:51'),
# MAGIC     (11, 'Gregorius', 'Gullane', 'ggullanea@prnewswire.com', 'F', '232-55-52-58', 
# MAGIC      '+62 (780) 859-1578', '1973-09-18', '2020-01-14 23:38:53'),
# MAGIC     (12, 'Una', 'Glayzer', 'uglayzerb@pinterest.com', 'M', '898-84-336-6', 
# MAGIC      '+380 (840) 437-3981', '1983-05-26', '2019-09-17 03:24:21'),
# MAGIC     (13, 'Jamie', 'Vosper', 'jvosperc@umich.edu', 'M', '247-95-68-44', 
# MAGIC      '+81 (205) 723-1942', '1972-03-18', '2020-07-23 16:39:33'),
# MAGIC     (14, 'Calley', 'Tilson', 'ctilsond@issuu.com', 'F', '415-48-894-3', 
# MAGIC      '+229 (698) 777-4904', '1987-06-12', '2020-06-05 12:10:50'),
# MAGIC     (15, 'Peadar', 'Gregorowicz', 'pgregorowicze@omniture.com', 'M', '403-39-5-869', 
# MAGIC      '+7 (267) 853-3262', '1996-09-21', '2018-05-29 23:51:31'),
# MAGIC     (16, 'Jeanie', 'Webling', 'jweblingf@booking.com', 'F', '399-83-05-03', 
# MAGIC      '+351 (684) 413-0550', '1994-12-27', '2018-02-09 01:31:11'),
# MAGIC     (17, 'Yankee', 'Jelf', 'yjelfg@wufoo.com', 'F', '607-99-0411', 
# MAGIC      '+1 (864) 112-7432', '1988-11-13', '2019-09-16 16:09:12'),
# MAGIC     (18, 'Blair', 'Aumerle', 'baumerleh@toplist.cz', 'F', '430-01-578-5', 
# MAGIC      '+7 (393) 232-1860', '1979-11-09', '2018-10-28 19:25:35'),
# MAGIC     (19, 'Pavlov', 'Steljes', 'psteljesi@macromedia.com', 'F', '571-09-6181', 
# MAGIC      '+598 (877) 881-3236', '1991-06-24', '2020-09-18 05:34:31'),
# MAGIC     (20, 'Darn', 'Hadeke', 'dhadekej@last.fm', 'M', '478-32-02-87', 
# MAGIC      '+370 (347) 110-4270', '1984-09-04', '2018-02-10 12:56:00'),
# MAGIC     (21, 'Wendell', 'Spanton', 'wspantonk@de.vu', 'F', null, 
# MAGIC      '+84 (301) 762-1316', '1973-07-24', '2018-01-30 01:20:11'),
# MAGIC     (22, 'Carlo', 'Yearby', 'cyearbyl@comcast.net', 'F', null, 
# MAGIC      '+55 (288) 623-4067', '1974-11-11', '2018-06-24 03:18:40'),
# MAGIC     (23, 'Sheila', 'Evitts', 'sevittsm@webmd.com', null, '830-40-5287',
# MAGIC      null, '1977-03-01', '2020-07-20 09:59:41'),
# MAGIC     (24, 'Sianna', 'Lowdham', 'slowdhamn@stanford.edu', null, '778-0845', 
# MAGIC      null, '1985-12-23', '2018-06-29 02:42:49'),
# MAGIC     (25, 'Phylys', 'Aslie', 'paslieo@qq.com', 'M', '368-44-4478', 
# MAGIC      '+86 (765) 152-8654', '1984-03-22', '2019-10-01 01:34:28')
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM users LIMIT 10

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT date_format(created_ts, 'yyyy') created_year, count(*) user_count
# MAGIC FROM users
# MAGIC GROUP BY 1
# MAGIC ORDER BY 1 ASC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT user_id, user_dob, user_email_id, date_format(user_dob, 'EEEE') user_day_of_birth
# MAGIC FROM users
# MAGIC WHERE date_format(user_dob, 'MM') = '05'
# MAGIC ORDER BY date_format(user_dob, 'MM-dd') ASC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT user_id, upper(concat_ws(' ', user_first_name, user_last_name)) user_name, user_email_id, created_ts, date_format(created_ts, 'yyyy') created_year
# MAGIC FROM users
# MAGIC WHERE date_format(created_ts, 'yyyy') = '2019'
# MAGIC ORDER BY user_name ASC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT CASE user_gender WHEN 'M' THEN 'Male' WHEN 'F' THEN 'Female' ELSE 'Not specified' END user_gender, count(*) user_count 
# MAGIC FROM users
# MAGIC GROUP BY 1
# MAGIC ORDER BY 2 DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT user_id, user_unique_id, CASE WHEN length(replace(user_unique_id, '-', '')) < 9 THEN 'Invalid Unique Id' ELSE coalesce(substring(replace(user_unique_id, '-', ''), -4, 4), 'Not Specified') END user_unique_id_last4
# MAGIC FROM users
# MAGIC ORDER BY 1 ASC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT replace(substring_index(user_phone_no, ' ', 1), '+', '') country_code, count(*) user_count
# MAGIC FROM users
# MAGIC WHERE user_phone_no IS NOT NULL
# MAGIC GROUP BY 1
# MAGIC ORDER BY CAST(country_code AS INT)

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS users

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE users(
# MAGIC   user_id INT,
# MAGIC   user_fname STRING,
# MAGIC   user_lname STRING,
# MAGIC   user_phones ARRAY<STRING>
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO users
# MAGIC VALUES
# MAGIC (1, 'Scott', 'Tiger', ARRAY('+1 (234) 567 8901', '+1 (123) 456, 7890')),
# MAGIC (2, 'Donald', 'Duck', NULL),
# MAGIC (3, 'Mickey', 'Mouse', ARRAY('+1 (456) 789 0123'))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM users

# COMMAND ----------

# MAGIC %sql
# MAGIC Select user_id, size(user_phones) phones_count
# MAGIC FROM users

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT user_id, nvl2(user_phones, size(user_phones), 0) phones_count
# MAGIC FROM users

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT user_id, explode(user_phones) user_phone
# MAGIC FROM users

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT user_id, explode_outer(user_phones) user_phone
# MAGIC FROM users

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS users

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE users(
# MAGIC   user_id INT,
# MAGIC   user_fname STRING,
# MAGIC   user_lname STRING,
# MAGIC   user_phones STRUCT<home: STRING, mobile: STRING>
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO users
# MAGIC VALUES
# MAGIC (1, 'Scott', 'Tiger', STRUCT('+1 (234) 567 8901', '+1 (123) 456, 7890')),
# MAGIC (2, 'Donald', 'Duck', STRUCT(NULL, NULL)),
# MAGIC (3, 'Mickey', 'Mouse', STRUCT('+1 (456) 789 0123', NULL))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM users

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT user_id, user_phones.* FROM users

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT user_id, user_phones.home user_phone_home, user_phones.mobile user_phone_mobile FROM users

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM PARQUET.`dbfs:/public/retail_db_parquet/order_items`

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TEMPORARY VIEW order_items AS SELECT * FROM PARQUET.`dbfs:/public/retail_db_parquet/order_items`

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT order_item_id,
# MAGIC order_item_order_id,
# MAGIC order_item_product_id,
# MAGIC order_item_subtotal,
# MAGIC STRUCT(order_item_quantity, order_item_product_price) order_item_trans_details
# MAGIC FROM order_items

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS users

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE users(
# MAGIC   user_id INT,
# MAGIC   user_fname STRING,
# MAGIC   user_lname STRING,
# MAGIC   user_phones ARRAY<STRUCT<phone_type: STRING, phone_number: STRING>>
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO users
# MAGIC VALUES
# MAGIC (1, 'Scott', 'Tiger', ARRAY(STRUCT('home','+1 (234) 567 8901'), STRUCT('mobile', '+1 (123) 456, 7890'))),
# MAGIC (2, 'Donald', 'Duck', NULL),
# MAGIC (3, 'Mickey', 'Mouse', ARRAY(STRUCT('home', '+1 (456) 789 0123')))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM users

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT user_id,
# MAGIC explode(user_phones) user_phone
# MAGIC FROM users

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT user_id,
# MAGIC explode(user_phones) user_phone
# MAGIC FROM users

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH user_phones_exploded AS 
# MAGIC (SELECT user_id,
# MAGIC explode_outer(user_phones) user_phone
# MAGIC FROM users)
# MAGIC SELECT user_id, user_phone.*
# MAGIC FROM user_phones_exploded

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH user_phones_exploded AS 
# MAGIC (SELECT user_id,
# MAGIC explode_outer(user_phones) user_phone
# MAGIC FROM users)
# MAGIC SELECT user_id, user_phone.phone_type user_phone_type, user_phone.phone_number user_phone_number
# MAGIC FROM user_phones_exploded

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS users

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE users(
# MAGIC   user_id INT,
# MAGIC   user_fname STRING,
# MAGIC   user_lname STRING,
# MAGIC   user_phone_type STRING,
# MAGIC   user_phone_number STRING
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO users
# MAGIC VALUES
# MAGIC (1, 'Scott', 'Tiger', 'home','+1 (234) 567 8901'),
# MAGIC (1, 'Scott', 'Tiger', 'mobile', '+1 (123) 456, 7890'),
# MAGIC (2, 'Donald', 'Duck', NULL, NULL),
# MAGIC (3, 'Mickey', 'Mouse', 'home', '+1 (456) 789 0123')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM users

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT user_id, user_fname, user_lname, count(*) phone_count
# MAGIC FROM users
# MAGIC GROUP BY 1,2,3

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT user_id, user_fname, user_lname, collect_list(user_phone_number) user_phones
# MAGIC FROM users
# MAGIC GROUP BY 1,2,3

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT user_id, user_fname, user_lname, concat_ws(',', collect_list(user_phone_number)) user_phones
# MAGIC FROM users
# MAGIC GROUP BY 1,2,3

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT user_id, user_fname, user_lname, STRUCT(user_phone_type, user_phone_number) user_phone
# MAGIC FROM users

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT user_id, user_fname, user_lname, nvl2(user_phone_number, STRUCT(user_phone_type, user_phone_number), NULL) user_phone
# MAGIC FROM users

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT user_id, user_fname, user_lname, collect_list(nvl2(user_phone_number, STRUCT(user_phone_type, user_phone_number), NULL)) user_phones
# MAGIC FROM users
# MAGIC GROUP BY 1,2,3

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS users

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE users(
# MAGIC   user_id INT,
# MAGIC   user_fname STRING,
# MAGIC   user_lname STRING,
# MAGIC   user_phone_numbers STRING
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO users
# MAGIC VALUES
# MAGIC (1, 'Scott', 'Tiger', '+1 (234) 567 8901,+1 (123) 456 7890'),
# MAGIC (2, 'Donald', 'Duck', NULL),
# MAGIC (3, 'Mickey', 'Mouse', '+1 (456) 789 0123')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM users

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT user_id, user_fname, user_lname, split(user_phone_numbers, ',') user_phones
# MAGIC FROM users

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT user_id, user_fname, user_lname, explode(split(user_phone_numbers, ',')) user_phone
# MAGIC FROM users

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT user_id, user_fname, user_lname, explode_outer(split(user_phone_numbers, ',')) user_phone
# MAGIC FROM users

# COMMAND ----------

