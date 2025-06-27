CREATE SCHEMA "e_commerce_db";

CREATE TABLE "product_data" (
  "product_id" integer PRIMARY KEY,
  "department_id" integer,
  "department" varchar,
  "product_name" varchar
);

CREATE TABLE "orders" (
  "order_num" varchar,
  "order_id" integer PRIMARY KEY,
  "user_id" integer,
  "order_timestamp" timestamp,
  "total_amount" decimal(10,2),
  "date" date
);

CREATE TABLE "order_items" (
  "id" integer PRIMARY KEY,
  "order_id" integer,
  "user_id" integer,
  "days_since_prior_order" integer,
  "product_id" integer,
  "add_to_cart_order" integer,
  "reordered" boolean,
  "order_timestamp" timestamp
);

CREATE TABLE "e_commerce_db"."product_data" (
  "product_id" integer PRIMARY KEY,
  "category_id" integer,
  "product_name" varchar
);

CREATE TABLE "e_commerce_db"."category" (
  "category_id" integer PRIMARY KEY,
  "category" varchar
);

CREATE TABLE "e_commerce_db"."users" (
  "user_id" int PRIMARY KEY
);

CREATE TABLE "e_commerce_db"."orders" (
  "order_qty" integer,
  "order_id" integer PRIMARY KEY,
  "user_id" int,
  "order_timestamp" timestamp,
  "total_amount" decimal(10,2)
);

CREATE TABLE "e_commerce_db"."order_items" (
  "id" integer PRIMARY KEY,
  "order_id" integer,
  "days_since_prior_order" int,
  "product_id" integer,
  "add_to_cart_order" int,
  "reordered" bool
);

ALTER TABLE "e_commerce_db"."product_data" ADD FOREIGN KEY ("category_id") REFERENCES "e_commerce_db"."category" ("category_id");

ALTER TABLE "e_commerce_db"."orders" ADD FOREIGN KEY ("user_id") REFERENCES "e_commerce_db"."users" ("user_id");

ALTER TABLE "e_commerce_db"."order_items" ADD FOREIGN KEY ("order_id") REFERENCES "e_commerce_db"."orders" ("order_id");

ALTER TABLE "e_commerce_db"."order_items" ADD FOREIGN KEY ("product_id") REFERENCES "e_commerce_db"."product_data" ("product_id");
