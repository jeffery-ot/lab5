CREATE SCHEMA "e_commerce_db";

CREATE TABLE "e_commerce_db"."product_data" (
  "product_id" integer PRIMARY KEY,
  "department_id" integer,
  "product_name" varchar
);

CREATE TABLE "e_commerce_db"."department" (
  "department_id" integer PRIMARY KEY,
  "department" varchar
);

CREATE TABLE "e_commerce_db"."users" (
  "user_id" int PRIMARY KEY
);

CREATE TABLE "e_commerce_db"."orders" (
  "order_num" integer,
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

ALTER TABLE "e_commerce_db"."product_data" ADD FOREIGN KEY ("department_id") REFERENCES "e_commerce_db"."department" ("department_id");

ALTER TABLE "e_commerce_db"."orders" ADD FOREIGN KEY ("user_id") REFERENCES "e_commerce_db"."users" ("user_id");

ALTER TABLE "e_commerce_db"."order_items" ADD FOREIGN KEY ("order_id") REFERENCES "e_commerce_db"."orders" ("order_id");

ALTER TABLE "e_commerce_db"."order_items" ADD FOREIGN KEY ("product_id") REFERENCES "e_commerce_db"."product_data" ("product_id");
