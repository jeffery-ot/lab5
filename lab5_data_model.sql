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
  "user_id" uuid UNIQUE PRIMARY KEY NOT NULL
);

CREATE TABLE "e_commerce_db"."orders" (
  "order_num" integer,
  "order_id" integer PRIMARY KEY,
  "user_id" uuid,
  "order_timestamp" timestamp,
  "total_amount" decimal(10,2),
  "order_date" date
);

CREATE TABLE "e_commerce_db"."order_items" (
  "id" integer PRIMARY KEY,
  "order_id" integer,
  "user_id" uuid,
  "days_since_prior_order" int,
  "product_id" integer,
  "add_to_cart_order" varchar,
  "reordered" varchar,
  "order_timestamp" timestamp,
  "order_date" date
);

ALTER TABLE "e_commerce_db"."product_data" ADD FOREIGN KEY ("department_id") REFERENCES "e_commerce_db"."department" ("department_id");

ALTER TABLE "e_commerce_db"."orders" ADD FOREIGN KEY ("user_id") REFERENCES "e_commerce_db"."users" ("user_id");

ALTER TABLE "e_commerce_db"."order_items" ADD FOREIGN KEY ("order_id") REFERENCES "e_commerce_db"."orders" ("order_id");

ALTER TABLE "e_commerce_db"."order_items" ADD FOREIGN KEY ("user_id") REFERENCES "e_commerce_db"."users" ("user_id");

ALTER TABLE "e_commerce_db"."order_items" ADD FOREIGN KEY ("product_id") REFERENCES "e_commerce_db"."product_data" ("product_id");

ALTER TABLE "e_commerce_db"."order_items" ADD FOREIGN KEY ("order_date") REFERENCES "e_commerce_db"."order_items" ("id");
