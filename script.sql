-- Создание таблицы-факта sales
CREATE TABLE sales (
    datetime timestamp,
    id_product int,
    cheque_id int,
    shop_id int,
    quantity int
)
DISTRIBUTED BY (id_product) PARTITION BY RANGE (datetime)
(
    START (DATE '2023-08-01') END (DATE '2023-09-01'),
    START (DATE '2023-09-01') END (DATE '2023-10-01'),
    START (DATE '2023-10-01') END (DATE '2023-11-01')
);



-- Создание таблицы-измерения product
CREATE TABLE product (
    product_id serial PRIMARY KEY,
    product_name text,
    price numeric(10, 2)
);

-- Заполнение таблицы-измерения данными (произвольными товарами)
INSERT INTO product (product_name, price)
VALUES
    ('Product 1', 10.99),
    ('Product 2', 19.99),
    ('Product 3', 5.99);

-- Заполнение таблицы-факта данными на 1 августа только
INSERT INTO sales (datetime, id_product, cheque_id, shop_id, quantity)
SELECT
    TIMESTAMP '2023-08-01 10:00:00' + (random() * 90) * interval '1 minute',
    (random() * 3 + 1)::int,
    (random() * 100 + 1)::int,
    (random() * 2 + 1)::int,
    (random() * 10 + 1)::int
FROM generate_series(1, 300) AS id;

-- Создание случайных данных для таблицы sales на протяжении 90 дней
INSERT INTO sales (datetime, id_product, cheque_id, shop_id, quantity)
SELECT
    TIMESTAMP '2023-08-01 00:00:00' + (random() * 90) * interval '1 day',
    (random() * 3 + 1)::int,
    (random() * 100 + 1)::int,
    (random() * 2 + 1)::int,
    (random() * 10 + 1)::int
FROM generate_series(1, 300) AS id;


-- Напишите запрос, который рассчитывает сумму продаж определенного 
-- товара за определенную единицу времени
select sum(quantity * price) as total_amount from sales as sl
join product as pr on sl.id_product = pr.product_id
where sl.id_product = 1 and EXTRACT(month from datetime) = 8;


-- План запроса
Aggregate  (cost=0.00..437.00 rows=1 width=8)
  ->  Gather Motion 4:1  (slice1; segments: 4)  (cost=0.00..437.00 rows=1 width=8)
        ->  Aggregate  (cost=0.00..437.00 rows=1 width=8)
              ->  Nested Loop  (cost=0.00..437.00 rows=1 width=11)
                    Join Filter: true
                    ->  Sequence  (cost=0.00..431.00 rows=1 width=16)
                          ->  Partition Selector for sales (dynamic scan id: 1)  (cost=10.00..100.00 rows=25 width=4)
                                Partitions selected: 3 (out of 3)
                          ->  Dynamic Seq Scan on sales (dynamic scan id: 1)  (cost=0.00..431.00 rows=1 width=16)
                                Filter: ((id_product = 1) AND (date_part('month'::text, datetime) = '8'::double precision))
                    ->  Index Scan using product_pkey on product  (cost=0.00..6.00 rows=1 width=7)
                          Index Cond: ((product_id = sales.id_product) AND (product_id = 1))
Optimizer: Pivotal Optimizer (GPORCA)