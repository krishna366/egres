\set id random_zipfian(1, :rows, 1.1)
SELECT n FROM big WHERE id = :id;
