\set id random_zipfian(1, :rows, 1.1)
UPDATE big SET n = n + 1 WHERE id = :id;
