\set aid random_zipfian(1, 100000 * :scale, 1.1)
\set delta random(-5000, 5000)
UPDATE pgbench_accounts SET abalance = abalance + :delta WHERE aid = :aid;
