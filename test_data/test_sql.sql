-- SET SQL_SAFE_UPDATES = 0; -- enable this if using MySQL Workbench

-- set the appropriate database (ie. `use komodo;`)
-- insert a dummy capture record
INSERT INTO komodo.captures (`capture_id`, `session_id`, `start`, `end`) VALUES ('126_1630443513898', 126, 1630443513898, 1630443719921);

-- reset the flag on the dummy record
UPDATE komodo.captures SET processed = NULL WHERE capture_id = '126_1630443513898';

-- querying the message JSON field 
SELECT * from komodo.data where message->'$.entityType' = 0 ORDER BY id DESC;
