-- show names of all tables (quote must be single quote, as type is an identifier, not a string)
SELECT NAME FROM sqlite_master WHERE TYPE='table'; 

-- show columns of a table
PRAGMA table_info(table_name);

/* | cid              | INTEGER | 列的序号（从0开始）。 |
   | name             | TEXT    | 列的名称。 |
   | type             | TEXT    | 列的数据类型（如INTEGER、TEXT、REAL等）。 |
   | not null         | INTEGER | 是否允许NULL值：0表示允许，1表示不允许。 |
   | default value    | TEXT    | 列的默认值。如果没有默认值，则为NULL。 |
   | primary key      | INTEGER | 是否是主键：0表示不是主键，1或更大的值表示主键的序号（从1开始）。 | */


-- count the number of rows in a table
SELECT COUNT(*) FROM TableName;