\copy categories FROM './data/raw_data/categories.csv' DELIMITER ';' CSV HEADER;

\copy customers FROM './data/raw_data/customers.csv' DELIMITER ';' CSV HEADER;

\copy employees FROM './data/raw_data/employees.csv' DELIMITER ';' CSV HEADER;

\copy order_details FROM './data/raw_data/orderdetails.csv' DELIMITER ';' CSV HEADER;

\copy orders FROM './data/raw_data/orders.csv' DELIMITER ';' CSV HEADER NULL '';

\copy products FROM './data/raw_data/products.csv' DELIMITER ';' CSV HEADER;

\copy shippers FROM './data/raw_data/shippers.csv' DELIMITER ';' CSV HEADER;

\copy suppliers FROM './data/raw_data/suppliers.csv' DELIMITER ';' CSV HEADER;