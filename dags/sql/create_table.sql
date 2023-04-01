CREATE TABLE IF NOT EXISTS data.pp_complete (
    transaction_id VARCHAR PRIMARY KEY,
    price int,
    transfer_date timestamp,
    postcode text,
    property_type char(1),
    old_new char(1),
    duration char(1),
    address1 text,
    address2 text,
    street text,
    locality text,
    city text,
    district text,
    county text,
    category char(1),
    status char(1)
);