CREATE TABLE cake_filters (
    filter_id SERIAL PRIMARY KEY,
    vertical_id INT,
    cake_filter_id INT unique,
    filter_type_id INT,
    filter_name TEXT,
    operator TEXT,
    lead_field TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP  
);


CREATE TABLE buyer_contract_filters (
    buyer_filter_id SERIAL PRIMARY KEY,
    vertical_id INT,
    buyer_id INT,
    contract_id INT,
    filter_id INT not null foriegn key references cake_filters(filter_id),
    filter_type_id INT,
    filter_name TEXT,
    filter_values TEXT[],
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP  
);


drop table buyer_contract_filters;
drop table cake_filters;
drop table buyer_contract_zips;