CREATE TABLE buyer_contract_zips (
    buyer_zip_id SERIAL PRIMARY KEY,
    vertical_id INT,
    buyer_id INT,
    contract_id INT,
    zips TEXT[],  -
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (vertical_id, buyer_id, contract_id)  
);

drop table buyer_contract_zips;