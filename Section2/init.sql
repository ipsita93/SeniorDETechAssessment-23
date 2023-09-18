-- Drop tables that exists
DROP TABLE IF EXISTS tbl_Members CASCADE;
DROP TABLE IF EXISTS tbl_Items CASCADE;
DROP TABLE IF EXISTS tbl_Transactions CASCADE;

-- Create the Members table
CREATE TABLE tbl_Members (
    membership_id SERIAL PRIMARY KEY,
    first_name VARCHAR(255) NOT NULL,
    last_name VARCHAR(255) NOT NULL,
    email VARCHAR(255) NOT NULL,
    date_of_birth DATE NOT NULL,
    mobile_no CHAR(8) NOT NULL,
    above_18 BOOLEAN NOT NULL
);

-- Create an index on the membership_id column in Members table 
CREATE INDEX idx_membership_id ON tbl_Members (membership_id);

-- Create the Items table
CREATE TABLE tbl_Items (
    item_id SERIAL PRIMARY KEY,
    item_name VARCHAR(255) NOT NULL,
    manufacturer_name VARCHAR(255) NOT NULL,
    cost DECIMAL(10, 2) NOT NULL,
    weight_kg DECIMAL(5, 2) NOT NULL
);

-- Create indexes on the item_id column in Items table
CREATE INDEX idx_item_id ON tbl_Items (item_id);

-- Create the Transactions table
CREATE TABLE tbl_Transactions (
    transaction_id SERIAL PRIMARY KEY,
    membership_id INT NOT NULL,
    item_id INT NOT NULL,
    quantity INT NOT NULL,
    total_items_price DECIMAL(10, 2) NOT NULL,
    total_items_weight_kg DECIMAL(5, 2) NOT NULL,
    transaction_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (membership_id) REFERENCES tbl_Members(membership_id)
    FOREIGN KEY (item_id) REFERENCES tbl_Items(item_id)
);