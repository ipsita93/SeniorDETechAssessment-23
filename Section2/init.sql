-- Create the Members table
CREATE TABLE tbl_Members (
    # auto-increment PK field
    membership_id SERIAL PRIMARY KEY,

    first_name VARCHAR(255) NOT NULL,
    last_name VARCHAR(255) NOT NULL,
    email VARCHAR(255) NOT NULL,
    date_of_birth DATE NOT NULL,
    mobile_no CHAR(8) NOT NULL,
    above_18 BOOLEAN NOT NULL
);

-- Create an index on the membership_id column
CREATE INDEX idx_membership_id ON tbl_Members (membership_id);

-- Create the Items table
CREATE TABLE tbl_Items (
    # auto-increment PK field
    item_id SERIAL PRIMARY KEY,

    item_name VARCHAR(255) NOT NULL,
    manufacturer_name VARCHAR(255) NOT NULL,
    cost DECIMAL(10, 2) NOT NULL,
    weight_kg DECIMAL(5, 2) NOT NULL
);

-- Create indexes on the Items table on item_name column
CREATE INDEX idx_item_name ON tbl_Items (item_name);

-- Create the Transactions table
CREATE TABLE tbl_Transactions (
    # auto-increment PK field
    transaction_id SERIAL PRIMARY KEY,
    membership_id INT NOT NULL,
    item_id INT NOT NULL,

    quantity INT NOT NULL,
    # quantity x cost 
    total_items_price DECIMAL(10, 2) NOT NULL,
    # quantity x weight_kg 
    total_items_weight_kg DECIMAL(5, 2) NOT NULL,
    transaction_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    FOREIGN KEY (membership_id) REFERENCES tbl_Members(membership_id)
    FOREIGN KEY (item_id) REFERENCES tbl_Items(item_id)
);