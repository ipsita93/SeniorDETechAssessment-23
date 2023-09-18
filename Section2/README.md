# Section 2: Databases 

## E-commerce Data Model

- A total of 3 tables are created to support the e-commerce website application.
- Refer to PostgreSQL Database Setup using Docker, ER Diagram, DDL statements and Sample Analytical SQL Queries as per instructions below. 


## PostgreSQL Database Setup using Docker

To manage sales transactions effectively, we will set up a PostgreSQL database using a Docker image for containerized deployment. Below are the key tasks:

### 1. Run the Postgres Container in Docker

`docker-compose up -d`

The above command has created and started the Postgres container.

### 2. Verify Executing Container

`docker ps`

It can be seen that the “PostgresCont” container is executing efficiently.

### 3. Interact With Executing Container

`docker exec -it PostgresCont bash`

Subsequently, the “PostgresCont” container has been accessed and now we can run commands in it.

### 4. Establish a Connection With a Database

'psql -h localhost -U postgres'

After that, we can execute/run SQL queries and psql commands in the SQL shell.

### 5. Execute psql Commands

We have efficiently installed PostgreSQL using Docker Compose. Now, we can run our DDL statements to set up the database schema. 

## Entity-Relationship Diagram (ERD)
*Refer to `er_diagram.drawio` or `screenshots/er_diagram_drawio.png`*

## Data Definition Language (DDL) Statements
*Refer to `init.sql` and `screenshots/` folder to see output on PGadmin UI on localhost:5050/browser/*

The Data Definition Language (DDL) statements used to set up the database schema for our e-commerce platform. The schema includes tables for Members, Items, and Transactions, along with necessary indices and foreign key constraints.

### Dropping Existing Tables

We begin by dropping tables if they already exist. The `CASCADE` option ensures that associated objects, such as indexes and foreign keys, are also dropped.

```sql
-- Drop tables that exist
DROP TABLE IF EXISTS tbl_Members CASCADE;
DROP TABLE IF EXISTS tbl_Items CASCADE;
DROP TABLE IF EXISTS tbl_Transactions CASCADE;
```

### Database Schema

The database schema will include the following entities:

#### Members
- Membership ID (Primary Key)
- First Name
- Last Name
- Email
- Date of Birth
- Mobile Number
- Above 18 (True or False)

#### Items
- Item ID (Primary Key)
- Item Name
- Manufacturer Name
- Cost
- Weight (in kg)

#### Transactions
- Transaction ID (Primary Key)
- Membership ID (Foreign Key)
- Item ID (Foreign Key)
- Quantity
- Total Items Price
- Total Items Weight (in kg)
- Transaction Date (with default as current timestamp)

## Sample Analytical SQL Queries
*Refer `sample_queries.sql`*

**Top 10 Members by Spending**: Retrieve the top 10 members who have spent the most on purchases.

- membership_id: Unique identifier for each member.
- first_name and last_name: Member's first and last names.
- total_spending: Total amount spent by the member on purchases.

```sql
SELECT
    m.membership_id,
    m.first_name,
    m.last_name,
    SUM(t.total_items_price) AS total_spending
FROM
    tbl_Members m
JOIN
    tbl_Transactions t ON m.membership_id = t.membership_id
GROUP BY
    m.membership_id
ORDER BY
    total_spending DESC
LIMIT 10;
```


**Top 3 Frequent Items**: Identify the top 3 items that are frequently purchased by members.

Assumption: "frequently purchased" refers to highest total quantity of items bought by any members.

- item_id: Unique identifier for each item.
- purchase_count: Quantity of items purchased by members.

```sql
SELECT
    i.item_id,
    COUNT(t.quantity) AS purchase_count
FROM
    tbl_Items i
JOIN
    tbl_Transactions t ON i.item_id = t.item_id
GROUP BY
    i.item_id
ORDER BY
    purchase_count DESC
LIMIT 3;
```