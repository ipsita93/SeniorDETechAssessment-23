-- Which are the top 10 members by spending
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

-- Which are the top 3 items that are frequently brought by members
SELECT
    i.item_name,
    COUNT(t.item_id) AS purchase_count
FROM
    tbl_Items i
JOIN
    tbl_Transactions t ON i.item_id = t.item_id
GROUP BY
    i.item_name
ORDER BY
    purchase_count DESC
LIMIT 3;
