-- 创建用户表
CREATE TABLE users (
    id INT KEY,
    name VARCHAR(64),
    age INT,
    email VARCHAR(128),
    created_at DATETIME,
    is_active BOOL
);

-- 创建订单表
CREATE TABLE orders (
    order_id INT KEY,
    user_id INT,
    order_date DATETIME,
    total_amount DOUBLE,
    status VARCHAR(20)
);

-- 插入用户数据
INSERT INTO users VALUES (1, '张三', 25, 'zhangsan@example.com', '2024-03-20 10:00:00', 'true');
INSERT INTO users VALUES (2, '李四', 30, 'lisi@example.com', '2024-03-20 11:00:00', 'true');
INSERT INTO users VALUES (3, '王五', 35, 'wangwu@example.com', '2024-03-20 12:00:00', 'false');

-- 插入订单数据
INSERT INTO orders VALUES (1001, 1, '2024-03-20 10:30:00', 199.99, 'completed');
INSERT INTO orders VALUES (1002, 2, '2024-03-20 11:30:00', 299.99, 'pending');
INSERT INTO orders VALUES (1003, 1, '2024-03-20 12:30:00', 399.99, 'completed');

-- 基本查询
SELECT * FROM users;
SELECT name, email FROM users;
SELECT * FROM users WHERE age > 25;

-- 连接查询
SELECT users.name, orders.order_id, orders.total_amount 
FROM users 
JOIN orders 
ON users.id = orders.user_id;

-- 修改表结构
ALTER TABLE users ADD COLUMN phone VARCHAR(20);
ALTER TABLE users DROP COLUMN is_active;
ALTER TABLE users RENAME COLUMN email TO contact_email;


-- 更新数据
UPDATE users SET age = 26 WHERE id = 1;
UPDATE users SET age = 31, contact_email = 'lisi.new@example.com' WHERE id = 2;

-- 删除数据
DELETE FROM users WHERE id = 3;

-- 重命名表
ALTER TABLE users RENAME TO customers;

-- 删除表
DROP TABLE customers;
DROP TABLE orders;