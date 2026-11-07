WHENEVER SQLERROR EXIT SQL.SQLCODE

ALTER SESSION SET CONTAINER = FREEPDB1;

DECLARE
    user_count NUMBER;
BEGIN
    SELECT COUNT(*) INTO user_count
    FROM dba_users
    WHERE username = 'APP_USER';

    IF user_count = 0 THEN
        EXECUTE IMMEDIATE
            'CREATE USER app_user IDENTIFIED BY "YourStrongPassword123!"';
    ELSE
        EXECUTE IMMEDIATE
            'ALTER USER app_user IDENTIFIED BY "YourStrongPassword123!"';
    END IF;
END;
/

GRANT CONNECT, RESOURCE, UNLIMITED TABLESPACE TO app_user;

DECLARE
    table_count NUMBER;
BEGIN
    SELECT COUNT(*) INTO table_count
    FROM all_tables
    WHERE owner = 'APP_USER'
      AND table_name = 'INVOICES';

    IF table_count = 0 THEN
        EXECUTE IMMEDIATE '
            CREATE TABLE app_user.invoices (
                invoice_id VARCHAR2(50) PRIMARY KEY,
                customer_email VARCHAR2(255) NOT NULL,
                customer_id VARCHAR2(50) NOT NULL,
                total_amount NUMBER(12, 2) NOT NULL,
                status VARCHAR2(20) NOT NULL,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
            )';
    END IF;
END;
/

MERGE INTO app_user.invoices target
USING (
    SELECT 'INV-9001' invoice_id, 'alice@example.com' customer_email,
           'CUST-001' customer_id, 1250.00 total_amount, 'PAID' status,
           TIMESTAMP '2026-01-15 09:00:00 +00:00' created_at
    FROM dual
    UNION ALL
    SELECT 'INV-9002', 'bob@example.com', 'CUST-002', 450.50, 'PENDING',
           TIMESTAMP '2026-02-20 11:30:00 +00:00'
    FROM dual
    UNION ALL
    SELECT 'INV-9003', 'alice@example.com', 'CUST-001', 89.99, 'PAID',
           TIMESTAMP '2026-03-01 16:45:00 +00:00'
    FROM dual
) seed
ON (target.invoice_id = seed.invoice_id)
WHEN NOT MATCHED THEN
    INSERT (invoice_id, customer_email, customer_id, total_amount, status, created_at)
    VALUES (seed.invoice_id, seed.customer_email, seed.customer_id, seed.total_amount,
            seed.status, seed.created_at);

COMMIT;
