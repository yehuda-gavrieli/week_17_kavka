from main import get_db_connection,app



@app.get("/analytics/top-customers") 
def top_customers():
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    query = """
    SELECT c.customerName, COUNT(o.orderNumber) as order_count
    FROM customers c
    JOIN orders o ON c.customerNumber = o.customerNumber
    GROUP BY c.customerNumber
    ORDER BY order_count DESC
    LIMIT 10
    """
    cursor.execute(query)
    result = cursor.fetchall()
    conn.close()
    return result


@app.get("/analytics/customers-without-orders")
def get_inactive_customers():
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    query = """
    SELECT c.customerName
    FROM customers c
    LEFT JOIN orders o ON c.customerNumber = o.customerNumber
    WHERE o.orderNumber IS NULL
    """
    cursor.execute(query)
    result = cursor.fetchall()
    conn.close()
    return result


@app.get("/analytics/zero-credit-active-customers") 
def zero_credit():
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    query = """
    SELECT DISTINCT c.customerName
    FROM customers c
    JOIN orders o ON c.customerNumber = o.customerNumber
    WHERE c.creditLimit = 0
    """
    cursor.execute(query)
    result = cursor.fetchall()
    conn.close()
    return result









