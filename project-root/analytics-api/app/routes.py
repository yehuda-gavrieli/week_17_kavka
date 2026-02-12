from main import get_db_connection,app


@app.get("/analytics/top-customers") 
def top_customers():
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    query = """SELECT customers.customerName, COUNT(orders.orderNumber) order_count
            FROM customers 
            JOIN orders ON customers.customerNumber = orders.customerNumber
            GROUP BY customers.customerNumber
            ORDER BY order_count DESC LIMIT 10"""

    cursor.execute(query)
    result = cursor.fetchall()
    conn.close()
    return result


@app.get("/analytics/customers-without-orders")
def get_inactive_customers():
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    query = """SELECT customers.customerName FROM customers 
            LEFT JOIN orders ON customers.customerNumber = orders.customerNumber
            WHERE orders.orderNumber IS NULL"""
    cursor.execute(query)
    result = cursor.fetchall()
    conn.close()
    return result


@app.get("/analytics/zero-credit-active-customers") 
def zero_credit():
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    query = """SELECT DISTINCT customers.customerName
            FROM customers
            JOIN orders ON customers.customerNumber = orders.customerNumber
            WHERE customers.creditLimit = 0 """
    cursor.execute(query)
    result = cursor.fetchall()
    conn.close()
    return result









