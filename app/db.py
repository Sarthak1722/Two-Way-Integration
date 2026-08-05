import mysql.connector
import os
from dotenv import load_dotenv

# Specify the full path to the .env file
dotenv_path = os.path.join(os.path.dirname(__file__), '..', '.env')

# Load environment variables from the .env file
load_dotenv(dotenv_path)

# database configuration
db_config = {
    "host": os.getenv("DB_HOST", "localhost"),
    "user": os.getenv("DB_USER", "root"),
    "password": os.getenv("DB_PASSWORD", "root_password"),
    "port": os.getenv("DB_PORT", "3306"),
}

# database global connection and cursor
conn = None
cursor = None

# initializing database connection
def init_db():
    global conn, cursor
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor()

        create_db_query = "CREATE DATABASE IF NOT EXISTS zenskar"
        cursor.execute(create_db_query)
        conn.database = "zenskar"

        create_table_query = """
        CREATE TABLE IF NOT EXISTS customer (
            id INT AUTO_INCREMENT PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            email VARCHAR(255) NOT NULL UNIQUE
        )
        """
        cursor.execute(create_table_query)

        check_column_query = """
            SELECT COUNT(*)
            FROM information_schema.columns
            WHERE table_name = 'customer' AND column_name = 'stripe_id'
        """

        cursor.execute(check_column_query)
        column_exists = cursor.fetchone()[0]

        if not column_exists:
            alter_table_query = """
                ALTER TABLE customer
                ADD COLUMN stripe_id VARCHAR(255) NULL
            """
            cursor.execute(alter_table_query)

        conn.commit()
    except Exception as e:
        print(f"Error: {str(e)}")

# CRUD functions
def read_customer(customer_id):
    try:
        query = "SELECT * FROM customer WHERE id = %s"
        cursor.execute(query, (customer_id,))
        customer_data = cursor.fetchone()
        
        return customer_data
    except Exception as e:
        print(f"Error reading customer: {str(e)}")
        raise Exception("Internal Server Error")
    
def create_customer(name, email, stripe_id=None):
    try:
        query = "INSERT INTO customer (name, email, stripe_id) VALUES (%s, %s, %s)"
        values = (name, email, stripe_id)
        cursor.execute(query, values)
        conn.commit()

        customer_message = {
            "name": name,
            "email": email,
            "stripe_id": stripe_id,
            "action": "create",
        }

        return customer_message
    except Exception as e:
        print(f"Error creating customer: {str(e)}")
        raise Exception("Internal Server Error")



def delete_customer(customer_id):
    try:
        customer_data = read_customer(customer_id)

        if customer_data is None:
            raise Exception("Customer not present")

        query = "DELETE FROM customer WHERE id = %s"
        cursor.execute(query, (customer_id,))
        conn.commit()

        name = customer_data[1]
        email = customer_data[2]
        stripe_id = customer_data[3]

        customer_message = {
            "id": customer_id,
            "action": "delete",
            "email": email,
            "stripe_id": stripe_id,
            "name": name,
        }
        
        return customer_message
    except Exception as e:
        print(f"Error deleting customer: {str(e)}")
        raise Exception("Internal Server Error")

def delete_customer_by_email(email):
    try:
        query = "DELETE FROM customer WHERE email = %s"
        cursor.execute(query, (email,))
        conn.commit()

    except Exception as e:
        print(f"Error deleting customer: {str(e)}")
        raise Exception("Internal Server Error")

def update_customer_by_email(name, email, stripe_id=None):
    try:
        query = "UPDATE customer SET name = %s, email = %s, stripe_id = %s WHERE email = %s"
        values = (name, email, stripe_id, email)
        cursor.execute(query, values)
        conn.commit()

    except Exception as e:
        print(f"Error updating customer: {str(e)}")
        raise Exception("Internal Server Error")

def update_customer_by_stripe_id(name, email, stripe_id):
    try:
        query = "UPDATE customer SET name = %s, email = %s, stripe_id = %s WHERE stripe_id = %s"
        values = (name, email, stripe_id, stripe_id)
        cursor.execute(query, values)
        conn.commit()

    except Exception as e:
        print(f"Error updating customer: {str(e)}")
        raise Exception("Internal Server Error")

# Get all customers
def get_all_customers():
    try:
        query = "SELECT * FROM customer"
        cursor.execute(query)
        customers = cursor.fetchall()

        customer_list = [{"id": row[0], "name": row[1], "email": row[2], "stripe_id": row[3]} for row in customers]

        return customer_list
    except Exception as e:
        print(f"Error fetching customers: {str(e)}")
        raise Exception("Internal Server Error")
    
def update_customer(customer_id, name, email, stripe_id=None):
    try:
        if stripe_id is None:
            customer_data = read_customer(customer_id)
            if customer_data is None:
                raise Exception("Customer not present")
            
            stripe_id = customer_data[3]
            
        query = "UPDATE customer SET name = %s, email = %s, stripe_id = %s WHERE id = %s"
        values = (name, email, stripe_id, customer_id)
        cursor.execute(query, values)
        conn.commit()

        customer_message = {
            "name": name,
            "email": email,
            "stripe_id": stripe_id,
            "action": "update",
        }

        return customer_message
    except Exception as e:
        print(f"Error updating customer: {str(e)}")
        raise Exception("Internal Server Error")
    
def close_db():
    try:
        conn.close()
    except Exception as e:
        print(f"Error closing database connection: {str(e)}")

