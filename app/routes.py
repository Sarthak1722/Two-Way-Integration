from fastapi import FastAPI, HTTPException, Body
from db import create_customer, read_customer, delete_customer, get_all_customers,update_customer
from Kafka.Kafka import kafka_event_producer
import json

# Initialize FastAPI app
app = FastAPI()
def configure_apis(app):

    # Route to add new customer
    @app.post("/customer")
    async def add_customer(name: str = Body(...), email: str = Body(...)):
        try:
            customer_message = create_customer(name, email)
            json_message = json.dumps(customer_message)
            kafka_event_producer(json_message)
            return {"message": "Customer added successfully"}
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Failed to add customer: {str(e)}")

    # Route to get customer by id
    @app.get("/customer/{customer_id}")
    async def get_customer(customer_id: int):
        try:
            customer = read_customer(customer_id)
            if customer:
                return {"customer": customer}
            else:
                raise HTTPException(status_code=404, detail="Customer not found")
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Failed to retrieve customer: {str(e)}")



    # Route to remove existing customer
    @app.delete("/customer/{customer_id}")
    async def remove_customer(customer_id: int):
        try:
            customer_message = delete_customer(customer_id)
            json_message = json.dumps(customer_message)
            kafka_event_producer(json_message)
            return {"message": "Customer deleted successfully"}
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Error deleting customer: {str(e)}")

    # Route to get all customers
    @app.get("/customers")
    async def get_all_customers_route():
        try:
            customers = get_all_customers()
            return {"customers": customers}
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Error fetching customers: {str(e)}")
        

    # Route to update existing customer
    @app.put("/customer/{customer_id}")
    async def update_customer_info(customer_id: int, name: str = Body(...), email: str = Body(...)):
        try:
            customer_message = update_customer(customer_id, name, email)
            json_message = json.dumps(customer_message)
            kafka_event_producer(json_message)
            return {"message": "Customer updated successfully"}
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Error updating customer: {str(e)}")

