import os
import stripe
from dotenv import load_dotenv
from fastapi import FastAPI, Request, HTTPException
from db import create_customer, delete_customer_by_email, update_customer_by_email, update_customer_by_stripe_id

dotenv_path = os.path.join(os.path.dirname(__file__), '..', '..', '..', '.env')
load_dotenv(dotenv_path)

stripe.api_key = os.getenv("STRIPE_SECRET_KEY")
stripe_signing_key = os.getenv("STRIPE_WEBHOOK_SIGNING_KEY")

# Initialize FastAPI app
app = FastAPI()
def stripe_webhook_endpoints(app_webhook):

    def verify_webhook_signature(payload, sig_header):
        try:
            event = stripe.Webhook.construct_event(payload, sig_header, stripe_signing_key)
            print(f"Webhook verified: {event['id']}")
            return event
        except ValueError as e:
            # Invalid payload
            raise HTTPException(status_code=400, detail="Invalid payload")
        except stripe.error.SignatureVerificationError as e:
            # Invalid signature
            raise HTTPException(status_code=400, detail="Invalid signature")

    @app_webhook.post('/webhook/stripe')
    async def handle_customer_added(request: Request):
        try:
            payload = await request.body()
            sig_header = request.headers.get('Stripe-Signature')

            # Verify the Stripe webhook signature for security
            event = verify_webhook_signature(payload, sig_header)

            stripe_id = event['data']['object']['id']
            email = event['data']['object']['email']
            name = event['data']['object']['name']

            print(stripe_id,name,email)

            # Handle the customer added event
            if event['type'] == 'customer.created':
                try:
                    create_customer(name, email)
                except Exception as e:
                    print("Customer already exists!")
                update_customer_by_email(name, email, stripe_id)
            elif event['type'] == 'customer.deleted':
                delete_customer_by_email(email)
            elif event['type'] == 'customer.updated':
                update_customer_by_stripe_id(name, email, stripe_id)

            return {'status': 'success'}
        except Exception as e:
            raise HTTPException(status_code=400, detail="Error processing webhook")

    @app_webhook.get('/webhook/stripe')
    async def stripe_webhook_status():
        return {'message': 'Stripe Webhook Active'}
