import psycopg2
from fastapi import FastAPI, HTTPException, Request, BackgroundTasks
from fastapi.responses import JSONResponse, HTMLResponse
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel
import requests
import os
from dotenv import load_dotenv
import random
import string
import json
from typing import List
import sqlalchemy
from sqlalchemy.orm import sessionmaker
from models import User  # Ensure models.py is in the same directory
from sqlalchemy import create_engine
from fastapi.staticfiles import StaticFiles

# Load environment variables
load_dotenv()

# Fetch environment variables for PostgreSQL and Facil.io API
USER = os.getenv("USER")
PASSWORD = os.getenv("PASSWORD")
HOST = os.getenv("HOST")
PORT = os.getenv("PORT")
DBNAME = os.getenv("DBNAME")
API_URL = "https://api.facilio.com"
FACILIO_EMAIL = os.getenv("FACILIO_EMAIL")
FACILIO_PASSWORD = os.getenv("FACILIO_PASSWORD")

# CRM API Configuration
CRM_URL = os.getenv("CRM_URL")
TENANT_ID = os.getenv("TENANT_ID")
CLIENT_ID = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")
RESOURCE = os.getenv("RESOURCE")

# FastAPI App Setup
app = FastAPI()

# Configure Jinja2 template engine
templates = Jinja2Templates(directory="templates")

# Serve static files (CSS, images, etc.)
app.mount("/static", StaticFiles(directory="static"), name="static")

# Headers for API requests
headers = {
    "Content-Type": "application/json"
}


# SQLAlchemy Setup for Staging DB
DATABASE_URL = f"postgresql://{USER}:{PASSWORD}@{HOST}:{PORT}/{DBNAME}"
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# Pydantic models for request body validation
class AuthRequest(BaseModel):
    email: str
    password: str

class DataResponse(BaseModel):
    success: bool
    data: List[dict]

# Helper function to generate a random string for anonymization
def random_string(length=8):
    return ''.join(random.choices(string.ascii_lowercase + string.digits, k=length))

# Anonymize sensitive user data
def anonymize_user_data(user_data: dict):
    user_data['email'] = f"{random_string(10)}@example.com"
    user_data['phone'] = f"+{random.randint(1000000000, 9999999999)}"
    user_data['name'] = f"User_{random_string(5)}"
    return user_data

# Function to authenticate user by email
def authenticate_by_email(email: str, password: str):
    payload = {
        "email": email,
        "password": password
    }
    response = requests.post(f"{API_URL}/user/authenticateByMail", headers=headers, data=json.dumps(payload))
    return response.json()

# Function to fetch data for a specific table from Facil.io
def fetch_data_for_table(access_token: str, table_name: str):
    headers["Authorization"] = f"Bearer {access_token}"
    response = requests.get(f"{API_URL}/tables/{table_name}/data", headers=headers)
    return response.json()

# Function to upsert data into PostgreSQL
def upsert_into_postgresql(table_name: str, data: dict, unique_column: str):
    try:
        # Connect to PostgreSQL database
        connection = psycopg2.connect(
            user=USER,
            password=PASSWORD,
            host=HOST,
            port=PORT,
            dbname=DBNAME,
            gssencmode='disable'
        )
        cursor = connection.cursor()

        # Construct SQL upsert statement
        columns = data.keys()
        values = [data[col] for col in columns]

        # Create the SET part for update (excluding unique column)
        update_part = ', '.join([f"{col} = %s" for col in columns if col != unique_column])

        # Construct SQL upsert statement using ON CONFLICT
        upsert_statement = f"""
        INSERT INTO {table_name} ({', '.join(columns)})
        VALUES ({', '.join(['%s'] * len(values))})
        ON CONFLICT ({unique_column}) 
        DO UPDATE SET {update_part}
        """
        
        # Execute the query with values
        cursor.execute(upsert_statement, values + values[1:])
        connection.commit()
        print(f"Data upserted into {table_name}")
    
    except Exception as e:
        print(f"Error upserting data into {table_name}: {e}")
    
    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()

# Function to migrate user data to CRM opportunity customer entity
def migrate_to_crm(user_data: dict):
    access_token = get_crm_access_token()
    crm_url = f"{CRM_URL}opportunitycustomers"
    
    crm_data = {
        "name": user_data['name'],
        "emailaddress1": user_data['email'],
        "telephone1": user_data['phone'],
        "websiteurl": "https://example.com",
        "customer_type_code": 1  # Assuming '1' corresponds to the type of customer in CRM
    }

    response = requests.post(crm_url, headers={
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }, data=json.dumps(crm_data))

    if response.status_code == 201:
        print(f"Successfully migrated user {user_data['name']} to CRM")
    else:
        print(f"Failed to migrate user {user_data['name']} to CRM. Status code: {response.status_code}")

# Function to get CRM access token
def get_crm_access_token():
    # Authenticate with Azure AD to get access token for CRM API
    url = f"https://login.microsoftonline.com/{TENANT_ID}/oauth2/v2.0/token"
    headers = {
        "Content-Type": "application/x-www-form-urlencoded"
    }
    data = {
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "scope": RESOURCE,
        "grant_type": "client_credentials"
    }
    response = requests.post(url, headers=headers, data=data)
    token_data = response.json()
    return token_data['access_token']

# FastAPI endpoint for authentication
@app.post("/authenticate")
async def authenticate(request: AuthRequest):
    auth_response = authenticate_by_email(request.email, request.password)
    if auth_response.get("success"):
        access_token = auth_response["accessToken"]
        return {"access_token": access_token}
    else:
        raise HTTPException(status_code=401, detail="Authentication failed")

# Function to fetch user data from the staging DB
def get_user_from_db(user_id: int):
    db = SessionLocal()
    try:
        user = db.query(User).filter(User.id == user_id).first()
        if user:
            return {
                "name": user.name,
                "email": user.email,
                "phone": user.phone
            }
        else:
            return None
    finally:
        db.close()

@app.post("/migrate")
async def migrate(user_id: int, background_tasks: BackgroundTasks):
    # Fetch user data from staging DB based on user ID
    user_data = get_user_from_db(user_id)
    
    if not user_data:
        raise HTTPException(status_code=404, detail="User not found in staging database")

    # Anonymize user data before migration
    anonymized_data = anonymize_user_data(user_data)

    # Simulate migration steps
    staging_status = "Success"  # Assuming we always copy to staging DB
    crm_status = "Success" if migrate_to_crm(anonymized_data) else "Failed"
    crm_entity_updated = True if crm_status == "Success" else False

    return JSONResponse({
        "staging_status": staging_status,
        "crm_status": crm_status,
        "crm_entity_updated": crm_entity_updated
    })

# FastAPI route to serve the index.html page
@app.get("/", response_class=HTMLResponse)
async def get_index(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})

# Start FastAPI app
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8000)
