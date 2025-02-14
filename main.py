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
from sqlalchemy import text


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
    crm_url = f"{CRM_URL}/opportunitycustomers"
    
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
        return True
    else:
        print(f"Failed to migrate user {user_data['name']} to CRM. Status code: {response.status_code}")
        return False

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

@app.get("/anonymize/{user_id}")
async def anonymize_user(user_id: int):
    try:
        # Fetch user data from the database
        user_data = get_user_from_db(user_id)
        
        if not user_data:
            raise HTTPException(status_code=404, detail="User not found")

        # Anonymize the user data
        anonymized_data = anonymize_user_data(user_data)

        # Log successful anonymization
        log_migration_result(user_id, "Success", "Not attempted", False)
        return {"success": True}

    except Exception as e:
        # Log anonymization failure
        log_migration_result(user_id, f"Failed: {str(e)}", "Not attempted", False)
        return {"success": False, "error": str(e)}
    
    
def log_migration_result(user_id: int, staging_status: str, crm_status: str, crm_entity_updated: bool):
    try:
        connection = psycopg2.connect(
            user=USER,
            password=PASSWORD,
            host=HOST,
            port=PORT,
            dbname=DBNAME,
            gssencmode='disable'
        )
        cursor = connection.cursor()

        # Ensure table exists
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS migration_logs (
            id SERIAL PRIMARY KEY,
            user_id INT,
            staging_status TEXT,
            crm_status TEXT,
            crm_entity_updated BOOLEAN,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """)

        # Insert log entry
        insert_query = """
        INSERT INTO migration_logs (user_id, staging_status, crm_status, crm_entity_updated)
        VALUES (%s, %s, %s, %s)
        """
        cursor.execute(insert_query, (user_id, staging_status, crm_status, crm_entity_updated))
        connection.commit()

    except Exception as e:
        print(f"Error logging migration result: {e}")

    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()


@app.get("/migration-logs", response_class=JSONResponse)
async def get_migration_logs():
    try:
        # Fetch logs from the database
        connection = psycopg2.connect(
            user=USER,
            password=PASSWORD,
            host=HOST,
            port=PORT,
            dbname=DBNAME,
            gssencmode='disable'
        )
        cursor = connection.cursor()

        # Query to fetch logs
        cursor.execute("SELECT user_id, staging_status, crm_status, created_at FROM migration_logs ORDER BY created_at DESC")
        logs = cursor.fetchall()

        # Convert to a list of dictionaries
        migration_logs = [
            {
                "user_id": log[0],
                "staging_status": log[1],
                "crm_status": log[2],
                "migrated_at": log[3].isoformat()  # Convert datetime to string
            }
            for log in logs
        ]

        return {"logs": migration_logs}

    except Exception as e:
        print(f"Error fetching migration logs: {e}")
        return {"logs": []}

    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()

@app.post("/migrate")
async def migrate(user_id: int, background_tasks: BackgroundTasks):
    try:
        # Fetch user data from the staging DB
        user_data = get_user_from_db(user_id)

        if not user_data:
            raise ValueError("User not found in staging database")

        # Anonymize user data before migration
        anonymized_data = anonymize_user_data(user_data)

        # Simulate migration steps
        staging_status = "Success"
        try:
            crm_status = "Success" if migrate_to_crm(anonymized_data) else "Failed"
        except Exception as e:
            crm_status = f"Failed: {str(e)}"

        crm_entity_updated = crm_status == "Success"

    except Exception as e:
        # Catch any error that occurs during the migration
        staging_status = f"Failed: {str(e)}"
        crm_status = "Not attempted"
        crm_entity_updated = False

    finally:
        # Ensure we always log the result, even if something goes wrong
        log_migration_result(user_id, staging_status, crm_status, crm_entity_updated)

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
