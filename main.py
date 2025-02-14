import psycopg2
from fastapi import FastAPI, HTTPException, Request, BackgroundTasks, Header
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
import logging
from sqlalchemy.orm import Session
from typing import Optional

logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

# Fetch environment variables for PostgreSQL and Facil.io API
USER = os.getenv("USER")
PASSWORD = os.getenv("PASSWORD")
HOST = os.getenv("HOST")
PORT = os.getenv("PORT")
DBNAME = os.getenv("DBNAME")
API_URL = os.getenv("API_URL")
#FACILIO_EMAIL = os.getenv("FACILIO_EMAIL")
#FACILIO_PASSWORD = os.getenv("FACILIO_PASSWORD")

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
    twoFactorCode: str = None

class DataResponse(BaseModel):
    success: bool
    data: List[dict]

# Helper function to generate a random string for anonymization
def random_string(length=8):
    return ''.join(random.choices(string.ascii_lowercase + string.digits, k=length))

# Anonymize sensitive user data
def anonymize_user_data(user_data: dict, user_id: int):
    try:
        # Anonymize data
        user_data['email'] = f"{random_string(10)}@example.com"
        user_data['phone'] = f"+{random.randint(1000000000, 9999999999)}"
        user_data['name'] = f"User_{random_string(5)}"

        # Log migration result after anonymization
        log_migration_result(
            user_id=user_id,
            staging_status="anonymized",
            crm_status="not_updated",  # Assuming CRM wasn't updated
            crm_entity_updated=False
        )

        return user_data
    except Exception as e:
        # Log any error during anonymization
        log_migration_result(
            user_id=user_id,
            staging_status="anonymization_failed",
            crm_status="not_updated",
            crm_entity_updated=False,
            error_message=str(e)
        )
        raise HTTPException(status_code=500, detail=f"Error anonymizing user data: {e}")


def authenticate_by_email(email: str, password: str, two_factor_code: str = None):
    payload = {
        "userMail": email,
        "userPassword": password,
        "userCode": two_factor_code  # Include 2FA code if provided
    }
    headers = {
        "Content-Type": "application/json"
    }
    
    # Log the payload for debugging
    logger.info(f"Sending authentication payload: {payload}")
    
    try:
        response = requests.post(f"{API_URL}/user/authenticateByMail", headers=headers, json=payload)
        
        # Log the raw response for debugging
        logger.info(f"API response status: {response.status_code}")
        logger.info(f"API response text: {response.text}")
        
        if response.status_code == 200:
            auth_data = response.json()

            # Check if 2FA is required
            if auth_data.get("twoFactorRequired") and not two_factor_code:
                return {"twoFactorRequired": True, "message": "2FA code required"}
            
            # Proceed if no 2FA is required or 2FA code is provided
            access_token = auth_data.get("accessToken")
            #refresh_token = auth_data.get("refreshToken")
            
            if not access_token:
                error_message = "Access token missing in response"
                log_migration_result(
                    user_id=-1, 
                    staging_status="authentication_error_by_email",
                    crm_status="not_updated",
                    crm_entity_updated=False,
                    error_message=error_message
                )
                raise HTTPException(status_code=500, detail=error_message)
            
            return {
                "success": True,
                "accessToken": access_token,
                #"refreshToken": refresh_token
            }

        else:
            # Handle API errors
            error_message = f"Authentication failed: {response.text}"
            log_migration_result(
                user_id=-1, 
                staging_status="authentication_failed",
                crm_status="not_updated",
                crm_entity_updated=False,
                error_message=error_message
            )
            raise HTTPException(status_code=response.status_code, detail=error_message)
    
    except Exception as e:
        error_message = f"Authentication error by email: {str(e)}"
        log_migration_result(
            user_id=-1, 
            staging_status="authentication_error_by_email",
            crm_status="not_updated",
            crm_entity_updated=False,
            error_message=error_message
        )
        raise HTTPException(status_code=500, detail=error_message)
    
def save_users_to_staging(users: list):
    db = SessionLocal()
    try:
        for user in users:
            # Ensure user is a dictionary and contains required fields
            if not isinstance(user, dict) or not user.get("id"):
                logger.error(f"Invalid user data: {user}")
                continue  # Skip invalid user data

            # Map API fields to your database model
            db_user = User(
                id=user.get("id"),
                name=user.get("name"),
                email=user.get("email"),
                phone=user.get("phone"),
            )
            # Upsert user
            db.merge(db_user)
        db.commit()
        logger.info(f"Successfully stored {len(users)} users in staging DB")
    except Exception as e:
        logger.error(f"Error saving users: {e}")
        db.rollback()
    finally:
        db.close()                



def fetch_user_by_id(user_id: int, access_token: str):
    headers = {
        "Authorization": f"Bearer {access_token}",
        "accept": "application/json"  # Required by the API
    }
    
    try:
        # Fetch user by ID from the external API
        response = requests.get(f"{API_URL}/user/get?id={user_id}", headers=headers)
        logger.info(f"Fetching user {user_id}: {response.status_code}, {response.text}")  # Debugging

        if response.status_code == 200:
            user_data = response.json()
            
            # Log the raw API response for debugging
            logger.info(f"Raw API response for user {user_id}: {user_data}")
            
            # Handle case where the API returns an empty response or invalid structure
            if not user_data or not isinstance(user_data, dict):
                logger.warning(f"User {user_id} not found or invalid response structure")
                return None
            
            # Extract required fields
            user = {
                "id": user_data.get("userID"),
                "name": user_data.get("userFirstname"),
                "email": user_data.get("userMail"),
                "phone": user_data.get("userMobile"),
            }
            
            # Log the extracted user data for debugging
            logger.info(f"Extracted user data for ID {user_id}: {user}")
            return user
        
        elif response.status_code == 404:
            logger.warning(f"User {user_id} not found")
            return None  # User does not exist
        else:
            raise HTTPException(status_code=response.status_code, detail=f"Failed to fetch user {user_id}: {response.text}")
    
    except Exception as e:
        logger.error(f"Error fetching user {user_id}: {str(e)}")
        return None  # Skip this user and continue



def fetch_users(access_token: str, offset: int, limit: int):
    headers = {
        "Authorization": f"Bearer {access_token}",
        "accept": "application/json"  # Required by the API
    }
    
    try:
        # Fetch users from the external API
        response = requests.get(f"{API_URL}/source/getUsers?offset={offset}&limit={limit}", headers=headers)
        logger.info(f"Fetching users: {response.status_code}, {response.text}")  # Debugging

        if response.status_code == 200:
            users_data = response.json()
            
            # Log the raw API response for debugging
            logger.info(f"Raw API response for users: {users_data}")
            
            # Handle case where the API returns an empty response or invalid structure
            if not users_data or not isinstance(users_data, list):
                logger.warning(f"No users found or invalid response structure")
                return []
            
            # Extract required fields for each user
            users = []
            for user_data in users_data:
                user = {
                    "id": user_data.get("userFaciliooID"),
                    "name": f"{user_data.get('userFirstname')} {user_data.get('userLastname')}",  # Combine first and last name
                    "email": user_data.get("userMail"),
                }
                users.append(user)
            
            # Log the extracted user data for debugging
            logger.info(f"Extracted user data: {users}")
            return users
        
        elif response.status_code == 404:
            logger.warning(f"No users found")
            return []  # No users exist
        else:
            raise HTTPException(status_code=response.status_code, detail=f"Failed to fetch users: {response.text}")
    
    except Exception as e:
        logger.error(f"Error fetching users: {str(e)}")
        return []  # Return an empty list in case of errors
    

def fetch_all_users(access_token: str):
    users = []
    user_id = 1  # Start with user ID 1
    max_users = 30  # Limit to the first 30 users
    max_attempts = 200  # Maximum number of user IDs to check

    while len(users) < max_users and user_id <= max_attempts:
        user = fetch_user_by_id(user_id, access_token)
        if user is not None:
            users.append(user)
            logger.info(f"User {user_id} found and added to the list")
        else:
            logger.warning(f"User {user_id} not found")
        user_id += 1  # Move to the next user ID

    if len(users) < max_users:
        logger.warning(f"Only {len(users)} users found after checking {max_attempts} IDs")
    else:
        logger.info(f"Successfully fetched {len(users)} users")

    return users



def fetch_all_users_at_once(access_token: str, batch_size: int = 25):
    all_users = []
    offset = 0  # Start with offset 0

    while True:
        # Fetch a batch of users
        users = fetch_users(access_token, offset=offset, limit=batch_size)
        
        # If no more users are returned, break the loop
        if not users:
            break
        
        # Add the fetched users to the list
        all_users.extend(users)
        
        # Increment the offset for the next batch
        offset += batch_size

    return all_users

@app.post("/fetch-and-save-users")
async def fetch_and_save_users(background_tasks: BackgroundTasks, authorization: Optional[str] = Header(None)):
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Unauthorized")

    access_token = authorization.split("Bearer ")[1]
    logger.info(f"Access token received: {access_token}")  # Debugging

    try:
        # Fetch users using the access token
#        users = fetch_all_users(access_token)
        users = fetch_all_users_at_once(access_token, batch_size=25)

        logger.info(f"Fetched {len(users)} users")  # Debugging

        if not users:
            raise HTTPException(status_code=404, detail="No users found")

        # Save users to the staging database
        save_users_to_staging(users)

        # Optionally, trigger background tasks
        background_tasks.add_task(save_users_to_staging, users)

        return {"success": True, "message": f"Users fetched and saved successfully. Total users: {len(users)}"}
    except HTTPException as e:
        raise e  # Re-raise HTTPException to return the correct status code
    except Exception as e:
        logger.error(f"Error fetching and saving users: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error fetching and saving users: {str(e)}")


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


@app.post("/authenticate")
async def authenticate(request: AuthRequest):
    logger.info(f"Authentication attempt for email: {request.email}")
    
    try:
        # Authenticate by email and password
        auth_response = authenticate_by_email(request.email, request.password, request.twoFactorCode)
        
        if auth_response.get("success"):
            # If authentication is successful, return tokens
            access_token = auth_response["accessToken"]
            #refresh_token = auth_response.get("refreshToken")  # Ensure this is included in the response
            
            logger.info(f"Authentication successful for email: {request.email}, Access Token: {access_token}")
            
            # Log successful authentication
            log_migration_result(
                user_id=-1,  # Use -1 for system-level logs
                staging_status="authentication_success",
                crm_status="not_updated",
                crm_entity_updated=False,
                error_message=None
            )
            
            # Return both tokens
            return {"access_token": access_token}
        
        elif auth_response.get("twoFactorRequired"):
            # If 2FA is required, return a response indicating that
            logger.info(f"2FA required for email: {request.email}")
            return {"twoFactorRequired": True, "message": "2FA code required"}
        
        else:
            # If authentication fails, log and raise an error
            logger.warning(f"Authentication failed for email: {request.email}")
            
            # Log failed authentication
            log_migration_result(
                user_id=-1,  # Use -1 for system-level logs
                staging_status="authentication_failed",
                crm_status="not_updated",
                crm_entity_updated=False,
                error_message=auth_response.get("message", "Unknown error")
            )
            
            raise HTTPException(status_code=401, detail=auth_response.get("message", "Authentication failed"))
    
    except Exception as e:
        logger.error(f"Error during authentication: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))
    
# Function to fetch user data from the staging DB
def get_user_from_db(user_id: int):
    db: Session = SessionLocal()
    try:
        user = db.query(User).filter(User.id == user_id).first()
        if user:
            return {
                "id": user.id,
                "name": user.name,
                "email": user.email,
                "phone": user.phone
            }
        return None
    except Exception as e:
        logger.error(f"Database error: {e}")
        return None
    finally:
        db.close()
        
@app.get("/anonymize/{user_id}")
async def anonymize_user(user_id: int = None, access_token: str = None):
    if user_id:
        user_data = get_user_from_db(user_id)
        if not user_data:
            return JSONResponse(status_code=404, content={"success": False, "message": f"User with ID {user_id} not found"})
        anonymized_data = anonymize_user_data(user_data)
        return {"success": True, "anonymized_data": anonymized_data}
    else:
        users = fetch_all_users(access_token)
  # Fetch all users if no user_id is provided
        anonymized_users = [anonymize_user_data(user) for user in users]
        return {"success": True, "anonymized_data": anonymized_users}

def log_migration_result(user_id: int, staging_status: str, crm_status: str, crm_entity_updated: bool, error_message: str = None):
    db = SessionLocal()
    try:
        db.execute(text("""
            CREATE TABLE IF NOT EXISTS migration_logs (
                id SERIAL PRIMARY KEY,
                user_id INT NOT NULL,
                staging_status TEXT NOT NULL,
                crm_status TEXT NOT NULL,
                crm_entity_updated BOOLEAN NOT NULL,
                error_message TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """))
        db.commit()

        db.execute(text("""
            INSERT INTO migration_logs (user_id, staging_status, crm_status, crm_entity_updated, error_message)
            VALUES (:user_id, :staging_status, :crm_status, :crm_entity_updated, :error_message)
        """), {
            "user_id": user_id,
            "staging_status": staging_status,
            "crm_status": crm_status,
            "crm_entity_updated": crm_entity_updated,
            "error_message": error_message
        })
        db.commit()
    except Exception as e:
        db.rollback()
        raise e
    finally:
        db.close()

@app.get("/migration-logs", response_class=JSONResponse)
async def get_migration_logs():
    try:
        # Log the incoming request
        logger.info("INFO: Fetching migration logs")

        connection = psycopg2.connect(
            user=USER,
            password=PASSWORD,
            host=HOST,
            port=PORT,
            dbname=DBNAME,
            gssencmode='disable'
        )
        cursor = connection.cursor()

        # Verify table schema
        cursor.execute("""
        SELECT column_name 
        FROM information_schema.columns 
        WHERE table_name = 'migration_logs'
        """)
        columns = [col[0] for col in cursor.fetchall()]
        
        if 'created_at' not in columns:
            raise Exception("Migration logs table missing created_at column")

        cursor.execute("""
        SELECT user_id, staging_status, crm_status, created_at 
        FROM migration_logs 
        ORDER BY created_at DESC
        """)
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
        # Log the error and return a failure response
        logger.error(f"Error fetching migration logs: {e}")
        return JSONResponse(content={"logs": []}, status_code=500)

    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()

@app.post("/migrate")
async def migrate_users(user_ids: list[int], background_tasks: BackgroundTasks):
    results = []

    for user_id in user_ids:
        try:
            # Fetch user data from the staging DB
            user_data = get_user_from_db(user_id)
            if not user_data:
                raise HTTPException(status_code=404, detail=f"User {user_id} not found in staging database")

            # Anonymize user data before migration
            anonymized_data = anonymize_user_data(user_data)

            # Log anonymization result
            log_migration_result(
                user_id=user_id,
                staging_status="Anonymization Success",
                crm_status="Pending",  # CRM status is pending until migration is complete
                crm_entity_updated=False
            )

            # Attempt migration
            staging_status = "Success"
            crm_status = "Success" if migrate_to_crm(user_data) else "Failed"
            
            # Determine if CRM entity was updated
            crm_entity_updated = crm_status == "Success"

            # Log migration result
            log_migration_result(
                user_id=user_id,
                staging_status=staging_status,
                crm_status=crm_status,
                crm_entity_updated=crm_entity_updated
            )

            results.append({"user_id": user_id, "status": "Success"})

        except Exception as e:
            # Log any errors during migration
            log_migration_result(
                user_id=user_id,
                staging_status="Failed",
                crm_status="Failed",
                crm_entity_updated=False
            )
            results.append({"user_id": user_id, "status": f"Failed: {str(e)}"})
    
    return {"results": results}

# FastAPI route to serve the index.html page
@app.get("/", response_class=HTMLResponse)
async def get_index(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})

# Start FastAPI app
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
