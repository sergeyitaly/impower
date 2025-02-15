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
import httpx
from urllib.parse import quote

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


class MigrateRequest(BaseModel):
    user_ids: List[int]

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

async def authenticate_by_email(email: str, password: str, two_factor_code: str = None):
    payload = {
        "email": email,
        "password": password,
        "twoFactorCode": two_factor_code,
        "skipMultiFactorAuthentication": True,
    }
    headers = {"Content-Type": "application/json"}
    logger.info(f"Sending authentication payload: {payload}")
    try:
        async with httpx.AsyncClient() as client:
            response = await client.post(f"{API_URL}/api/auth/login", headers=headers, json=payload)
        logger.info(f"API response status: {response.status_code}")
        logger.info(f"API response text: {response.text}")

        if response.status_code == 200:
            auth_data = response.json()
            logger.info(f"Raw API response: {auth_data}")

            # Handle 2FA requirement
            if auth_data.get("twoFactorRequired") and not two_factor_code:
                return {
                    "success": False,
                    "twoFactorRequired": True,
                    "message": "2FA code required",
                }

            # Ensure access token is present
            if "accessToken" not in auth_data:
                raise ValueError("Access token missing in response")

            return {
                "success": True,
                "accessToken": auth_data["accessToken"],
                "refreshToken": auth_data.get("refreshToken"),
            }

        # Handle non-200 status codes
        error_message = f"Authentication failed: {response.text}"
        logger.error(error_message)
        return {"success": False, "message": error_message}

    except httpx.RequestError as e:
        logger.error(f"HTTP request error: {str(e)}")
        raise HTTPException(status_code=500, detail="Authentication service unavailable")

    except ValueError as e:
        logger.error(f"Validation error: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/refresh-token")
async def refresh_token(refresh_token: str):
    headers = {"Content-Type": "application/json", "Authorization": f"Bearer {refresh_token}"}
    try:
        async with httpx.AsyncClient() as client:
            response = await client.post(f"{API_URL}/api/auth/refresh", headers=headers)
        if response.status_code == 200:
            new_tokens = response.json()
            return {"access_token": new_tokens["accessToken"], "refresh_token": new_tokens["refreshToken"]}
        logger.error(f"Token refresh failed: {response.text}")
        raise HTTPException(status_code=response.status_code, detail="Failed to refresh token")
    except httpx.RequestError as e:
        logger.error(f"HTTP request error: {str(e)}")
        raise HTTPException(status_code=500, detail="Token refresh service unavailable")



def fetch_users(access_token: str, offset: int, limit: int):
    headers = {
        "Authorization": f"Bearer {access_token}",
        "accept": "application/json",
        "api-version": "1.0",  # Replace with the required API version
        "Accept-Language": "en-DE",  # Replace with the desired language
    }

    try:
        # Fetch users from the external API
        response = requests.get(
            f"{API_URL}/api/accounts",
            headers=headers,
            params={
                "PageSize": limit,
                "PageNumber": offset // limit + 1,  # Calculate page number from offset
                "SortByAttributeName": "firstName",  # Sort by first name (adjust as needed)
                "AscendingOrder": True,  # Sort in ascending order (adjust as needed)
            },
        )

        # Log the full response for debugging
        logger.info(f"API response status: {response.status_code}")
        logger.info(f"API response text: {response.text}")

        if response.status_code == 200:
            accounts_data = response.json()

            # Log the raw API response for debugging
            logger.info(f"Raw API response for accounts: {accounts_data}")

            # Handle case where the API returns an empty response or invalid structure
            if not accounts_data or not isinstance(accounts_data, dict) or "items" not in accounts_data:
                logger.warning(f"No accounts found or invalid response structure")
                return []

            # Extract required fields for each account
            users = []
            for account in accounts_data["items"]:
                user = {
                    "id": account.get("id"),
                    "name": account.get('firstName'), 
                    "lastname": account.get('lastName'),
                    "email": account.get("email"),
                    "phone": account.get("phoneNumber")
                }
                users.append(user)

            # Log the extracted user data for debugging
            logger.info(f"Extracted user data: {users}")
            return users

        elif response.status_code == 401:
            logger.warning(f"Unauthorized: {response.text}")
            raise HTTPException(status_code=401, detail="Unauthorized: Invalid or missing access token")
        elif response.status_code == 404:
            logger.warning(f"No accounts found")
            return []  # No accounts exist
        else:
            raise HTTPException(
                status_code=response.status_code,
                detail=f"Failed to fetch accounts: {response.text}",
            )

    except Exception as e:
        logger.error(f"Error fetching accounts: {str(e)}")
        return []  # Return an empty list in case of errors    

@app.post("/fetch-and-save-users")
async def fetch_and_save_users(background_tasks: BackgroundTasks, authorization: Optional[str] = Header(None)):
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Unauthorized")

    access_token = authorization.split("Bearer ")[1]
    logger.info(f"Access token received: {access_token}")  # Debugging

    try:
        users = fetch_all_accounts(access_token, batch_size=25)
        logger.info(f"Fetched {len(users)} users")  # Debugging
        if not users:
            raise HTTPException(status_code=404, detail="No users found")
        save_accounts_to_staging(users)
        background_tasks.add_task(save_accounts_to_staging, users)
        return {"success": True, "message": f"Users fetched and saved successfully. Total users: {len(users)}"}
    except HTTPException as e:
        raise e  # Re-raise HTTPException to return the correct status code
    except Exception as e:
        logger.error(f"Error fetching and saving users: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error fetching and saving users: {str(e)}")
    
    
def save_accounts_to_staging(accounts: list):
    db = SessionLocal()
    try:
        for account in accounts:
            if not isinstance(account, dict) or not account.get("id"):
                logger.error(f"Invalid account data: {account}")
                continue

            # Handle null values for required fields
            name = account.get("firstName") or "N/A"  # Default value for name
            lastname = account.get("lastName") or "N/A"  # Default value for lastname
            email = account.get("email") or "N/A"  # Default value for email
            phone = account.get("phoneNumber") or "N/A"  # Default value for phone

            # Create the User object with default values for null fields
            db_account = User(
                id=account.get("id"),
                name=name,
                lastname=lastname,
                email=email,
                phone=phone,
            )
            db.merge(db_account)
        db.commit()
        logger.info(f"Successfully stored {len(accounts)} accounts in staging DB")
    except Exception as e:
        logger.error(f"Error saving accounts: {e}")
        db.rollback()
    finally:
        db.close()

def fetch_account_by_id(account_id: int, access_token: str):
    headers = {
        "Authorization": f"Bearer {access_token}",
        "accept": "application/json",
        "api-version": "1.0",
    }
    
    try:
        response = requests.get(f"{API_URL}/api/accounts/{account_id}", headers=headers)
        if response.status_code == 200:
            return response.json()
        elif response.status_code == 404:
            logger.warning(f"Account {account_id} not found")
            return None
        else:
            logger.error(f"Failed to fetch account {account_id}: {response.text}")
            return None
    except Exception as e:
        logger.error(f"Error fetching account {account_id}: {str(e)}")
        return None

def fetch_accounts(access_token: str, page_size: int, page_number: int):
    headers = {
        "Authorization": f"Bearer {access_token}",
        "accept": "application/json",
        "api-version": "1.0",
    }
    params = {
        "PageSize": page_size,
        "PageNumber": page_number,
    }
    
    try:
        response = requests.get(f"{API_URL}/api/accounts", headers=headers, params=params)
        if response.status_code == 200:
            return response.json().get("items", [])
        elif response.status_code == 401:
            logger.warning("Unauthorized: Invalid or missing access token")
        else:
            logger.error(f"Failed to fetch accounts: {response.text}")
    except Exception as e:
        logger.error(f"Error fetching accounts: {str(e)}")
    return []

def fetch_all_accounts(access_token: str, batch_size: int = 20):
    limit: int = 1000
    accounts = []
    page_number = 1
    
    while len(accounts) < limit:
        batch = fetch_accounts(access_token, batch_size, page_number)
        if not batch:
            break
        accounts.extend(batch)
        if len(accounts) >= limit:
            break
        page_number += 1
    
    return accounts[:limit]


@app.get("/api/crm-contacts-url")
async def get_crm_contacts_url():
    crm_contacts_url = os.getenv("CRM_CONTACTS_URL")
    if not crm_contacts_url:
        return JSONResponse(content={"error": "CRM contacts URL not configured"}, status_code=500)
    return {"crmContactsUrl": crm_contacts_url}


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

    try:
        response = requests.post(url, headers=headers, data=data)
        response.raise_for_status()  # Raise an exception for HTTP errors

        token_data = response.json()
        access_token = token_data['access_token']
        
        # Log success
        log_migration_result(
            user_id=-1,  # Use -1 for system-level logs
            staging_status="authentication_success",
            crm_status="access_token_received",
            crm_entity_updated=False,
            error_message=None
        )
        
        return access_token

    except requests.exceptions.RequestException as e:
        # Log failure
        log_migration_result(
            user_id=-1,  # Use -1 for system-level logs
            staging_status="authentication_failure",
            crm_status="access_token_not_received",
            crm_entity_updated=False,
            error_message=f"Error during token retrieval: {str(e)}"
        )
        print(f"Error during token retrieval: {str(e)}")
        return None


@app.post("/authenticate")
async def authenticate(request: AuthRequest):
    logger.info(f"Authentication attempt for email: {request.email}")

    try:
        auth_response = await authenticate_by_email(request.email, request.password, request.twoFactorCode)
        
        # Handle 2FA requirement
        if auth_response.get("twoFactorRequired"):
            logger.info(f"2FA required for email: {request.email}")
            return {"twoFactorRequired": True, "message": "2FA code required"}

        # Handle successful authentication
        if auth_response.get("success"):
            access_token = auth_response["accessToken"]
            refresh_token = auth_response.get("refreshToken")  # This might be None
            logger.info(f"Authentication successful for email: {request.email}, Access Token: {access_token}")
            log_migration_result(
                user_id=-1,  # Use -1 for system-level logs
                staging_status="authentication_success",
                crm_status="not_updated",
                crm_entity_updated=False,
                error_message=None
            )
            return {"access_token": access_token, "refresh_token": refresh_token}

        # Handle authentication failure
        else:
            logger.warning(f"Authentication failed for email: {request.email}")
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

    


def anonymize_data(data: str, field_type: str) -> str:
    if not data:
        return data

    if field_type == 'name':
        # Show the first 2 characters and anonymize the rest
        return data[:2] + "***" if len(data) > 2 else data + "***"
    
    elif field_type == 'lastname':
        # Show the last 2 characters and anonymize the rest
        return "***" + data[-2:] if len(data) > 2 else "***" + data
    
    elif field_type == 'email':
        # Show the first 3 characters of the local part and the domain
        local, domain = data.split('@') if '@' in data else (data, "")
        return local[:3] + "***@" + domain if domain else local[:3] + "***"
    
    elif field_type == 'phone':
        # Show the first 4 characters and anonymize the rest
        return data[:4] + "****" if len(data) > 4 else data + "****"

    return data



# Function to fetch user data from the staging DB
def get_user_from_db(user_id: int):
    db: Session = SessionLocal()
    try:
        user = db.query(User).filter(User.id == user_id).first()
        if user:
            return {
                "id": user.id,
                "name": user.name,
                "lastname": user.lastname,
                "email": user.email,
                "phone": user.phone
            }
        return None
    except Exception as e:
        logger.error(f"Database error: {e}")
        return None
    finally:
        db.close()

def get_users_from_db():
    db: Session = SessionLocal()
    try:
        users = db.query(User).all()  # Fetch all users
        return [{"id": user.id, "name": user.name,  "lastname": user.lastname, "email": user.email, "phone": user.phone} for user in users]
    except Exception as e:
        logger.error(f"Database error: {e}")
        return []
    finally:
        db.close()

@app.get("/users")
async def get_users():
    users = get_users_from_db()
    if not users:
        raise HTTPException(status_code=404, detail="No users found in the staging database")
    return users

def migrate_to_crm(user_data: dict):
    access_token = get_crm_access_token()
    crm_url = f"{CRM_URL}/contacts"

    crm_data = {
        "firstname": user_data['name'],
        "lastname": user_data['lastname'],
        "emailaddress1": user_data['email'],  
        "telephone1": user_data['phone'],
    }

    try:
        # Step 1: Properly encode the anonymized email for the query
        encoded_email = quote(user_data['email'])
        query_url = f"{CRM_URL}/contacts?$filter=emailaddress1 eq '{encoded_email}'"
        
        response = requests.get(query_url, headers={
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json"
        })

        if response.status_code == 200:
            try:
                existing_contacts = response.json().get("value", [])
            except Exception as e:
                logger.error(f"Error parsing CRM response: {str(e)}")
                return {"success": False, "action": "query_failed", "error": str(e)}

            if existing_contacts:
                # User exists → Update their information
                contact_id = existing_contacts[0]['contactid']
                update_url = f"{CRM_URL}/contacts({contact_id})"
                
                update_response = requests.patch(update_url, headers={
                    "Authorization": f"Bearer {access_token}",
                    "Content-Type": "application/json"
                }, data=json.dumps(crm_data))

                if update_response.status_code in [200, 204]:
                    logger.info(f"Successfully updated contact {contact_id} in CRM.")
                    return {"success": True, "action": "updated"}
                else:
                    logger.error(f"Failed to update contact {contact_id} in CRM. "
                                 f"Status code: {update_response.status_code}, Response: {update_response.text}")
                    return {"success": False, "action": "update_failed"}
            else:
                # User does not exist → Create a new record
                create_response = requests.post(crm_url, headers={
                    "Authorization": f"Bearer {access_token}",
                    "Content-Type": "application/json"
                }, data=json.dumps(crm_data))

                if create_response.status_code in [200, 201, 204]:  # Allow multiple success codes
                    logger.info(f"Successfully inserted new contact into CRM. Response: {create_response.text}")
                    return {"success": True, "action": "inserted"}
                else:
                    logger.error(f"Failed to insert new contact into CRM. "
                                f"Status code: {create_response.status_code}, Response: {create_response.text}")
                    return {"success": False, "action": "creation_failed"}
        else:
            logger.error(f"Failed to query CRM for existing contacts. Status code: {response.status_code}, Response: {response.text}")
            return {"success": False, "action": "query_failed"}

    except Exception as e:
        logger.error(f"An error occurred while migrating user data to CRM: {str(e)}")
        return {"success": False, "action": "error", "error": str(e)}
    

class MigrateResponse(BaseModel):
    total_records: int
    success_count: int
    error_count: int
    update_count: int
    insert_count: int
    results: List[dict]


@app.post("/migrate", response_model=MigrateResponse)
async def migrate_users(request: MigrateRequest, background_tasks: BackgroundTasks):
    total_records = len(request.user_ids)
    success_count = 0
    error_count = 0
    update_count = 0
    insert_count = 0
    results = []

    for user_id in request.user_ids:
        try:
            # Step 1: Fetch user data from the staging database
            user_data = get_user_from_db(user_id)
            if not user_data:
                logger.error(f"User {user_id} not found in staging database")
                raise HTTPException(status_code=404, detail=f"User {user_id} not found in staging database")

            # Step 2: Apply anonymization to user data
            anonymized_data = {
                "id": user_data["id"],
                "name": anonymize_data(user_data["name"], "name"),
                "lastname": anonymize_data(user_data["lastname"], "lastname"),
                "email": anonymize_data(user_data["email"], "email"),  # Anonymized email
                "phone": anonymize_data(user_data["phone"], "phone"),
            }

            # Log anonymization success
            logger.info(f"Successfully anonymized data for user {user_id}")
            log_migration_result(
                user_id=user_id,
                staging_status="Anonymization Success",
                crm_status="Pending",
                crm_entity_updated=False
            )

            # Step 3: Migrate anonymized data to CRM
            staging_status = "Success"
            crm_result = migrate_to_crm(anonymized_data)  # Pass anonymized_data
            crm_status = "Success" if crm_result["success"] else "Failed"
            crm_entity_updated = crm_result["success"]

            # Step 4: Update counts based on CRM result
            if crm_result["success"]:
                success_count += 1
                if crm_result["action"] == "updated":
                    update_count += 1
                elif crm_result["action"] == "inserted":
                    insert_count += 1
            else:
                error_count += 1

            # Log CRM migration result
            logger.info(f"CRM migration result for user {user_id}: {crm_result}")
            log_migration_result(
                user_id=user_id,
                staging_status=staging_status,
                crm_status=crm_status,
                crm_entity_updated=crm_entity_updated,
                error_message=crm_result.get("error", None)  # Log any errors from CRM
            )

            # Add result to the results list
            results.append({
                "user_id": user_id,
                "staging_status": staging_status,
                "crm_status": crm_status,
                "action": crm_result.get("action", "none"),
                "error": crm_result.get("error", None)  # Include error details if any
            })

        except Exception as e:
            # Handle unexpected errors
            error_count += 1
            logger.error(f"An unexpected error occurred while migrating user {user_id}: {str(e)}")
            log_migration_result(
                user_id=user_id,
                staging_status="Failed",
                crm_status="Failed",
                crm_entity_updated=False,
                error_message=str(e)
            )
            results.append({
                "user_id": user_id,
                "staging_status": "Failed",
                "crm_status": "Failed",
                "error": str(e)
            })
    
    # Return the final migration summary
    return {
        "total_records": total_records,
        "success_count": success_count,
        "error_count": error_count,
        "update_count": update_count,
        "insert_count": insert_count,
        "results": results
    }

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


# FastAPI route to serve the index.html page
@app.get("/", response_class=HTMLResponse)
async def get_index(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})

# Start FastAPI app
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
