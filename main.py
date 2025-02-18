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
from openpyxl import Workbook
from fastapi.responses import FileResponse
import time
from datetime import datetime
from sqlalchemy import Column, Integer, String, Boolean, DateTime, MetaData, Table, Float
from sqlalchemy.dialects.postgresql import JSONB
from fastapi.responses import StreamingResponse
from sqlalchemy.exc import SQLAlchemyError


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



FACILIOO_ENTITIES = [
    "accounts", "account-contact-details", "account-groups", "account-permissions",
    "activity-attempts", "attendances", "attributes", "attribute-groups", "attribute-group-types",
    "attribute-values", "auths", "bank-accounts", "billing-addresses", "booking-account-item-vs",
    "booking-account-vs", "chatgpt-settings", "colors", "conferences", "conference-document-settings",
    "conference-document-templates", "conference-settings", "consumption-brands", "consumption-meters",
    "consumption-readings", "consumption-reading-dates", "consumption-reading-extendeds",
    "consumption-types", "contact-details", "contact-types", "customer-apps", "customer-app-custom-contents",
    "documents", "document-groups", "document-shares", "docuwares", "entrances", "erp-imports",
    "faqs", "faq-groups", "faq-group-visuals", "files", "file-types", "generic-party-settings",
    "health-checks", "inquiries", "inquiry-categories", "inquiry-sources", "mandates", "nearby-places-categories",
    "notices", "pantaeniuses", "parties", "predefined-votes", "processes", "process-feeds", "process-feed-types",
    "process-insurance-claims", "process-notifications", "process-types", "properties", "property-emergency-contacts",
    "property-management-companies", "property-managers", "property-roles", "property-role-defaults",
    "resolutions", "resolution-options", "resolution-option-templates", "resolution-templates",
    "signees", "smart-locks", "smart-lock-types", "sum-votes", "tenants", "terms", "topics", "topic-notes",
    "topic-templates", "topic-template-resolution-templates", "trades", "units", "unit-contracts",
    "unit-contract-types", "unit-types", "user-tasks", "user-task-collections", "user-task-notifications",
    "user-task-priorities", "votes", "voting-end-reasons", "voting-groups", "voting-group-votes",
    "voting-majorities", "voting-procedures", "voting-sessions", "webhooks", "webhook-attempts",
    "webhook-events", "webhook-registrations", "work-orders", "work-order-appointment-requests",
    "work-order-appointment-request-dates", "work-order-feed-entries", "work-order-statuses", "work-order-types"
]

FACILIOO_ENT = []

def get_facilioo_entities(access_token: str):
    try:
        # Define the Facilioo API endpoint to fetch entities
        query_url = f"{API_URL}/api/entities"  # Adjust the endpoint as per your Facilioo API
        response = requests.get(query_url, headers={
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json"
        })

        if response.status_code != 200:
            raise Exception(f"Failed to fetch Facilioo entities: {response.text}")

        # Parse the response to extract entity names
        entities_data = response.json().get("value", [])  # Adjust based on the Facilioo API response structure
        entities = [entity["name"].lower().replace(" ", "-") for entity in entities_data]  # Format entity names

        # Update the global FACILIOO_ENTITIES array
        global FACILIOO_ENTITIES
        FACILIOO_ENTITIES = entities

        return entities
    except Exception as e:
        logging.error(f"Error fetching Facilioo entities: {str(e)}")
        raise

#FACILIOO_ENTITIES = [entity.replace("-", "_") for entity in FACILIOO_ENTITIES]
@app.get("/facilioo-entities")
async def get_facilioo_entities():
    return FACILIOO_ENTITIES

def is_boolean_column(column_name: str) -> bool:
    return column_name.startswith(("is", "has", "can", "should", "allow"))

def infer_schema_from_record(record: dict) -> list:
    columns = [Column("id", Integer, primary_key=True)]
    for key, value in record.items():
        if key == "id":
            continue  # Skip the primary key
        column_name = key.lower()  # Convert column name to lowercase
        if value is None:
            logger.warning(f"Column {column_name} has a None value. Defaulting to String.")
            columns.append(Column(column_name, String))
        elif isinstance(value, str):
            columns.append(Column(column_name, String))
        elif isinstance(value, int):
            columns.append(Column(column_name, Integer))
        elif isinstance(value, bool):
            columns.append(Column(column_name, Boolean))
        elif isinstance(value, (dict, list)):
            columns.append(Column(column_name, JSONB))
        elif isinstance(value, float):
            columns.append(Column(column_name, Float))
        elif isinstance(value, datetime):
            columns.append(Column(column_name, DateTime))
        else:
            logger.warning(f"Unknown type for column {column_name}: {type(value)}. Defaulting to String.")
            columns.append(Column(column_name, String))  # Default to String for unknown types
    return columns

def create_table_for_entity(entity_name: str, sample_record: dict):
    metadata = MetaData()
    columns = infer_schema_from_record(sample_record)
    table = Table(entity_name.lower(), metadata, *columns)
    try:
        metadata.create_all(engine)
        logger.info(f"Created table for entity: {entity_name.lower()}")
        logger.info(f"Table schema: {[col.name for col in columns]}")
        return [col.name for col in columns]  # Return the list of column names
    except Exception as e:
        logger.error(f"Error creating table for entity {entity_name.lower()}: {str(e)}")
        raise  # Re-raise the exception to stop further execution

def fetch_and_save_entity_data(access_token: str, entity_name: str):
    headers = {
        "Authorization": f"Bearer {access_token}",
        "accept": "application/json",
        "api-version": "1.0",
    }

    # Pagination settings
    page_size = 20  # Number of records per page
    page_number = 1   # Start with the first page
    total_records = 0
    table_name = None  # Initialize table_name outside the try block
    inferred_columns = []  # Store inferred columns

    try:
        while True:
            # Fetch data for the current page
            logger.info(f"Fetching page {page_number} for entity: {entity_name}")
            response = requests.get(
                f"{API_URL}/api/{entity_name}",
                headers=headers,
                params={
                    "PageSize": page_size,
                    "PageNumber": page_number,
                    "AscendingOrder": True,
                },
            )

            if response.status_code != 200:
                logger.error(f"Failed to fetch data for entity {entity_name}: {response.text}")
                break

            entity_data = response.json().get("items", [])
            if not entity_data:
                logger.info(f"No more data found for entity {entity_name}.")
                break

            # Replace hyphens with underscores in the table name
            table_name = entity_name.lower().replace("-", "_")

            # Infer schema from the first page (if not already done)
            if page_number == 1:
                sample_record = {}
                for record in entity_data:
                    sample_record.update(record)  # Merge all fields into sample_record

                # Create the table and get inferred columns
                logger.info(f"Creating table for entity: {table_name}")
                inferred_columns = create_table_for_entity(table_name, sample_record)

            # Save data to the database
            db = SessionLocal()
            try:
                logger.info(f"Saving {len(entity_data)} records from page {page_number} for entity {table_name}")
                for record in entity_data:
                    record_dict = {k.lower(): v for k, v in record.items() if k != "id"}  # Convert keys to lowercase

                    # Cast boolean values to integer for all boolean-like columns
                    for column_name, value in record_dict.items():
                        if is_boolean_column(column_name) and isinstance(value, bool):
                            record_dict[column_name] = int(value)

                    # Serialize dictionary values to JSON
                    for column_name, value in record_dict.items():
                        if isinstance(value, dict):
                            record_dict[column_name] = json.dumps(value)

                    # Escape reserved keywords in column names
                    escaped_columns = [f'"{col}"' if col.lower() == "order" else col for col in record_dict.keys()]

                    stmt = text(f"""
                        INSERT INTO {table_name} (id, {', '.join(escaped_columns)})
                        VALUES (:id, {', '.join([f':{k}' for k in record_dict.keys()])})
                        ON CONFLICT (id) DO UPDATE SET
                        {', '.join([f'"{col}" = EXCLUDED."{col}"' if col.lower() == "order" else f"{col} = EXCLUDED.{col}" for col in record_dict.keys()])}
                    """)
                    logger.debug(f"Executing SQL: {stmt}")
                    db.execute(stmt, {"id": record["id"], **record_dict})

                db.commit()
                total_records += len(entity_data)
                logger.info(f"Successfully saved {len(entity_data)} records from page {page_number} for entity {table_name}")
            except Exception as e:
                logger.error(f"Error saving data for entity {table_name if table_name else entity_name}: {str(e)}")
                db.rollback()
            finally:
                db.close()

            # Move to the next page
            page_number += 1

        logger.info(f"Total records saved for entity {table_name if table_name else entity_name}: {total_records}")
        return {"success": True, "columns": inferred_columns, "total_records": total_records}  # Return inferred columns and total records
    except Exception as e:
        logger.error(f"Error fetching data for entity {table_name if table_name else entity_name}: {str(e)}")
        raise  # Re-raise the exception to stop further execution


@app.post("/fetch-entity-fields")
async def fetch_entity_fields(entity_name: str, authorization: str = Header(None)):
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Unauthorized")
    
    access_token = authorization.split("Bearer ")[1]
    headers = {
        "Authorization": f"Bearer {access_token}",
        "accept": "application/json",
        "api-version": "1.0",
    }

    try:
        response = requests.get(f"{API_URL}/api/{entity_name}", headers=headers, params={"PageSize": 1, "PageNumber": 1})
        
        if response.status_code != 200:
            raise HTTPException(status_code=500, detail=f"Failed to fetch entity data: {response.text}")

        entity_data = response.json().get("items", [])
        if not entity_data:
            return {"success": False, "message": f"No data found for entity {entity_name}", "columns": []}

        # Extract field names from the first record
        sample_record = entity_data[0] if entity_data else {}
        columns = list(sample_record.keys())

        return {"success": True, "columns": columns}

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching entity fields: {str(e)}")


@app.get("/get-total-rows")
async def get_total_rows(entity_name: str):
    try:
        # Fetch the total number of rows from the database
        table_name = entity_name.lower().replace("-", "_")
        db = SessionLocal()
        total_records = db.execute(text(f"SELECT COUNT(*) FROM {table_name}")).scalar()
        db.close()
        return {"total_records": total_records}
    except Exception as e:
        logger.error(f"Error fetching total rows for entity {entity_name}: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error fetching total rows for entity {entity_name}: {str(e)}")

@app.post("/fetch-and-save-entity")
async def fetch_and_save_entity(entity_name: str, background_tasks: BackgroundTasks, authorization: Optional[str] = Header(None)):
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Unauthorized")
    access_token = authorization.split("Bearer ")[1]
    try:
        result = fetch_and_save_entity_data(access_token, entity_name)
        return {"success": True, "message": f"Entity {entity_name} data fetching and saving initiated", "columns": result["columns"]}
    except Exception as e:
        logger.error(f"Error fetching and saving entity {entity_name}: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error fetching and saving entity {entity_name}: {str(e)}")

@app.get("/stream-progress")
async def stream_progress(entity_name: str):
    def generate_progress():
        for i in range(101):
            time.sleep(0.1)  # Simulate some processing time
            yield f"data: {i}\n\n"
    return StreamingResponse(generate_progress(), media_type="text/event-stream")


@app.get("/get-entity-columns")
async def get_entity_columns(entity_name: str):
    try:
        # Fetch the schema for the entity
        metadata = MetaData()
        table = Table(entity_name.lower(), metadata, autoload_with=engine)
        columns = [col.name for col in table.columns]
        return columns
    except SQLAlchemyError as e:
        logger.error(f"Error fetching columns for entity {entity_name}: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error fetching columns for entity {entity_name}: {str(e)}")
    
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

def correct_entity_name(entity_name: str) -> str:
    # Handling special pluralization cases
    if entity_name.endswith('y'):
        return entity_name[:-1] + 'ies'  # For words ending in 'y', change to 'ies'
    elif entity_name.endswith('s'):
        return entity_name  # Already plural, no change
    else:
        return entity_name + 's'  # General case: add 's'
    
    
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

def get_crm_entities(access_token: str):
    try:
        # Define the CRM API endpoint to fetch entities
        query_url = f"{CRM_URL}/EntityDefinitions"  # Adjust the endpoint as per your CRM API
        response = requests.get(query_url, headers={
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json"
        })

        if response.status_code != 200:
            raise Exception(f"Failed to fetch CRM entities: {response.text}")

        # Parse the response to extract entity names
        entities_data = response.json().get("value", [])
        entities = [entity["LogicalName"] for entity in entities_data]  # Adjust based on the CRM API response structure

        return entities
    except Exception as e:
        logger.error(f"Error fetching CRM entities: {str(e)}")
        raise

@app.get("/crm-entities")
async def fetch_crm_entities(authorization: Optional[str] = Header(None)):
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Unauthorized")

    access_token = authorization.split("Bearer ")[1]
    try:
        # Get CRM access token
        crm_access_token = get_crm_access_token()
        if not crm_access_token:
            raise HTTPException(status_code=500, detail="Failed to get CRM access token")

        # Fetch CRM entities
        entities = get_crm_entities(crm_access_token)
        return {"entities": entities}
    except Exception as e:
        logger.error(f"Error fetching CRM entities: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error fetching CRM entities: {str(e)}")
    

def get_crm_entity_fields(entity_name: str, access_token: str):
    try:
        # Define the CRM API endpoint to fetch entity metadata
        query_url = f"{CRM_URL}/EntityDefinitions(LogicalName='{entity_name}')/Attributes"
        response = requests.get(query_url, headers={
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json"
        })

        if response.status_code != 200:
            raise Exception(f"Failed to fetch entity fields: {response.text}")

        # Parse the response to extract field names
        attributes_data = response.json().get("value", [])
        columns = [attribute["LogicalName"] for attribute in attributes_data]  # Adjust based on API response

        return columns
    except Exception as e:
        logger.error(f"Error fetching CRM entity fields: {str(e)}")
        raise


@app.get("/crm-entity-fields")
async def fetch_crm_entity_fields(entity_name: str, authorization: Optional[str] = Header(None)):
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Unauthorized")

    access_token = authorization.split("Bearer ")[1]
    try:
        # Get CRM access token
        crm_access_token = get_crm_access_token()
        if not crm_access_token:
            raise HTTPException(status_code=500, detail="Failed to get CRM access token")

        # Fetch entity columns
        columns = get_crm_entity_fields(entity_name, crm_access_token)
        return {"success": True, "columns": columns}
    except Exception as e:
        logger.error(f"Error fetching CRM entity fields: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error fetching CRM entity fields: {str(e)}")


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
    

def export_to_excel(data: list, entity_name: str) -> str:
    wb = Workbook()
    ws = wb.active
    ws.title = entity_name
    if data:
        headers = list(data[0].keys())
        ws.append(headers)
    for row in data:
        anonymized_row = {
            "id": row["id"],
            "name": anonymize_data(row["name"], "name"),
            "lastname": anonymize_data(row["lastname"], "lastname"),
            "email": anonymize_data(row["email"], "email"),
            "phone": anonymize_data(row["phone"], "phone"),
        }
        ws.append(list(anonymized_row.values()))
    file_path = f"{entity_name}_export.xlsx"
    wb.save(file_path)
    return file_path

def delete_file_after_delay(file_path: str, delay: int = 60):
    time.sleep(delay)
    if os.path.exists(file_path):
        os.remove(file_path)

@app.get("/download-excel/{file_name}")
async def download_excel(file_name: str):
    file_path = f"./{file_name}"
    if not os.path.exists(file_path):
        raise HTTPException(status_code=404, detail="File not found")
    return FileResponse(file_path, filename=file_name)

class MigrateResponse(BaseModel):
    total_records: int
    success_count: int
    error_count: int
    update_count: int
    insert_count: int
    entity_name: str  
    results: List[dict]
    excel_file_url: str

@app.post("/migrate", response_model=MigrateResponse)
async def migrate_users(request: MigrateRequest, background_tasks: BackgroundTasks):
    total_records = len(request.user_ids)
    success_count = 0
    error_count = 0
    update_count = 0
    insert_count = 0
    results = []
    entity_name = "Account"  # Replace with dynamic entity name if needed
    data_to_export = []  # List to store data for Excel export

    for user_id in request.user_ids:
        try:
            # Step 1: Fetch user data from the staging database
            user_data = get_user_from_db(user_id)
            if not user_data:
                logger.error(f"User {user_id} not found in staging database")
                raise HTTPException(status_code=404, detail=f"User {user_id} not found in staging database")

            # Add user data to the export list
            data_to_export.append(user_data)

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
    
    # Export data to Excel
    excel_file_path = export_to_excel(data_to_export, entity_name)
    background_tasks.add_task(delete_file_after_delay, excel_file_path, delay=60)

    # Return the final migration summary
    return {
        "total_records": total_records,
        "success_count": success_count,
        "error_count": error_count,
        "update_count": update_count,
        "insert_count": insert_count,
        "entity_name": entity_name,
        "results": results,
        "excel_file_url": f"/download-excel/{os.path.basename(excel_file_path)}"  # URL to download the Excel file
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
