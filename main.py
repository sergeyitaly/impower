import psycopg2
from fastapi import FastAPI, HTTPException, Request, BackgroundTasks, Header, Depends, Query
from fastapi.responses import JSONResponse, HTMLResponse
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel
import requests
import os
from dotenv import load_dotenv
import random
import string
import json
from typing import Dict, List
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
from fastapi.responses import FileResponse
import time
from datetime import datetime
from sqlalchemy import Column, Integer, String, Boolean, DateTime, MetaData, Table, Float, JSON
from sqlalchemy.dialects.postgresql import JSONB
from fastapi.responses import StreamingResponse
from sqlalchemy.exc import SQLAlchemyError
import re
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Generator


logging.basicConfig(level=logging.INFO)
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
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

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



FACILIOO_ENTITIES = [
    "accounts", "account-contact-details", "account-groups", "account-permissions",
    "activity-attempts", "attendances", "attributes", "attribute-groups", "attribute-group-types",
    "attribute-values", "auths", "bank-accounts", "billing-addresses", "booking-account-item",
    "booking-account", "chatgpt-settings", "colors", "conferences", "conference-document-settings",
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

@app.get("/facilioo-entities")
async def get_facilioo_entities():
    return FACILIOO_ENTITIES

class FaciliooEntitiesRequest(BaseModel):
    access_token: str


RESERVED_KEYWORDS = ["order", "group", "select", "insert", "update", "delete", "where"]

def is_boolean_like(value):
    if isinstance(value, bool):
        return True
    if isinstance(value, str):
        return value.lower() in ("true", "false", "t", "f", "yes", "no", "y", "n")
    return False

def convert_to_boolean(value):
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return value == 1
    if isinstance(value, str):
        return value.lower() in ("true", "t", "yes", "y")
    return False  # Default fallback

def infer_schema_from_record(record: dict) -> list:
    columns = [Column("id", Integer, primary_key=True)]
    for key, value in record.items():
        if key == "id":
            continue
        column_name = key.lower()
        if value is None:
            logger.warning(f"Column {column_name} has a None value. Defaulting to String.")
            columns.append(Column(column_name, String))
            continue
        if is_boolean_like(value):
            columns.append(Column(column_name, Boolean))
        elif isinstance(value, str):
            columns.append(Column(column_name, String))
        elif isinstance(value, int):
            columns.append(Column(column_name, Integer))
        elif isinstance(value, float):
            columns.append(Column(column_name, Float))
        elif isinstance(value, (dict, list)):
            columns.append(Column(column_name, JSON))
        elif isinstance(value, datetime):
            columns.append(Column(column_name, DateTime))
        else:
            logger.warning(f"Unknown type for column {column_name}: {type(value)}. Defaulting to String.")
            columns.append(Column(column_name, String))

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

def strip_html_tags(text):
    clean = re.sub(r'<.*?>', '', text)
    return clean.strip() 

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
            table_name = table_entity_name(entity_name)
            if page_number == 1:
                sample_record = {}
                if entity_data:  # If data is fetched, use the first record to infer the schema
                    for record in entity_data:
                        sample_record.update(record)
                else:  # If no data is fetched, create an empty table with default columns
                    sample_record = {"id": 1, "name": "default", "created_at": datetime.now()}  # Example default columns
                logger.info(f"Creating table for entity: {table_name}")
                inferred_columns = create_table_for_entity(table_name, sample_record)
            # Save data to the database
            db = SessionLocal()
            try:
                logger.info(f"Saving {len(entity_data)} records from page {page_number} for entity {table_name}")
                for record in entity_data:
                    record_dict = {k.lower(): v for k, v in record.items() if k != "id"}  # Convert keys to lowercase                        
                    for column_name, value in record_dict.items():
                        if is_boolean_like(value):  # Convert boolean-like values to proper booleans
                            record_dict[column_name] = convert_to_boolean(value)
                    for column_name, value in record_dict.items():
                        if isinstance(value, (dict, list)):
                            record_dict[column_name] = json.dumps(value)  # Convert complex types to JSON
                        elif isinstance(value, str):
                            record_dict[column_name] = strip_html_tags(value)  # Remove HTML tags
                    escaped_columns = [f'"{col}"' if col.lower() in RESERVED_KEYWORDS else col for col in record_dict.keys()]
                    stmt = text(f"""
                        INSERT INTO {table_name} (id, {', '.join(escaped_columns)})
                        VALUES (:id, {', '.join([f':{k}' for k in record_dict.keys()])})
                        ON CONFLICT (id) DO UPDATE SET
                        {', '.join([f'"{col}" = EXCLUDED."{col}"' if col.lower() in RESERVED_KEYWORDS else f"{col} = EXCLUDED.{col}" for col in record_dict.keys()])}
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

@app.post("/facilioo-entities-with-status")
def get_facilioo_entities_with_status(request: FaciliooEntitiesRequest):
    access_token = request.access_token
    if not access_token:
        raise HTTPException(status_code=401, detail="Access token is required")

    headers = {
        "Authorization": f"Bearer {access_token}",
        "accept": "application/json",
        "api-version": "1.0",
    }
    entities = FACILIOO_ENTITIES  # Replace with your actual list of entities
    entities_with_status = []

    for entity in entities:
        try:
            logger.info(f"Checking status for entity: {entity}")
            response = requests.get(
                f"{API_URL}/api/{entity}",
                headers=headers,
                params={"PageSize": 1, "PageNumber": 1, "AscendingOrder": True},
            )

        #    logger.info(f"Response status code for {entity}: {response.status_code}")
        #    logger.info(f"Response body for {entity}: {response.text}")  # Log the response body for debugging

            if response.status_code == 200:
                entity_data = response.json().get("items", [])
                if entity_data:
        #            logger.info(f"Entity {entity} has data: {entity_data}")
                    entities_with_status.append({"name": entity, "has_data": True})  # Entity has data
                else:
                    logger.info(f"Entity {entity} has no data.")
                    entities_with_status.append({"name": entity, "has_data": False})  # Entity has no data
            else:
        #        logger.warning(f"Failed to fetch data for entity {entity}. Status code: {response.status_code}")
                entities_with_status.append({"name": entity, "has_data": False})  # Assume no data if API call fails
        except Exception as e:
        #    logger.error(f"Error checking status for entity {entity}: {str(e)}", exc_info=True)
            entities_with_status.append({"name": entity, "has_data": False})  # Assume no data on error

    #logger.info(f"Final entities_with_status: {entities_with_status}")
    return entities_with_status


class ExportRequest(BaseModel):
    entity_name: str

@app.post("/fetch-and-export/")
def fetch_and_export_entity_data(
    request: ExportRequest,  # Use the Pydantic model for request validation
    db: Session = Depends(get_db),
    background_tasks: BackgroundTasks = None,
):
    try:
        entity_name = table_entity_name(request.entity_name)
        logger.info(f"Entity name: {entity_name}")
        data_to_export = fetch_all_entity_data_from_staging(db, entity_name)
        if not data_to_export:
            raise HTTPException(status_code=404, detail="No data found to export.")

        # Export data to Excel
        excel_file_path = export_all_entity_to_excel(data_to_export, entity_name)
        background_tasks.add_task(delete_file_after_delay, excel_file_path, delay=60)
        logger.info(f"Data exported to Excel file: {excel_file_path}")

        return {
            "success": True,
            "columns": list(data_to_export[0].keys()) if data_to_export else [],
            "total_records": len(data_to_export),
            "excel_file_url": f"/download-excel/{os.path.basename(excel_file_path)}"
        }
    except Exception as e:
        logger.error(f"Failed to export data to Excel: {str(e)}")
        raise HTTPException(status_code=500, detail="Failed to export data to Excel.")

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

        # Extract field names from the first record and convert them to lowercase
        sample_record = entity_data[0] if entity_data else {}
        columns = [col.lower() for col in sample_record.keys()]

        return {"success": True, "columns": columns}

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching entity fields: {str(e)}")


def anonymize_field(field_type: str, data: str) -> str:
    if data is None or not isinstance(data, str) or not data.strip():
        # Return a default anonymized value for invalid or missing data
        return "***"

    if field_type == 'name':
        # Show the first 2 characters and anonymize the rest
        return data[:2] + "***" if len(data) > 2 else data + "***"
    
    elif field_type == 'email':
        # Anonymize the email by showing the first part and masking the domain
        if '@' in data:
            local_part, domain = data.split('@', 1)
            return local_part[:2] + "***@" + "***"
        else:
            # Handle invalid email format
            return "***"
    
    elif field_type == 'phone':
        # Show the last 4 digits and anonymize the rest
        if len(data) >= 4:
            return "***" + data[-4:]
        else:
            # Handle short phone numbers
            return "***"
    
    else:
        # Default case: anonymize the entire field
        return "***"


def fetch_all_entity_data_from_staging(db: Session, entity_name: str) -> list:
    logger.info(f"Fetching all data from staging for entity: {entity_name}")

    try:
        # Construct and log the query
        entity_name = table_entity_name(entity_name)
        query = text(f"SELECT * FROM {entity_name}")
        logger.debug(f"Executing query: {query}")

        # Execute the query
        result = db.execute(query)
        column_names = result.keys()  # Get column names from the query result
        records = [dict(zip(column_names, row)) for row in result.fetchall()]  # Convert tuples to dictionaries
        
        logger.info(f"Retrieved {len(records)} records for entity: {entity_name}")
        logger.debug(f"Sample record: {records[0] if records else 'No records found'}")  # Debug log

        if not records:
            logger.warning(f"No records found for entity: {entity_name}")
            return []
        for record in records:
            for key, value in record.items():
                if isinstance(value, bool):
                    record[key] = str(value)  
        return records  # Return all records as-is
    
    except Exception as e:
        logger.error(f"Error fetching data for entity {entity_name}: {e}", exc_info=True)
        return []


def fetch_entity_data_from_staging(db: Session, entity_name: str, matched_fields: list) -> list:
    logger.info(f"Fetching data from staging for entity: {entity_name}")
    logger.info(f"Matched fields are: {matched_fields}")
    try:
        entity_name = table_entity_name(entity_name)
        query = text(f"SELECT * FROM {entity_name}")
        logger.debug(f"Executing query: {query}")
        result = db.execute(query)
        column_names = result.keys()  # Get column names from the query result
        records = [dict(zip(column_names, row)) for row in result.fetchall()]  # Convert tuples to dictionaries
        logger.info(f"Retrieved {len(records)} records for entity: {entity_name}")
        logger.debug(f"Sample record: {records[0] if records else 'No records found'}")  # Debug log
        for record in records:
            for key, value in record.items():
                if isinstance(value, bool):
                    record[key] = str(value) 
        if not records:
            logger.warning(f"No records found for entity: {entity_name}")
            return []
        processed_records = []
        for record in records:
            processed_record = {}
            for field_pair in matched_fields:  # Iterate over the list of field strings
                facilioo_field, crm_field = field_pair.split('-')
                if facilioo_field in record:  # Ensure the source field exists
                    processed_record[facilioo_field] = record[facilioo_field]
                else:
                    logger.warning(f"Facilioo field '{facilioo_field}' not found in record for entity: {entity_name}")
                    processed_record[facilioo_field] = None  # Handle missing fields safely
            processed_records.append(processed_record)

        logger.debug(f"Sample processed record: {processed_records[0] if processed_records else 'No records found'}")  # Debug log
        return processed_records
    
    except Exception as e:
        logger.error(f"Error fetching data for entity {entity_name}: {e}", exc_info=True)
        return []    


@app.get("/get-total-rows")
async def get_total_rows(entity_name: str):
    try:
        # Fetch the total number of rows from the database
        #table_name = entity_name.lower().replace("-", "_")
        table_name = table_entity_name(entity_name)
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
        entity_name = table_entity_name(entity_name)
        metadata = MetaData()
        table = Table(entity_name.lower(), metadata, autoload_with=engine)
        columns = [col.name for col in table.columns]
        return columns.lower()
    except SQLAlchemyError as e:
        logger.error(f"Error fetching columns for entity {entity_name}: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error fetching columns for entity {entity_name}: {str(e)}")
    

def table_entity_name(name: str) -> str:
    # Remove unwanted characters except letters, numbers, underscores, hyphens, and spaces
    cleaned_name = re.sub(r"[^a-zA-Z0-9_\- ]", "", name).strip()
    # Replace spaces and hyphens with underscores
    cleaned_name = re.sub(r"[\s\-]+", "_", cleaned_name)
    return cleaned_name

class MatchingRequest(BaseModel):
    selectedFaciliooEntity: str
    selectedCrmEntity: str
    matchedFields: List[str]

@app.post("/save-matching-columns")
async def save_matching_columns(request: MatchingRequest, authorization: str = Header(None)):
    db = SessionLocal()
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Unauthorized")
    try:
        selectedFaciliooEntity = table_entity_name(request.selectedFaciliooEntity)
        selectedCrmEntity = table_entity_name(request.selectedCrmEntity)
        if not selectedFaciliooEntity or not selectedCrmEntity:
            raise HTTPException(status_code=400, detail="Both entities must be provided.")
        entity_pair = f"{selectedFaciliooEntity}-{selectedCrmEntity}"
        db.execute(text("""
            CREATE TABLE IF NOT EXISTS matching_table (
                entity_pair TEXT PRIMARY KEY,
                matched_fields JSONB
            )
        """))
        db.commit()

        # Clean and validate matched fields
        non_empty_fields = []
        for field in request.matchedFields:
            if not field or not field.strip():
                continue
            # Split the field into source and target
            if "-" not in field:
                logger.error(f"Invalid matched field format: {field}. Skipping.")
                continue
            source_field, target_field = field.split("-", 1)
            non_empty_fields.append(f"{source_field}-{target_field}")

        if not non_empty_fields:
            raise HTTPException(status_code=400, detail="No valid matched fields to insert.")

        # Check if the entity pair already exists
        existing_row = db.execute(
            text("SELECT matched_fields FROM matching_table WHERE entity_pair = :entity_pair"),
            {"entity_pair": entity_pair}
        ).fetchone()

        if existing_row:
            # Overwrite existing fields with the new fields to preserve order
            db.execute(
                text("UPDATE matching_table SET matched_fields = :fields WHERE entity_pair = :entity_pair"),
                {"fields": json.dumps(non_empty_fields), "entity_pair": entity_pair}
            )
        else:
            # Insert new row
            db.execute(
                text("INSERT INTO matching_table (entity_pair, matched_fields) VALUES (:entity_pair, :fields)"),
                {"entity_pair": entity_pair, "fields": json.dumps(non_empty_fields)}
            )

        db.commit()
        return {"success": True, "message": f"Successfully saved {len(non_empty_fields)} matched fields for '{entity_pair}'."}

    except Exception as e:
        db.rollback()
        print("Error saving matching columns:", str(e))
        raise HTTPException(status_code=500, detail=f"Error saving matching columns: {str(e)}")

    finally:
        db.close()
        
@app.get("/get-entity-data")
async def get_entity_data(entity_name: str, db: Session = Depends(get_db), authorization: Optional[str] = Header(None)):
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Unauthorized")
    try:
        entity_name = table_entity_name(entity_name)
        query = text(f"SELECT * FROM {entity_name}")  # Replace with your table naming convention
        result = db.execute(query)
        entity_data = [dict(row) for row in result.mappings()]
        if not entity_data:
            raise HTTPException(status_code=404, detail=f"No data found for entity: {entity_name}")
        return {"success": True, "data": entity_data}
    except Exception as e:
        logger.error(f"Error fetching entity data for {entity_name}: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error fetching entity data: {str(e)}")
    
@app.get("/get-matching-fields")
async def get_matching_fields(selectedFaciliooEntity: str, selectedCrmEntity: str, authorization: str = Header(None)):
    db = SessionLocal()
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Unauthorized")

    try:
        selectedFaciliooEntity = table_entity_name(selectedFaciliooEntity)
        #selectedCrmEntity = table_entity_name(selectedCrmEntity)
        if not selectedFaciliooEntity or not selectedCrmEntity:
            raise HTTPException(status_code=400, detail="Both entities must be provided.")
        entity_pair = f"{selectedFaciliooEntity}-{selectedCrmEntity}"
        existing_row = db.execute(
            text("SELECT matched_fields FROM matching_table WHERE entity_pair = :entity_pair"),
            {"entity_pair": entity_pair}).fetchone()
        if existing_row:
            return {"success": True, "matched_fields": existing_row[0]}
        else:
            return {"success": True, "matched_fields": None}
    except Exception as e:
        print("Error fetching matching fields:", str(e))
        raise HTTPException(status_code=500, detail=f"Error fetching matching fields: {str(e)}")
    finally:
        db.close()


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


def get_crm_entities_status(access_token: str):
    try:
        # Define the CRM API endpoint to fetch entities
        query_url = f"{CRM_URL}/EntityDefinitions"
        headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json"
        }
        response = requests.get(query_url, headers=headers)
        entities_with_status = []
        if response.status_code == 200:
            entity_data = response.json().get("value", [])  # Adjust key based on CRM API response
            for entity in entity_data:
                entity_name = entity.get("LogicalName", "Unknown Entity")  # Adjust key as per CRM response
                entity_url = f"{CRM_URL}/{entity_name}"  # Adjust endpoint to fetch records for the entity
                entity_response = requests.get(entity_url, headers=headers)
                if entity_response.status_code == 200:
                    entity_records = entity_response.json().get("value", [])  # Adjust key based on CRM API response
                    has_data = bool(entity_records)  # True if data exists, False otherwise
                else:
                    logger.warning(f"Failed to fetch data for entity {entity_name}. Status code: {entity_response.status_code}")
                    has_data = False  # Assume no data if API call fails
                entities_with_status.append({"name": entity_name, "has_data": has_data})
        else:
            logger.error(f"Failed to fetch CRM entities. Status: {response.status_code}, Response: {response.text}")
    except Exception as e:
        logger.error(f"Error checking CRM entities: {str(e)}", exc_info=True)

    return entities_with_status



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

        # Parse the response to extract field names and required status
        attributes_data = response.json().get("value", [])
        columns = [
            {
                "name": attribute["LogicalName"],
                "mandatory": attribute.get("RequiredLevel", {}).get("Value") == "ApplicationRequired"
            }
            for attribute in attributes_data
        ]

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

def extract_record_id_from_error(response):
    try:
        # Assuming the response JSON contains the error details in a structure like:
        # {"error": {"message": "Conflict", "details": [{"id": "existing_record_id"}]}}
        error_data = response.json()
        
        # Check if the error contains the details with the ID
        if "error" in error_data and "details" in error_data["error"]:
            for detail in error_data["error"]["details"]:
                # Adjust based on the actual structure of your CRM's error response
                if "id" in detail:
                    return detail["id"]
        
        # Fallback if ID is not found, raise an exception
        raise ValueError("Could not extract record ID from error response.")
    except Exception as e:
        logger.error(f"Error extracting record ID from error response: {str(e)}")
        raise ValueError("Failed to parse the error response.")
    


def validate_and_convert_guid(value):
    try:
        if isinstance(value, str):
            if len(value) == 36 and '-' in value:
                return str(uuid.UUID(value))  # Validate and return as-is
            elif len(value) == 32 and all(c in '0123456789abcdefABCDEF' for c in value):
                return str(uuid.UUID(value))
            else:
                return None  # Invalid format
        elif isinstance(value, int):
            namespace = uuid.NAMESPACE_OID  # Use a fixed namespace for consistency
            return str(uuid.uuid5(namespace, str(value)))
        else:
            return None  # Invalid type
    except (ValueError, AttributeError, TypeError):
        return None
    

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

def sanitize_text(value: str) -> str:
    """ Preprocess text fields to avoid triggering text analytics. """
    if not isinstance(value, str):
        return value  # Skip processing for non-text fields
    sanitized_value = value.strip().lower()  # Normalize case and remove whitespace
    sanitized_value = ''.join(e for e in sanitized_value if e.isalnum() or e.isspace())  # Remove special characters
    return sanitized_value[:250]  # Truncate to 250 characters to avoid analytics triggers

def preprocess_field(value):
    # Example: Remove special characters or format the data
    if isinstance(value, str):
        return value.strip()  # Remove leading/trailing whitespace
    return value


def migrate_entity_to_crm(entity_data: dict, matched_fields: list, selected_crm_entity: str) -> Dict:
    # If matched_fields is a JSON string, parse it to a list
    if isinstance(matched_fields, str):
        matched_fields = json.loads(matched_fields)
    
    access_token = get_crm_access_token()
    crm_url = f"{CRM_URL}/{correct_entity_name(selected_crm_entity)}"
    logger.info(f"Matched fields received: {matched_fields}")
    
    crm_data = {}
    failed_fields = []  # Track fields that fail validation or processing

    # Iterate over the matched fields (now a list of strings in the format 'facilioo_field-crm_field')
    for field_pair in matched_fields:
        # Split the string into 'facilioo_field' and 'crm_field'
        facilioo_field, crm_field = field_pair.split('-')
        crm_field = crm_field.replace(' *', '').strip()
        logger.info(f"Processing source field: {facilioo_field}, CRM field: {crm_field}")
        
        # Check if the facilioo_field exists in the entity data
        if facilioo_field in entity_data:
            field_value = entity_data[facilioo_field]
            
            # Handle GUID fields (e.g., attributeid)
            if crm_field.lower().endswith('id') and crm_field.lower() != 'id':
                guid_value = validate_and_convert_guid(field_value)
                if guid_value:
                    crm_data[crm_field] = guid_value
                else:
                    logger.warning(f"Invalid GUID value for field '{crm_field}'. Skipping field.")
                    failed_fields.append(f"{facilioo_field}-{crm_field}")  # Track failed field pair      

            else:
                # Anonymize sensitive data based on field type
                if facilioo_field.lower() in ['name', 'lastname', 'email', 'phone']:
                    anonymized_value = anonymize_data(str(field_value), crm_field.lower())
                    crm_data[crm_field] = anonymized_value
                else:
                    if crm_field=='name': 
                        field_value=field_value[:100]
                    crm_data[crm_field] = preprocess_field(field_value)
                    #crm_data[crm_field] = sanitize_text(field_value)

        else:
            logger.warning(f"Facilioo field {facilioo_field} not found in entity_data. Skipping field.")
            failed_fields.append(f"{facilioo_field}-{crm_field}")  # Track failed field pair
    
    logger.info(f"CRM Data being sent: {crm_data}")

    try:
        # Step 1: Use the unique_identifier_field to implement Upsert
        unique_identifier_field = matched_fields[0].split('-')[1]  # First CRM field
        unique_identifier_value = crm_data.get(unique_identifier_field)

        if unique_identifier_value:
            # Construct the query URL to check for existing records
            query_url = f"{crm_url}?$filter={unique_identifier_field} eq '{unique_identifier_value}'"
            query_response = requests.get(query_url, headers={
                "Authorization": f"Bearer {access_token}",
                "Content-Type": "application/json"
            })

            if query_response.status_code == 200:
                existing_records = query_response.json().get("value", [])
                if existing_records:
                    # Extract the record_id (primary key) from the existing record
                    record_id = existing_records[0].get(f"{selected_crm_entity.lower()}id")  # e.g., "attributeid"

                    if not record_id:
                        logger.error(f"Primary key field not found in existing records.")
                        return {
                            "success": False,
                            "action": "update_failed",
                            "record_id": entity_data.get("id", "N/A"),
                            "staging_status": "Success",
                            "crm_status": "Failed",
                            "error": "Primary key field not found in existing records.",
                            "failed_fields": failed_fields
                        }

                    # Record exists, perform an update
                    update_url = f"{crm_url}({record_id})"
                    update_response = requests.patch(update_url, headers={
                        "Authorization": f"Bearer {access_token}",
                        "Content-Type": "application/json"
                    }, data=json.dumps(crm_data))

                    if update_response.status_code in [200, 204]:
                        logger.info(f"Successfully updated record {record_id} in CRM.")
                        return {
                            "success": True,
                            "action": "updated",
                            "record_id": entity_data.get("id", "N/A"),
                            "staging_status": "Success",
                            "crm_status": "Success",
                            "error": None,
                            "failed_fields": failed_fields
                        }
                    else:
                        logger.error(f"Failed to update record {record_id} in CRM. Status code: {update_response.status_code}, Response: {update_response.text}")
                        return {
                            "success": False,
                            "action": "update_failed",
                            "record_id": entity_data.get("id", "N/A"),
                            "staging_status": "Success",
                            "crm_status": "Failed",
                            "error": update_response.text,
                            "failed_fields": failed_fields
                        }

        # Step 2: If the record does not exist, perform an insert
        create_response = requests.post(crm_url, headers={
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json"
        }, data=json.dumps(crm_data))

        if create_response.status_code in [200, 201, 204]:  # Success status codes
            logger.info(f"Successfully inserted new record into CRM. Response: {create_response.text}")
            return {
                "success": True,
                "action": "inserted",
                "record_id": entity_data.get("id", "N/A"),
                "staging_status": "Success",
                "crm_status": "Success",
                "error": None,
                "failed_fields": failed_fields
            }
        else:
            logger.error(f"Failed to insert or update record in CRM. Status code: {create_response.status_code}, Response: {create_response.text}")
            return {
                "success": False,
                "action": "creation_failed",
                "record_id": entity_data.get("id", "N/A"),
                "staging_status": "Success",
                "crm_status": "Failed",
                "error": create_response.text,
                "failed_fields": failed_fields
            }

    except Exception as e:
        logger.error(f"An error occurred while migrating entity data to CRM: {str(e)}")
        return {
            "success": False,
            "action": "error",
            "record_id": entity_data.get("id", "N/A"),
            "staging_status": "Success",
            "crm_status": "Failed",
            "error": str(e),
            "failed_fields": failed_fields
        }


def export_all_entity_to_excel(data: list, entity_name: str) -> str:
    from openpyxl import Workbook
    import re

    def sanitize_filename(name):
        return re.sub(r'[\\/*?:"<>|]', "_", name)

    wb = Workbook()
    ws = wb.active
    ws.title = entity_name

    if not data:
        raise ValueError("Data is empty. Cannot export to Excel.")

    headers = list(data[0].keys())  # Get all field names from the first record
    ws.append(headers)

    for record in data:
        if not isinstance(record, dict):
            raise ValueError(f"Invalid record in data: {record}. Expected a dictionary.")

        # Ensure all values are serializable (e.g., convert booleans to strings)
        record = {k: str(v) if isinstance(v, bool) else v for k, v in record.items()}
        anonymized_row = {}

        for field_name, value in record.items():
            if not isinstance(field_name, str):
                raise ValueError(f"Invalid field_name: {field_name}. Expected a string.")

            field_type = None

            if 'email' in field_name.lower():
                field_type = 'email'
            elif 'name' in field_name.lower() and 'last' not in field_name.lower():
                field_type = 'name'
            elif 'lastname' in field_name.lower():
                field_type = 'lastname'
            elif 'phone' in field_name.lower():
                field_type = 'phone'

            if field_type:
                anonymized_row[field_name] = anonymize_data(value, field_type)
            else:
                anonymized_row[field_name] = value  # No anonymization

        ws.append([str(anonymized_row.get(header)) for header in headers])

    file_path = f"{sanitize_filename(entity_name)}_export.xlsx"
    wb.save(file_path)
    return file_path
    
def export_entity_to_excel(data: list, entity_name: str, matched_fields: list) -> str:
    from openpyxl import Workbook

    wb = Workbook()
    ws = wb.active
    ws.title = entity_name

    # Ensure matched_fields is a list of strings
    if not isinstance(matched_fields, list):
        raise ValueError("matched_fields must be a list of strings.")

    crm_fields = []
    for field_pair in matched_fields:
        if isinstance(field_pair, str):
            crm_fields.append(field_pair.split('-')[1])
        else:
            raise ValueError(f"Invalid field_pair in matched_fields: {field_pair}. Expected a string.")

    ws.append(crm_fields)

    for record in data:
        if not isinstance(record, dict):
            raise ValueError(f"Invalid record in data: {record}. Expected a dictionary.")

        anonymized_row = {}

        for field_pair in matched_fields:
            if not isinstance(field_pair, str):
                raise ValueError(f"Invalid field_pair in matched_fields: {field_pair}. Expected a string.")

            facilioo_field, crm_field = field_pair.split('-')

            if facilioo_field in record:
                value = record[facilioo_field]
                field_type = None

                # ✅ Ensure crm_field is a string before using .lower()
                if isinstance(crm_field, str):
                    if 'email' in crm_field.lower():
                        field_type = 'email'
                    elif 'name' in crm_field.lower() and 'last' not in crm_field.lower():
                        field_type = 'name'
                    elif 'lastname' in crm_field.lower():
                        field_type = 'lastname'
                    elif 'phone' in crm_field.lower():
                        field_type = 'phone'

                if field_type:
                    anonymized_row[crm_field] = anonymize_data(value, field_type)
                else:
                    anonymized_row[crm_field] = value  # No anonymization
            else:
                anonymized_row[crm_field] = None

        ws.append([anonymized_row.get(crm_field, None) for crm_field in crm_fields])

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


def process_batch(batch: List[dict], matched_fields: list, selected_crm_entity: str) -> dict:
    success_count = 0
    error_count = 0
    update_count = 0
    insert_count = 0
    results = []

    for record in batch:
        try:
            crm_result = migrate_entity_to_crm(record, matched_fields, selected_crm_entity)
            if crm_result["success"]:
                success_count += 1
                if crm_result["action"] == "updated":
                    update_count += 1
                elif crm_result["action"] == "inserted":
                    insert_count += 1
            else:
                error_count += 1

            results.append({
                "record_id": record.get("id"),
                "staging_status": "Success",
                "crm_status": "Success" if crm_result["success"] else "Failed",
                "action": crm_result.get("action", "none"),
                "error": crm_result.get("error", None),
                "failed_fields": crm_result.get("failed_fields", [])
            })
        except Exception as e:
            error_count += 1
            results.append({
                "record_id": record.get("id"),
                "staging_status": "Failed",
                "crm_status": "Failed",
                "error": str(e),
                "failed_fields": []
            })

    return {
        "success_count": success_count,
        "error_count": error_count,
        "update_count": update_count,
        "insert_count": insert_count,
        "results": results
    }

def migrate_entity_in_parallel(entity_data: List[dict], matched_fields: list, selected_crm_entity: str, batch_size: int = 100) -> dict:
    total_records = len(entity_data)
    batches = [entity_data[i:i + batch_size] for i in range(0, total_records, batch_size)]

    success_count = 0
    error_count = 0
    update_count = 0
    insert_count = 0
    results = []

    with ThreadPoolExecutor() as executor:
        futures = [executor.submit(process_batch, batch, matched_fields, selected_crm_entity) for batch in batches]

        for future in as_completed(futures):
            batch_result = future.result()
            success_count += batch_result["success_count"]
            error_count += batch_result["error_count"]
            update_count += batch_result["update_count"]
            insert_count += batch_result["insert_count"]
            results.extend(batch_result["results"])

    return {
        "total_records": total_records,
        "success_count": success_count,
        "error_count": error_count,
        "update_count": update_count,
        "insert_count": insert_count,
        "results": results
    }

def fetch_entity_data_in_chunks(db: Session, entity_name: str, matched_fields: list, chunk_size: int = 1000) -> Generator[List[dict], None, None]:
    """
    Fetch data from the database in chunks using a generator.

    Args:
        db (Session): Database session.
        entity_name (str): Name of the entity/table.
        matched_fields (list): List of fields to match (unused in this function but kept for compatibility).
        chunk_size (int): Number of records to fetch per chunk.

    Yields:
        List[dict]: A list of records (dictionaries) for each chunk.
    """
    offset = 0
    while True:
        query = text(f"SELECT * FROM {entity_name} LIMIT {chunk_size} OFFSET {offset}")
        result = db.execute(query)
        records = [dict(zip(result.keys(), row)) for row in result.fetchall()]
        if not records:
            break
        yield records
        offset += chunk_size

class MigrateEntityRequest(BaseModel):
    selected_facilioo_entity: str
    selected_crm_entity: str


class MigrateResponse(BaseModel):
    total_records: int
    success_count: int
    error_count: int
    update_count: int
    insert_count: int
    entity_name: str  
    results: List[dict]
    excel_file_url: Optional[str]

@app.post("/migrate-entity-new", response_model=MigrateResponse)
async def migrate_entity(
    request: MigrateEntityRequest,
    background_tasks: BackgroundTasks,
    authorization: str = Header(None)
):
    selected_facilioo_entity = table_entity_name(request.selected_facilioo_entity)
    selected_crm_entity = request.selected_crm_entity

    # Fetch matched fields
    matched_fields_response = await get_matching_fields(selected_facilioo_entity, selected_crm_entity, authorization)
    matched_fields_list = matched_fields_response.get("matched_fields", [])
    if not matched_fields_list:
        raise HTTPException(status_code=400, detail="No matched fields found for the selected entities.")

    # Fetch data from staging database in chunks
    with SessionLocal() as db:
        entity_data = []
        for chunk in fetch_entity_data_in_chunks(db, selected_facilioo_entity, matched_fields_list):
            entity_data.extend(chunk)

    if not entity_data:
        raise HTTPException(status_code=400, detail="No records found in the staging database.")

    # Process data in parallel
    migration_result = migrate_entity_in_parallel(entity_data, matched_fields_list, selected_crm_entity)

    # Export data to Excel
    try:
        excel_file_path = export_entity_to_excel(entity_data, selected_crm_entity, matched_fields_list)
        background_tasks.add_task(delete_file_after_delay, excel_file_path, delay=180)
    except Exception as e:
        logger.error(f"Failed to export data to Excel: {str(e)}")
        raise HTTPException(status_code=500, detail="Failed to export data to Excel.")

    return {
        "total_records": migration_result["total_records"],
        "success_count": migration_result["success_count"],
        "error_count": migration_result["error_count"],
        "update_count": migration_result["update_count"],
        "insert_count": migration_result["insert_count"],
        "entity_name": selected_facilioo_entity,
        "results": migration_result["results"],
        "excel_file_url": f"/download-excel/{os.path.basename(excel_file_path)}"
    }



@app.post("/migrate-entity", response_model=MigrateResponse)
async def migrate_entity(
    request: MigrateEntityRequest,
    background_tasks: BackgroundTasks,
    authorization: str = Header(None)
):
    selected_facilioo_entity = table_entity_name(request.selected_facilioo_entity)
    selected_crm_entity = request.selected_crm_entity

    # Fetch matched fields
    matched_fields_response = await get_matching_fields(selected_facilioo_entity, selected_crm_entity, authorization)
    matched_fields_list = matched_fields_response.get("matched_fields", [])
    if not matched_fields_list:
        raise HTTPException(status_code=400, detail="No matched fields found for the selected entities.")

    # Fetch data from staging database
    with SessionLocal() as db:
        entity_data = fetch_entity_data_from_staging(db, selected_facilioo_entity, matched_fields_list)

    if not entity_data:
        raise HTTPException(status_code=400, detail="No records found in the staging database.")

    total_records = len(entity_data)
    success_count = 0
    error_count = 0
    update_count = 0
    insert_count = 0
    results = []
    data_to_export = []

    logger.info(f"Starting migration for {total_records} records.")

    for record in entity_data:
        try:
            # Log the record for debugging
            logger.info(f"Processing record: {record}")

            # Migrate to CRM
            crm_result = migrate_entity_to_crm(record, matched_fields_list, selected_crm_entity)
            if crm_result["success"]:
                success_count += 1
                if crm_result["action"] == "updated":
                    update_count += 1
                elif crm_result["action"] == "inserted":
                    insert_count += 1
            else:
                error_count += 1

            # Track failed fields
            failed_fields = crm_result.get("failed_fields", [])  # Ensure this is returned from migrate_entity_to_crm

            # Add the record to the export data
            data_to_export.append(record)

            # Log result
            results.append({
                "record_id": record.get("id"),
                "staging_status": "Success",
                "crm_status": "Success" if crm_result["success"] else "Failed",
                "action": crm_result.get("action", "none"),
                "error": crm_result.get("error", None),
                "failed_fields": failed_fields  # Include failed fields in the result
            })

        except Exception as e:
            error_count += 1
            logger.error(f"An error occurred while migrating record {record.get('id')}: {str(e)}", exc_info=True)
            results.append({
                "record_id": record.get("id"),
                "staging_status": "Failed",
                "crm_status": "Failed",
                "error": str(e),
                "failed_fields": []  # No specific fields failed, just a general error
            })

    # Export data to Excel
    try:
        excel_file_path = export_entity_to_excel(data_to_export, selected_crm_entity, matched_fields_list)
        background_tasks.add_task(delete_file_after_delay, excel_file_path, delay=180)
    except Exception as e:
        logger.error(f"Failed to export data to Excel: {str(e)}")
        raise HTTPException(status_code=500, detail="Failed to export data to Excel.")

    return {
        "total_records": total_records,
        "success_count": success_count,
        "error_count": error_count,
        "update_count": update_count,
        "insert_count": insert_count,
        "entity_name": selected_facilioo_entity,
        "results": results,
        "excel_file_url": f"/download-excel/{os.path.basename(excel_file_path)}"
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
