import psycopg2
from fastapi import FastAPI, HTTPException, Request, BackgroundTasks, Header, Depends, Query
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
from sqlalchemy import create_engine
from fastapi.staticfiles import StaticFiles
from sqlalchemy import text
import logging
from sqlalchemy.orm import Session
from typing import Optional, Any
import httpx
from urllib.parse import quote
from fastapi.responses import FileResponse
import time
from datetime import datetime
from sqlalchemy import Column, Integer, String, Boolean, DateTime, MetaData, Table, Float, JSON, and_
from sqlalchemy.dialects.postgresql import JSONB
from fastapi.responses import StreamingResponse
from sqlalchemy.exc import SQLAlchemyError
import re
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Generator
import pandas as pd
from fastapi.responses import PlainTextResponse, JSONResponse, HTMLResponse
import xml.etree.ElementTree as ET
from sqlalchemy import insert

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
# Load environment variables
load_dotenv()
USER = os.getenv("USER")
PASSWORD = os.getenv("PASSWORD")
HOST = os.getenv("HOST")
PORT = os.getenv("PORT")
DBNAME = os.getenv("DBNAME")
API_URL = os.getenv("API_URL")
API_KEY = os.getenv("API_KEY")
APP_ID = os.getenv("APP_ID")
CRM_MAIN_URL = os.getenv("CRM_MAIN_URL")

# CRM API Configuration
CRM_URL = os.getenv("CRM_URL")
TENANT_ID = os.getenv("TENANT_ID")
CLIENT_ID = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")
RESOURCE = os.getenv("RESOURCE")

MAX_PAGE_SIZE = 100

# FastAPI App Setup
app = FastAPI()
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()



class PageInfo(BaseModel):
    current_page: int
    page_size: int
    total_pages: int
    total_elements: int

class EntityResponse(BaseModel):
    success: bool
    message: str
    data: Dict
    page_info: PageInfo

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



IMPOWER_ENTITIES = [
    "connections",
    "contacts",
    "contracts",
    "documents",
    "document-tags",
    "error-codes",
    "heating-center-units",
    "heating-centers",
    "heating-cost-reports",
    "invoices",
    "invoice-items",
    "posting-items",
    "profit-and-loss-reports",
    "properties",
    "units"
]


def get_impower_entities(access_token: str):
    try:
        # Define the impower API endpoint to fetch entities
        query_url = f"{API_URL}/entities"  # Adjust the endpoint as per your impower API
        response = requests.get(query_url, headers={
            "Authorization": f"Bearer {access_token}"
        })

        if response.status_code != 200:
            raise Exception(f"Failed to fetch impower entities: {response.text}")

        # Parse the response to extract entity names
        entities_data = response.json().get("value", [])  # Adjust based on the impower API response structure
        entities = [entity["name"].lower().replace(" ", "-") for entity in entities_data]  # Format entity names

        # Update the global IMPOWER_ENTITIES array
        global IMPOWER_ENTITIES
        IMPOWER_ENTITIES = entities

        return entities
    except Exception as e:
        logging.error(f"Error fetching impower entities: {str(e)}")
        raise

@app.get("/impower-entities")
async def get_impower_entities():
    return IMPOWER_ENTITIES

class impowerEntitiesRequest(BaseModel):
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
    """Infer SQLAlchemy schema from record dictionary with nullable columns."""
    print(f"Using Column class from: {Column.__module__}")  # Should print sqlalchemy.sql.schema

    columns = [Column("id", Integer, primary_key=True, nullable=False)]

    for key, value in record.items():
        if key == "id":
            continue

        column_name = key.lower()
        if column_name in RESERVED_KEYWORDS:
            column_name = f"{column_name}_field"

        py_type = type(value)

        type_map = {
            bool: Boolean,
            str: String,
            int: Integer,
            float: Float,
            dict: JSON,
            list: JSON,
            datetime: DateTime
        }

        col_type = type_map.get(py_type, String)
        columns.append(Column(column_name, col_type, nullable=True))

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



# Pydantic model for the response
class EntityStatus(BaseModel):
    entity_name: str
    has_data: bool

# Function to create the "impower_entities_with_status" table
def create_impower_entities_with_status_table():
    try:
        # Define the table schema
        metadata = MetaData()
        impower_entities_with_status = Table(
            "impower_entities_with_status",
            metadata,
            Column("entity_name", String, nullable=False, primary_key=True),  # Entity name (primary key)
            Column("has_data", Boolean, nullable=False),  # Boolean flag for data status
        )
        # Create the table in the database
        metadata.create_all(engine)
        logger.info("Table 'impower_entities_with_status' created successfully.")
    except Exception as e:
        logger.error(f"Error creating table 'impower_entities_with_status': {e}")
        raise

# Endpoint to fetch entities with status from the database
@app.get("/get-impower-entities-with-status", response_model=List[EntityStatus])
def get_impower_entities_with_status(db: Session = Depends(get_db)):
    try:
        # Query the database to fetch all entities with their status
        stmt = text("SELECT entity_name, has_data FROM impower_entities_with_status")
        result = db.execute(stmt)
        entities_with_status = [{"entity_name": row[0], "has_data": row[1]} for row in result]
        return entities_with_status
    except Exception as e:
        logger.error(f"Error fetching entities with status from the database: {str(e)}")
        raise HTTPException(status_code=500, detail="Internal server error")

def strip_html_tags(text):
    clean = re.sub(r'<.*?>', '', text)
    return clean.strip() 


def get_table_class(table_name: str) -> Table:
    """Dynamically get the SQLAlchemy Table class for the given table name"""
    # This should return your actual table class - adjust according to your models
    # For example, if you have a Properties model:

    raise ValueError(f"Unknown table: {table_name}")

def insert_entity_data(table_name: str, records: List[Dict]) -> int:
    """Universal function to insert records for any entity"""
    if not records:
        raise HTTPException(
            status_code=400,
            detail="No records provided for insertion"
        )

    try:
        with SessionLocal() as session:
            # Reflect the table structure from database
            metadata = MetaData()
            table = Table(table_name, metadata, autoload_with=session.bind)
            
            # Prepare data for insertion or update
            validated_records = []
            table_column_names = set(col.name for col in table.columns)

            for record in records:
                processed_record = {}
                for col_name in table_column_names:
                    column = table.columns[col_name]
                    
                    # Handle nested structures by converting to JSON string
                    if col_name in record and isinstance(record[col_name], (dict, list)):
                        processed_record[col_name] = json.dumps(record[col_name])
                    elif col_name in record:
                        processed_record[col_name] = record[col_name]
                    else:
                        # If it's a nullable column, set it to None
                        if column.nullable:
                            processed_record[col_name] = None
                        elif not column.primary_key:
                            # For non-nullable and non-primary key columns, set to a default value
                            processed_record[col_name] = None  # or use a default value if needed

                validated_records.append(processed_record)

            # Get primary key columns
            primary_key_columns = [col.name for col in table.primary_key.columns]

            # Iterate over records and perform upsert
            updated_count = 0
            inserted_count = 0
            for record in validated_records:
                # Check if the record already exists
                if not primary_key_columns:
                    # If no primary key, always insert
                    stmt = table.insert().values(record)
                    session.execute(stmt)
                    inserted_count += 1
                    continue
                    
                # Build filter condition for primary keys
                filter_conditions = []
                for pk_col in primary_key_columns:
                    if pk_col in record:
                        filter_conditions.append(getattr(table.c, pk_col) == record[pk_col])
                
                if not filter_conditions:
                    # If no PK values provided, insert
                    stmt = table.insert().values(record)
                    session.execute(stmt)
                    inserted_count += 1
                    continue
                
                # Check if record exists
                existing = session.execute(
                    table.select().where(and_(*filter_conditions)))
                existing_record = existing.first()

                if existing_record:
                    # Update existing record
                    stmt = (
                        table.update()
                        .where(and_(*filter_conditions))
                        .values(record))
                    session.execute(stmt)
                    updated_count += 1
                else:
                    # Insert new record
                    stmt = table.insert().values(record)
                    session.execute(stmt)
                    inserted_count += 1

            session.commit()
            return inserted_count + updated_count

    except SQLAlchemyError as e:
        logger.error(f"Database upsert error: {str(e)}")
        if 'session' in locals():
            session.rollback()
        raise HTTPException(
            status_code=500,
            detail=f"Data insertion or update failed: {str(e)}"
        )
                
            
@app.post("/fetch-and-save-entity")
async def fetch_and_save_entity(
    entity_name: str,
    background_tasks: BackgroundTasks,
    authorization: Optional[str] = Header(None),
    page: int = 0,
    size: int = 30
):
    """Universal entity fetcher that handles both JSON and XML"""
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Unauthorized")
    
    access_token = authorization.split("Bearer ")[1]
    headers = {"Authorization": f"Bearer {access_token}"}
    params = {"size": size, "page": page}
    url = f"{API_URL}/{entity_name}"

    try:
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        
        content_type = response.headers.get('Content-Type', '')
        
        # XML Handling
        if 'application/xml' in content_type or 'text/xml' in content_type:
            try:
                root = ET.fromstring(response.text)
                records = parse_xml_to_records(root)
                return await handle_records(entity_name, records, page, size, len(records))
            except ET.ParseError as e:
                logger.error(f"XML parsing failed: {str(e)}")
                raise HTTPException(status_code=422, detail="Invalid XML format")
        
        # JSON Handling
        try:
            data = response.json()
        except ValueError:
            raise HTTPException(status_code=422, detail="Invalid JSON response")
            
        if isinstance(data, dict):
            if 'content' in data:  # Paginated response
                records = normalize_records(data['content'])
                page_info = {
                    'current_page': page,
                    'page_size': size,
                    'total_pages': data.get('totalPages', 1),
                    'total_elements': data.get('totalElements', len(records))
                }
            else:  # Single object
                records = [normalize_records(data)]
                page_info = None
        elif isinstance(data, list):  # Direct array response
            records = normalize_records(data)
            page_info = None
        else:
            raise HTTPException(status_code=422, detail="Unrecognized JSON structure")
            
        return await handle_records(entity_name, records, page, size, len(records), page_info)

    except requests.RequestException as e:
        logger.error(f"Request failed: {str(e)}")
        raise HTTPException(status_code=502, detail="Upstream service error")
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")

def normalize_records(records: Union[List[Dict], Dict]) -> List[Dict]:
    """Normalize records to consistent format"""
    normalized = []
    
    if isinstance(records, dict):
        records = [records]
    
    for record in records:
        if not isinstance(record, dict):
            continue
            
        # Flatten nested structures
        normalized_record = {}
        for key, value in record.items():
            if isinstance(value, dict):
                for subkey, subvalue in value.items():
                    normalized_record[f"{key}_{subkey}"] = subvalue
            else:
                normalized_record[key] = value
                
        # Standardize field names
        normalized_record = {k.lower().replace('.', '_'): v 
                           for k, v in normalized_record.items()}
        normalized.append(normalized_record)
    
    return normalized

def parse_xml_to_records(root: ET.Element) -> List[Dict]:
    """Convert XML structure to normalized records"""
    records = []
    
    # Handle different XML structures
    if root.tag.endswith('List'):  # List response
        for item in root:
            records.append(xml_element_to_dict(item))
    elif root.tag.endswith('Response'):  # Wrapped response
        content = root.find('content')
        if content is not None:
            for item in content:
                records.append(xml_element_to_dict(item))
    else:  # Single item
        records.append(xml_element_to_dict(root))
    
    return records

def xml_element_to_dict(element: ET.Element) -> Dict[str, Any]:
    """Convert XML element to dictionary"""
    result = {}
    for child in element:
        if len(child) > 0:  # Has children
            result[child.tag] = xml_element_to_dict(child)
        else:
            result[child.tag] = child.text
    return result

async def handle_records(
    entity_name: str,
    records: List[Dict],
    page: int,
    size: int,
    count: int,
    page_info: Optional[Dict] = None
):
    """Common handling for normalized records"""
    if not records:
        raise HTTPException(status_code=404, detail="No records found")
    
    table_name = sanitize_table_name(entity_name)
    first_record = records[0]
    
    try:
        created_columns = create_table_for_entity(table_name, first_record)
        inserted_count = insert_entity_data(table_name, records)
        
        response_data = {
            "table_name": table_name,
            "columns": list(created_columns) if created_columns else [],
            "sample_data": first_record,
            "inserted_count": inserted_count,
            "page_info": page_info or {
                "current_page": page,
                "page_size": size,
                "total_pages": (count + size - 1) // size,
                "total_elements": count
            }
        }
        
        return JSONResponse(content=jsonable_encoder(response_data))
    
    except Exception as e:
        logger.error(f"Database operation failed: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail="Database operation failed")



async def fetch_entity_data(
    access_token: str,
    entity_name: str,
    page: int = 0,
    size: int = 30
) -> Dict:
    """Fetch data for a specific entity with proper record extraction"""
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }

    params = {
        "size": size,
        "page": page
    }
    
    try:
        # Handle entity-specific endpoints
        if entity_name.lower() == "documents":
            response = requests.get(f"{API_URL}/documents", headers=headers, params=params)
        elif entity_name.lower() == "invoices":
            response = requests.get(f"{API_URL}/invoices", headers=headers, params=params)
        elif entity_name.lower() == "contacts":
            response = requests.get(f"{API_URL}/contacts", headers=headers, params=params)
        else:
            response = requests.get(f"{API_URL}/{entity_name}", headers=headers, params=params)

        if response.status_code != 200:
            error_detail = {
                "status_code": response.status_code,
                "response_text": response.text[:200],
                "request_url": response.request.url,
            }
            logger.error(f"API request failed: {json.dumps(error_detail, indent=2)}")
            raise HTTPException(
                status_code=response.status_code,
                detail=f"API request failed: {response.text[:200]}"
            )

        data = response.json()
        content = data.get("content", [])

        if not content:
            raise HTTPException(
                status_code=404,
                detail=f"No data found for entity {entity_name}"
            )

        # Process records - ensure each has unique ID
        records = []
        seen_ids = set()
        
        for item in content:
            # Handle nested entity structure if present
            if isinstance(item, dict) and len(item) == 1:
                # Case where entity is nested under a single key
                record = next(iter(item.values()))
                if isinstance(record, list):
                    # If the value is a list, take first item
                    record = record[0] if record else {}
            else:
                record = item
            
            # Ensure record has an ID and it's unique in this batch
            if not isinstance(record, dict):
                continue
            if 'id' not in record:
                logger.warning(f"Record missing ID: {record}")
                continue
            if record['id'] in seen_ids:
                logger.warning(f"Duplicate ID found: {record['id']}")
                continue
            seen_ids.add(record['id'])
            records.append(record)
        if not records:
            raise HTTPException(
                status_code=404,
                detail=f"No valid records found for entity {entity_name}"
            )
        # Use first record for schema
        first_record = records[0]
        table_name = sanitize_table_name(entity_name)
        
        # Get pagination info
        page_info = {
            "current_page": page,
            "page_size": size,
            "total_pages": data.get("totalPages", 1),
            "total_elements": data.get("totalElements", len(records))
        }

        return {
            "table_name": table_name,
            "sample_data": first_record,
            "records": records,
            "page_info": page_info
        }

    except HTTPException:
        raise
    except requests.RequestException as e:
        logger.error(f"Network error fetching {entity_name}: {str(e)}")
        raise HTTPException(
            status_code=503,
            detail=f"Network error accessing {entity_name} API"
        )

def sanitize_table_name(entity_name: str) -> str:
    """Convert entity name to safe table name"""
    return entity_name.lower().replace('-', '_')

def extract_columns(record: Dict) -> List[str]:
    """Extract and clean column names from a record"""
    columns = []
    for key in record.keys():
        clean_key = key.lower().replace(' ', '_')
        if needs_quoting(clean_key):
            clean_key = f'"{clean_key}"'
        columns.append(clean_key)
    return columns

def needs_quoting(column_name: str) -> bool:
    """Check if column name needs SQL quoting"""
    return (column_name.lower() in RESERVED_KEYWORDS or 
            not column_name.replace('_', '').isalnum())

class CRMEntityLinkRequest(BaseModel):
    entity_name: str

@app.get("/crm-entity-link")
async def get_crm_entity_link(entity_name: str, authorization: Optional[str] = Header(None)):
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Unauthorized")

    access_token = authorization.split("Bearer ")[1]
    try:
        # Generate the CRM entity link
        crm_entity_link = f"{CRM_MAIN_URL}/main.aspx?appid={APP_ID}&pagetype=entitylist&etn={entity_name}"
        
        # Return the link in the response
        return JSONResponse(content={"crm_entity_link": crm_entity_link})
    except Exception as e:
        logger.error(f"Error generating CRM entity link: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error generating CRM entity link: {str(e)}")


@app.post("/impower-entities-with-status")
async def get_impower_entities_with_status(
    request: impowerEntitiesRequest,
    db: Session = Depends(get_db)
):
    access_token = request.access_token
    if not access_token:
        raise HTTPException(status_code=401, detail="Access token is required")

    headers = {
        "Authorization": f"Bearer {access_token}",
        "accept": "application/json"
    }
    
    # List of entities to check (adjust as needed)
    entities = IMPOWER_ENTITIES
    
    entities_with_status = []

    # First verify the API is reachable
    try:
        test_response = requests.get(
            f"{API_URL}/properties",
            headers=headers,
            timeout=5
        )
        if test_response.status_code == 404:
            raise HTTPException(
                status_code=400,
                detail="API endpoint not found. Please verify the base URL"
            )
    except requests.exceptions.RequestException as e:
        raise HTTPException(
            status_code=502,
            detail=f"API connection failed: {str(e)}"
        )

    # Check each entity
    for entity in entities:
        try:
            logger.info(f"Checking status for entity: {entity}")
            
            # Try the direct endpoint first
            response = requests.get(
                f"{API_URL}/{entity}",
                headers=headers,
                timeout=5
            )
            
            if response.status_code == 200:
                entities_with_status.append({
                    "entity_name": entity,
                    "has_data": True,
                    "status": "active"
                })
            else:
                # If direct endpoint fails
                response = requests.get(
                    f"{API_URL}/{entity}",
                    headers=headers,
                    timeout=5
                )
                
                if response.status_code == 200:
                    entities_with_status.append({
                        "entity_name": entity,
                        "has_data": True,
                        "status": "active"
                    })
                else:
                    entities_with_status.append({
                        "entity_name": entity,
                        "has_data": False,
                        "status": "inactive"
                    })
                    logger.warning(f"Entity {entity} not found")

        except Exception as e:
            logger.error(f"Error checking entity {entity}: {str(e)}")
            entities_with_status.append({
                "entity_name": entity,
                "has_data": False,
                "status": "error"
            })

    # Save results to database
    try:
        for entity_status in entities_with_status:
            stmt = text("""
                INSERT INTO impower_entities_with_status (entity_name, has_data, last_checked)
                VALUES (:entity_name, :has_data, CURRENT_TIMESTAMP)
                ON CONFLICT (entity_name) DO UPDATE SET
                has_data = EXCLUDED.has_data,
                last_checked = CURRENT_TIMESTAMP
            """)
            db.execute(stmt, {
                "entity_name": entity_status["entity_name"],
                "has_data": entity_status["has_data"]
            })
        db.commit()
    except Exception as e:
        db.rollback()
        logger.error(f"Database error: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail="Failed to save entity status"
        )

    return {
        "results": entities_with_status,
        "success": True,
        "count": len(entities_with_status),
        "active_count": sum(1 for e in entities_with_status if e["has_data"])
    }


class ExportEntityRequest(BaseModel):
    entity_name: str

def create_sample_record(schema: List[Column]) -> Dict[str, Any]:
    """Create a sample record with type-appropriate default values"""
    sample_record = {}
    for column in schema:
        try:
            if column.type == str:
                sample_record[column.name] = "sample_value"
            elif column.type == int:
                sample_record[column.name] = 0
            elif column.type == float:
                sample_record[column.name] = 0.0
            elif column.type == bool:
                sample_record[column.name] = False
            elif column.type == dict:
                sample_record[column.name] = {"key": "value"}
            elif column.type == list:
                sample_record[column.name] = ["item1", "item2"]
            elif column.type == datetime:
                sample_record[column.name] = datetime.now().isoformat()
            else:
                sample_record[column.name] = None
        except Exception as e:
            logger.warning(f"Couldn't set default value for {column.name}: {str(e)}")
            sample_record[column.name] = None
    return sample_record

def fetch_single_entity_data(
    access_token: str,
    entity_name: str,
    entity_id: Optional[str] = None,
    page: int = 0,  # Changed from 1 to 0 to match API standard
    size: int = 10
) -> Dict[str, Any]:
    """
    Fetch data from the API with proper pagination and error handling
    Returns the full response including pagination metadata
    """
    headers = {
        "Authorization": f"Bearer {access_token}"
                }
    
    # Enforce API limits
    size = min(size, 100)  # Maximum page size per API requirements
    
    params = {
        "page": page,
        "size": size
    }
    
    try:
        # Handle different endpoint patterns
        if entity_id:
            # Single entity by ID
            url = f"{API_URL}/{entity_name}/{entity_id}"
            response = requests.get(url, headers=headers, timeout=10)
        else:
            # Paginated list
            url = f"{API_URL}/{entity_name}"
            response = requests.get(url, headers=headers, params=params, timeout=10)
        
        if response.status_code != 200:
            error_msg = f"API request failed: {response.status_code} - {response.text[:200]}"
            logger.error(error_msg)
            return {
                "success": False,
                "error": error_msg,
                "status_code": response.status_code
            }
        
        data = response.json()
        
        # Standardize response format
        if entity_id:
            return {
                "success": True,
                "data": data,
                "page_info": {
                    "current_page": 0,
                    "page_size": 1,
                    "total_pages": 1,
                    "total_elements": 1
                }
            }
        else:
            return {
                "success": True,
                "data": data.get("content", []),
                "page_info": {
                    "current_page": data.get("pageable", {}).get("pageNumber", page),
                    "page_size": data.get("pageable", {}).get("pageSize", size),
                    "total_pages": data.get("totalPages", 1),
                    "total_elements": data.get("totalElements", 0)
                }
            }
            
    except requests.exceptions.RequestException as e:
        error_msg = f"Network error fetching {entity_name}: {str(e)}"
        logger.error(error_msg)
        return {
            "success": False,
            "error": error_msg,
            "status_code": 503
        }


class ExportRequest(BaseModel):
    access_token: str

@app.post("/fetch-and-export_all_entities/")
def fetch_and_export_entity_data(
    request: ExportRequest,
    background_tasks: BackgroundTasks = None,
    db: Session = Depends(get_db),
):
    try:
        access_token = request.access_token
        all_data = {}

        # Loop through all the entities
        for entity in IMPOWER_ENTITIES:
            logger.info(f"Fetching data for entity: {entity}")
            data = fetch_single_entity_data(access_token, entity)
            if data:
                anonymized_records = []
                for record in data:
                    anonymized_record = {}
                    for field_name, field_value in record.items():
                        # Handle nested structures first
                        if isinstance(field_value, (dict, list)):
                            # Convert nested structures to JSON strings
                            anonymized_record[field_name] = json.dumps(field_value, ensure_ascii=False)
                        elif field_name.lower() in ['name', 'lastname', 'firstname', 'fullname', 'email', 'phone', 'phonenumber']:
                            anonymized_record[field_name] = anonymize_data(str(field_value), field_name.lower())
                        else:
                            anonymized_record[field_name] = field_value
                    anonymized_records.append(anonymized_record)
                all_data[entity] = anonymized_records
            else:
                logger.info(f"No data found for entity {entity}. Creating empty sheet.")
                all_data[entity] = []

        # Export the data to Excel with proper handling of complex types
        excel_file_path = export_all_entities_to_excel(all_data, "impower_export")

        if background_tasks:
            background_tasks.add_task(delete_file_after_delay, excel_file_path, delay=60)

        return {
            "success": True,
            "columns": list(all_data.keys()),
            "total_records": sum(len(data) for data in all_data.values()),
            "excel_file_url": f"/download-excel/{os.path.basename(excel_file_path)}"
        }
    
    except Exception as e:
        logger.error(f"Failed to export data to Excel: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Failed to export data to Excel: {str(e)}"
        )


def export_all_entities_to_excel(data: Dict[str, List[Dict[str, Any]]], entity_name: str) -> str:
    """Export data to Excel with proper handling of complex types"""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    excel_file_path = f"{entity_name}_export_{timestamp}.xlsx"
    
    try:
        with pd.ExcelWriter(excel_file_path, engine="openpyxl") as writer:
            for entity, records in data.items():
                if records:
                    # Create DataFrame with all records
                    df = pd.DataFrame(records)
                    
                    # Ensure all columns are present (handle case where some records have missing fields)
                    all_columns = set()
                    for record in records:
                        all_columns.update(record.keys())
                    
                    # Reindex DataFrame to include all possible columns
                    df = df.reindex(columns=list(all_columns))
                    
                    # Write to Excel
                    df.to_excel(writer, sheet_name=entity[:31], index=False)  # Sheet name max 31 chars
                else:
                    # Create empty DataFrame with sample columns if no data exists
                    empty_df = pd.DataFrame(columns=["id", "name", "created_at"])
                    empty_df.to_excel(writer, sheet_name=entity[:31], index=False)
        
        return excel_file_path
        
    except Exception as e:
        # Clean up partially created file if error occurs
        if os.path.exists(excel_file_path):
            os.remove(excel_file_path)
        raise RuntimeError(f"Excel export failed: {str(e)}")
    
@app.post("/fetch-and-export/")
def fetch_and_export_entity_data(
    request: ExportEntityRequest,  # Use the Pydantic model for request validation
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

@app.get("/fetch-entity-fields")
async def get_entity_fields(
    entity_name: str,
    authorization: str = Header(None),
    page: int = 1,
    size: int = 1
):
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Unauthorized")
    
    access_token = authorization.split("Bearer ")[1]
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json"
    }

    params = {"page": page, "size": size}
    
    try:
        response = requests.get(
            f"{API_URL}/{entity_name}",
            headers=headers,
            params=params
        )
        
        if response.status_code == 200:
            data = response.json()
            content = data.get("content", data.get("items", []))
            
            if not content:
                return JSONResponse(
                    status_code=404,
                    content={"success": False, "message": "No data found", "columns": []}
                )
            
            sample_record = content[0]
            if isinstance(sample_record, dict) and len(sample_record) == 1:
                sample_record = next(iter(sample_record.values()))
            
            columns = list(sample_record.keys()) if isinstance(sample_record, dict) else []
            
            return {
                "success": True,
                "columns": [col.lower() for col in columns],
                "sample_record": sample_record
            }
            
        raise HTTPException(
            status_code=response.status_code,
            detail=response.text[:200]
        )
        
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=str(e)
        )    

    
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
                impower_field, crm_field = field_pair.split('-')
                if impower_field in record:  # Ensure the source field exists
                    processed_record[impower_field] = record[impower_field]
                else:
                    logger.warning(f"impower field '{impower_field}' not found in record for entity: {entity_name}")
                    processed_record[impower_field] = None  # Handle missing fields safely
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
    selectedimpowerEntity: str
    selectedCrmEntity: str
    matchedFields: List[str]

@app.post("/save-matching-columns")
async def save_matching_columns(request: MatchingRequest, authorization: str = Header(None)):
    db = SessionLocal()
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Unauthorized")
    try:
        selectedimpowerEntity = table_entity_name(request.selectedimpowerEntity)
        selectedCrmEntity = table_entity_name(request.selectedCrmEntity)
        if not selectedimpowerEntity or not selectedCrmEntity:
            raise HTTPException(status_code=400, detail="Both entities must be provided.")
        entity_pair = f"{selectedimpowerEntity}-{selectedCrmEntity}"
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
async def get_matching_fields(selectedimpowerEntity: str, selectedCrmEntity: str, authorization: str = Header(None)):
    db = SessionLocal()
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Unauthorized")

    try:
        selectedimpowerEntity = table_entity_name(selectedimpowerEntity)
        #selectedCrmEntity = table_entity_name(selectedCrmEntity)
        if not selectedimpowerEntity or not selectedCrmEntity:
            raise HTTPException(status_code=400, detail="Both entities must be provided.")
        entity_pair = f"{selectedimpowerEntity}-{selectedCrmEntity}"
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


async def verify_api_key(api_key: str):
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json"
    }
    
    try:
        # Use the known working endpoint from your curl example
        async with httpx.AsyncClient() as client:
            response = await client.get(
                f"{API_URL}/properties",
                headers=headers
            )
        
        if response.status_code == 200:
            return True
        return False

    except httpx.RequestError as e:
        logger.error(f"HTTP request error: {str(e)}")
        return False

@app.post("/authenticate")
async def authenticate(request: Request):
    logger.info("API key authentication attempt")
    
    try:
        # Get API key from request body
        body = await request.json()
        api_key = body.get("api_key")
        
        if not api_key:
            logger.error("No API key provided")
            raise HTTPException(
                status_code=401,
                detail="API key is required"
            )

        # Verify the API key by making a request to a working endpoint
        is_valid = await verify_api_key(api_key)
        
        if is_valid:
            logger.info("Authentication successful with API key")
            log_migration_result(
                user_id=-1,
                staging_status="authentication_success",
                crm_status="not_updated",
                crm_entity_updated=False,
                error_message=None
            )
            return {
                "access_token": api_key,
                "token_type": "bearer"
            }
        else:
            logger.warning("API key authentication failed")
            log_migration_result(
                user_id=-1,
                staging_status="authentication_failed",
                crm_status="not_updated",
                crm_entity_updated=False,
                error_message="Invalid API key or insufficient permissions"
            )
            raise HTTPException(
                status_code=401,
                detail="Invalid API key or insufficient permissions"
            )

    except Exception as e:
        logger.error(f"Error during API key authentication: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail="Internal server error during authentication"
        )


def correct_entity_name(entity_name: str) -> str:
    special_cases = {
        'documentindex': 'documentindexes',
        'index': 'indices',
        'matrix': 'matrices',
        'vertex': 'vertices',
        'child': 'children',
        'person': 'people',
        'mouse': 'mice',
        'goose': 'geese',
        'tooth': 'teeth',
        'foot': 'feet',
        'ox': 'oxen',
        'man': 'men',
        'woman': 'women',
        'cactus': 'cacti',
        'fungus': 'fungi',
        'nucleus': 'nuclei',
        'syllabus': 'syllabi',
        'focus': 'foci',
        'radius': 'radii',
        'analysis': 'analyses',
        'basis': 'bases',
        'crisis': 'crises',
        'thesis': 'theses',
        'datum': 'data',
        'medium': 'media',
        'bacterium': 'bacteria',
        'curriculum': 'curricula',
        'alumnus': 'alumni',
        'addendum': 'addenda',
        'corpus': 'corpora',
        'genus': 'genera',
        'appendix': 'appendices',
        'vortex': 'vortices',
        'locus': 'loci'
    }
    
    if entity_name.endswith(('s', 'es', 'ies')) or entity_name in special_cases.values():
        return entity_name
    if entity_name in special_cases:
        return special_cases[entity_name]
    if entity_name.endswith('y'):
        if len(entity_name) > 1 and entity_name[-2] not in 'aeiou':
            return entity_name[:-1] + 'ies'
        else:
            return entity_name + 's'
    if entity_name.endswith('f'):
        return entity_name[:-1] + 'ves'
    elif entity_name.endswith('fe'):
        return entity_name[:-2] + 'ves'
    if entity_name.endswith('o'):
        if len(entity_name) > 1 and entity_name[-2] not in 'aeiou':
            return entity_name + 'es'
        else:
            return entity_name + 's'
    if entity_name.endswith(('s', 'x', 'z', 'ch', 'sh')):
        return entity_name + 'es'
    
    # Default case: add 's'
    return entity_name + 's'
    
    
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



def set_all_entities_customizable(access_token: str):
    try:
        # Fetch all entities
        entities_url = f"{CRM_URL}/EntityDefinitions"
        headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json"
        }
        response = requests.get(entities_url, headers=headers)
        
        if response.status_code != 200:
            logger.warning(f"Failed to fetch entities. Status code: {response.status_code}")
            return False

        # Loop through entities and update IsCustomizable
        entities_data = response.json().get("value", [])
        for entity in entities_data:
            entity_name = entity["LogicalName"]

            # Skip if the entity is not customizable
            if not entity.get("IsCustomizable", {}).get("CanBeChanged", False):
                logger.info(f"Skipping entity {entity_name} because it is not customizable.")
                continue

            update_url = f"{CRM_URL}/EntityDefinitions(LogicalName='{entity_name}')"
            payload = {
                "IsCustomizable": {
                    "Value": True,
                    "CanBeChanged": True
                }
            }

            # Send the PATCH request
            update_response = requests.patch(update_url, headers=headers, json=payload)
            if update_response.status_code == 204:
                logger.info(f"Entity {entity_name} is now customizable.")
            else:
                logger.warning(f"Failed to update entity {entity_name}. Status code: {update_response.status_code}")

        return True

    except Exception as e:
        logger.error(f"Error updating entities: {str(e)}")
        return False
    
    
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

        # Set all entities as customizable
        if not set_all_entities_customizable(crm_access_token):
            logger.warning("Failed to set all entities as customizable.")

        # Fetch CRM entities
        entities = get_crm_entities(crm_access_token)
        return {"entities": entities}
    except Exception as e:
        logger.error(f"Error fetching CRM entities: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error fetching CRM entities: {str(e)}")    

# Function to create the "crm_entities" table
def create_crm_entities_table():
    try:
        # Define the table schema
        metadata = MetaData()
        crm_entities = Table(
            "crm_entities",
            metadata,
            Column("entity_name", String, nullable=False, primary_key=True),  # Entity name (primary key)
            Column("is_active", Boolean, nullable=False, default=True),  # Boolean flag for active status
        )
        # Create the table in the database
        metadata.create_all(engine)
        logger.info("Table 'crm_entities' created successfully.")
    except Exception as e:
        logger.error(f"Error creating table 'crm_entities': {e}")
        raise


# Endpoint to fetch CRM entities and populate the table
@app.post("/crm-entities-table-save")
async def fetch_and_save_crm_entities(authorization: Optional[str] = Header(None), db: Session = Depends(get_db)):
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Unauthorized")

    access_token = authorization.split("Bearer ")[1]
    try:
        # Get CRM access token
        crm_access_token = get_crm_access_token()
        if not crm_access_token:
            raise HTTPException(status_code=500, detail="Failed to get CRM access token")

        # Set all entities as customizable
        if not set_all_entities_customizable(crm_access_token):
            logger.warning("Failed to set all entities as customizable.")

        # Fetch CRM entities
        entities = get_crm_entities(crm_access_token)
        if not entities:
            raise HTTPException(status_code=404, detail="No CRM entities found")

        # Save the entities to the database
        try:
            for entity in entities:
                stmt = text("""
                    INSERT INTO crm_entities (entity_name, is_active)
                    VALUES (:entity_name, :is_active)
                    ON CONFLICT (entity_name) DO UPDATE SET
                    is_active = EXCLUDED.is_active
                """)
                db.execute(stmt, {"entity_name": entity, "is_active": True})
            db.commit()
            logger.info("Successfully saved CRM entities to the database.")
        except Exception as e:
            db.rollback()
            logger.error(f"Error saving CRM entities to the database: {str(e)}")
            raise HTTPException(status_code=500, detail="Internal server error")

        return {"message": "CRM entities fetched and saved successfully", "entities": entities}
    except Exception as e:
        logger.error(f"Error fetching CRM entities: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error fetching CRM entities: {str(e)}")



# Pydantic model for the response
class CRMEntity(BaseModel):
    entity_name: str
    is_active: bool

# Endpoint to fetch CRM entities from the database
@app.get("/getfromdb-crm-entities", response_model=List[CRMEntity])
def get_crm_entities_from_db(db: Session = Depends(get_db)):
    try:
        # Query the database to fetch all CRM entities
        stmt = text("SELECT entity_name, is_active FROM crm_entities")
        result = db.execute(stmt)
        crm_entities = [{"entity_name": row[0], "is_active": row[1]} for row in result]
        return crm_entities
    except Exception as e:
        logger.error(f"Error fetching CRM entities from the database: {str(e)}")
        raise HTTPException(status_code=500, detail="Internal server error")




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

        # Parse the response to extract field names, required status, and upsert capability
        attributes_data = response.json().get("value", [])
        columns = [
            {
                "name": attribute["LogicalName"],
                "mandatory": attribute.get("RequiredLevel", {}).get("Value") == "ApplicationRequired",
                "upsert": attribute.get("IsValidForCreate", False) or attribute.get("IsValidForUpdate", False)
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

def extract_record_id_from_error(response):
    try:
        error_data = response.json()
        if "error" in error_data and "details" in error_data["error"]:
            for detail in error_data["error"]["details"]:
                if "id" in detail:
                    return detail["id"]
        raise ValueError("Could not extract record ID from error response.")
    except Exception as e:
        logger.error(f"Error extracting record ID from error response: {str(e)}")
        raise ValueError("Failed to parse the error response.")
    
def validate_and_convert_guid(value, entity_name=None):
    try:
        if isinstance(value, str):
            if len(value) == 36 and '-' in value:
                return str(uuid.UUID(value))
            elif len(value) == 32 and all(c in '0123456789abcdefABCDEF' for c in value):
                return str(uuid.UUID(value))
            elif entity_name:
                namespace = uuid.NAMESPACE_OID
                combined_value = f"{entity_name}_{value}"
                return str(uuid.uuid5(namespace, combined_value))
        elif isinstance(value, int) and entity_name:
            namespace = uuid.NAMESPACE_OID
            combined_value = f"{entity_name}_{value}"
            return str(uuid.uuid5(namespace, combined_value))
    except (ValueError, AttributeError, TypeError):
        return None
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
    
    elif field_type == 'firstname':
        # Show the last 2 characters and anonymize the rest
        return "***" + data[-2:] if len(data) > 2 else "***" + data

    elif field_type == 'fullname':
        # Show the last 2 characters and anonymize the rest
        return "***" + data[-2:] if len(data) > 2 else "***" + data
        
    elif field_type == 'email':
        # Show the first 3 characters of the local part and the domain
        local, domain = data.split('@') if '@' in data else (data, "")
        return local[:3] + "***@" + domain if domain else local[:3] + "***"
    
    elif field_type == 'phone':
        # Show the first 4 characters and anonymize the rest
        return data[:4] + "****" if len(data) > 4 else data + "****"
    
    elif field_type == 'phonenumber':
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
    logger.info(f"Using CRM entity type: {selected_crm_entity}")
    
    crm_data = {}
    failed_fields = []  # Track fields that fail validation or processing

    # Iterate over the matched fields (now a list of strings in the format 'impower_field-crm_field')
    for field_pair in matched_fields:
        # Split the string into 'impower_field' and 'crm_field'
        impower_field, crm_field = field_pair.split('-')
        crm_field = crm_field.replace(' *', '').strip()
        crm_field = crm_field.replace(' +', '').strip()

        logger.info(f"Processing source field: {impower_field}, CRM field: {crm_field}.")

        # Check if the impower_field exists in the entity data
        if impower_field in entity_data:
            field_value = entity_data[impower_field]
            
            # Handle null values first
            if field_value is None:
                crm_data[crm_field] = None
                continue
                
            # Handle array values
            if isinstance(field_value, (list, tuple)):
                crm_data[crm_field] = list(field_value)
                continue
                
            # Handle complex objects (assuming we need to add @odata.type)
            if isinstance(field_value, dict):
                # Add @odata.type annotation if not present
                if '@odata.type' not in field_value:
                    field_value['@odata.type'] = f"Microsoft.Dynamics.CRM.{crm_field}"
                crm_data[crm_field] = field_value
                continue

            # Convert to numeric if possible
            if isinstance(field_value, str) and field_value.replace('.', '', 1).isdigit():
                field_value = float(field_value) if '.' in field_value else int(field_value)

            # Handle special ID fields
            if crm_field.lower().endswith('id'):  # Corrected to check if crm_field ends with 'id'
                guid_value = validate_and_convert_guid(field_value, selected_crm_entity)
                if guid_value:
                    crm_data[crm_field] = guid_value  # Assign GUID directly
                else:
                    logger.warning(f"Invalid GUID value for '{crm_field}'. Skipping field.")
                    failed_fields.append(f"{impower_field}-{crm_field}")

            else:
                # Anonymize sensitive data based on field type
                if impower_field.lower() in ['name', 'lastname', 'firstname', 'fullname', 'email', 'phone', 'phonenumber']:
                    anonymized_value = anonymize_data(str(field_value), impower_field.lower())
                    crm_data[crm_field] = anonymized_value
                else:
                    if crm_field == 'name':
                        field_value = field_value[:100]  # Truncate name field if necessary
                    crm_data[crm_field] = preprocess_field(field_value)
    
    logger.info(f"CRM Data being sent: {crm_data}")

    try:
        # Step 1: Use the unique_identifier_field to implement Upsert
        unique_identifier_field = matched_fields[0].split('-')[1]  # First CRM field (e.g., accountid)
        unique_identifier_field = unique_identifier_field.replace(' *', '').strip()
        unique_identifier_field = unique_identifier_field.replace(' +', '').strip()
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
                    record_id = existing_records[0].get(f"{selected_crm_entity.lower()}id")  # e.g., "accountid"

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
            elif 'fullname' in field_name.lower():
                field_type = 'fullname'
            elif 'firstname' in field_name.lower():
                field_type = 'firstname'
            elif 'phone' in field_name.lower():
                field_type = 'phone'
            elif 'phonenumber' in field_name.lower():
                field_type = 'phonenumber'
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

            impower_field, crm_field = field_pair.split('-')

            if impower_field in record:
                value = record[impower_field]
                field_type = None

                # ✅ Ensure crm_field is a string before using .lower()
                if isinstance(crm_field, str):
                    if 'email' in crm_field.lower():
                        field_type = 'email'
                    elif 'name' in crm_field.lower():
                        field_type = 'name'
                    elif 'lastname' in crm_field.lower():
                        field_type = 'lastname'
                    elif 'fullname' in crm_field.lower():
                        field_type = 'fullname'
                    elif 'firstname' in crm_field.lower():
                        field_type = 'firstname'
                    elif 'phone' in crm_field.lower():
                        field_type = 'phone'
                    elif 'phonenumber' in crm_field.lower():
                        field_type = 'phonenumber'
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
    selected_impower_entity: str
    selected_crm_entity: str
    limit_records: Optional[int] = None  # Make sure this matches the case exactly


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
    selected_impower_entity = table_entity_name(request.selected_impower_entity)
    selected_crm_entity = request.selected_crm_entity

    # Fetch matched fields
    matched_fields_response = await get_matching_fields(selected_impower_entity, selected_crm_entity, authorization)
    matched_fields_list = matched_fields_response.get("matched_fields", [])
    if not matched_fields_list:
        raise HTTPException(status_code=400, detail="No matched fields found for the selected entities.")

    # Fetch data from staging database in chunks
    with SessionLocal() as db:
        entity_data = []
        for chunk in fetch_entity_data_in_chunks(db, selected_impower_entity, matched_fields_list):
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
        "entity_name": selected_impower_entity,
        "results": migration_result["results"],
        "excel_file_url": f"/download-excel/{os.path.basename(excel_file_path)}"
    }

@app.post("/migrate-entity", response_model=MigrateResponse)
async def migrate_entity(
    request: MigrateEntityRequest,
    background_tasks: BackgroundTasks,
    authorization: str = Header(None)
):
    selected_impower_entity = table_entity_name(request.selected_impower_entity)
    selected_crm_entity = request.selected_crm_entity
    limit_records = request.limit_records  # Get the limit from the request

    # Fetch matched fields
    matched_fields_response = await get_matching_fields(selected_impower_entity, selected_crm_entity, authorization)
    matched_fields_list = matched_fields_response.get("matched_fields", [])
    if not matched_fields_list:
        raise HTTPException(status_code=400, detail="No matched fields found for the selected entities.")

    # Fetch data from staging database
    with SessionLocal() as db:
        entity_data = fetch_entity_data_from_staging(db, selected_impower_entity, matched_fields_list)

    if not entity_data:
        raise HTTPException(status_code=400, detail="No records found in the staging database.")

    # Apply record limit if specified
    if limit_records and limit_records > 0:
        entity_data = entity_data[:limit_records]
        logger.info(f"Limiting migration to first {limit_records} records")

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
            failed_fields = crm_result.get("failed_fields", [])

            # Add the record to the export data
            data_to_export.append(record)

            # Log result
            results.append({
                "record_id": record.get("id"),
                "staging_status": "Success",
                "crm_status": "Success" if crm_result["success"] else "Failed",
                "action": crm_result.get("action", "none"),
                "error": crm_result.get("error", None),
                "failed_fields": failed_fields
            })

        except Exception as e:
            error_count += 1
            logger.error(f"An error occurred while migrating record {record.get('id')}: {str(e)}", exc_info=True)
            results.append({
                "record_id": record.get("id"),
                "staging_status": "Failed",
                "crm_status": "Failed",
                "error": str(e),
                "failed_fields": []
            })

    # Export data to Excel (only the processed records)
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
        "entity_name": selected_impower_entity,
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


class MigrationSequence(BaseModel):
    order_name: str
    entity_names: List[str]

@app.post("/create-migration-sequence/")
async def create_or_update_migration_sequence(sequence: MigrationSequence, authorization: str = Header(None)):
    db = SessionLocal()
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Unauthorized")
    
    try:
        # Debug logging
        print(f"Processing sequence: {sequence.order_name}")
        
        # Create table if not exists
        db.execute(text("""
            CREATE TABLE IF NOT EXISTS migration_sequences (
                order_name TEXT PRIMARY KEY,
                entity_names JSONB NOT NULL
            )
        """))
        db.commit()

        # Convert to JSON and trim whitespace
        entity_names_json = json.dumps([e.strip() for e in sequence.entity_names])
        sequence_name = sequence.order_name.strip()

        # Alternative UPSERT implementation that works across databases
        try:
            # First try to insert
            db.execute(
                text("""
                    INSERT INTO migration_sequences (order_name, entity_names)
                    VALUES (:order_name, :entity_names)
                """),
                {
                    "order_name": sequence_name,
                    "entity_names": entity_names_json
                }
            )
            message = "Sequence created successfully"
        except Exception as insert_error:
            # If insert fails, update instead
            db.rollback()
            result = db.execute(
                text("""
                    UPDATE migration_sequences 
                    SET entity_names = :entity_names
                    WHERE order_name = :order_name
                """),
                {
                    "order_name": sequence_name,
                    "entity_names": entity_names_json
                }
            )
            if result.rowcount == 0:
                raise HTTPException(status_code=400, detail="Sequence not found for update")
            message = "Sequence updated successfully"

        db.commit()
        return {"success": True, "message": message}

    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        print(f"Database error: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")
    finally:
        db.close()

@app.get("/get-migration-sequences/")
async def get_migration_sequences(authorization: str = Header(None)):
    db = SessionLocal()
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Unauthorized")
    
    try:
        # Create table if not exists (in case this is first call)
        db.execute(text("""
            CREATE TABLE IF NOT EXISTS migration_sequences (
                order_name TEXT PRIMARY KEY,
                entity_names JSONB
            )
        """))
        db.commit()

        # Get all sequences
        result = db.execute(
            text("SELECT order_name, entity_names FROM migration_sequences")
        ).fetchall()

        return [{"order_name": row[0], "entity_names": row[1]} for row in result]

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        db.close()

@app.get("/get-matching-entities/")
async def get_matching_entities(authorization: str = Header(None)):
    db = SessionLocal()
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Unauthorized")
    
    try:
        result = db.execute(
            text("SELECT entity_pair FROM matching_table")
        ).fetchall()

        return [row[0] for row in result]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        db.close()


class DeleteSequenceRequest(BaseModel):
    order_name: str

@app.delete("/delete-migration-sequence/")
async def delete_migration_sequence(
    request: DeleteSequenceRequest,  # Changed from MigrationSequence to DeleteSequenceRequest
    authorization: str = Header(None)
):
    db = SessionLocal()
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Unauthorized")
    
    try:
        # Verify sequence exists
        existing = db.execute(
            text("SELECT 1 FROM migration_sequences WHERE order_name = :order_name"),
            {"order_name": request.order_name.strip()}
        ).fetchone()

        if not existing:
            raise HTTPException(status_code=404, detail="Sequence not found")

        # Delete the sequence
        result = db.execute(
            text("DELETE FROM migration_sequences WHERE order_name = :order_name"),
            {"order_name": request.order_name.strip()}
        )
        
        db.commit()
        
        if result.rowcount == 0:
            raise HTTPException(status_code=404, detail="Sequence not found")
            
        return {"success": True, "message": "Sequence deleted successfully"}

    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        db.close()


@app.get("/get-migration-sequences/{sequence_name}")
async def get_migration_sequence(sequence_name: str, authorization: str = Header(None)):
    db = SessionLocal()
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Unauthorized")
    
    try:
        result = db.execute(
            text("SELECT order_name, entity_names FROM migration_sequences WHERE order_name = :order_name"),
            {"order_name": sequence_name}
        ).fetchone()

        if not result:
            raise HTTPException(status_code=404, detail="Sequence not found")

        return {"order_name": result[0], "entity_names": result[1]}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        db.close()

# FastAPI route to serve the index.html page
@app.get("/", response_class=HTMLResponse)
async def get_index(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})

#@app.on_event("startup")
#def on_startup():
#    create_impower_entities_with_status_table()

# Start FastAPI app
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
