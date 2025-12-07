# server.py
import os
import logging
import json
from typing import Optional, Any, Dict
from datetime import datetime
from motor.motor_asyncio import AsyncIOMotorClient
from dotenv import load_dotenv
from bson import ObjectId
import re
load_dotenv() 

from mcp.server.fastmcp import FastMCP

HOST = os.getenv("MCP_HOST", "127.0.0.1")
PORT = int(os.getenv("MCP_PORT", "8000"))
TRANSPORT = os.getenv("MCP_TRANSPORT", "streamable-http")

MONGODB_URL = os.getenv("MONGO_URI")
DATABASE_NAME = os.getenv("MONGO_DB")
COLLECTION_NAME = os.getenv("MONGO_COLLECTION")

logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
logger = logging.getLogger("flightops.mcp.server")

mcp = FastMCP("FlightOps MCP Server")

_mongo_client: Optional[AsyncIOMotorClient] = None
_db = None
_col = None

async def get_mongodb_client():
    """Initialize and return the global Motor client, DB and collection."""
    global _mongo_client, _db, _col
    if _mongo_client is None:
        logger.info("Connecting to MongoDB: %s", MONGODB_URL)
        _mongo_client = AsyncIOMotorClient(MONGODB_URL)
        _db = _mongo_client[DATABASE_NAME]
        _col = _db[COLLECTION_NAME]
    return _mongo_client, _db, _col

# def normalize_flight_number(flight_number: Any) -> Optional[str]:  # ✅ Changed return type to str
#     """
#     Convert flight_number to string for MongoDB query.
#     MongoDB stores flightNumber as STRING, not int.
#     """
#     if flight_number is None or flight_number == "":
#         return None
    
#     # Convert to string and strip whitespace
#     try:
#         return str(flight_number).strip()
#     except Exception:
#         logger.warning(f"Could not normalize flight_number: {flight_number}")
#         return None
def normalize_flight_number(flight_number: Any) -> Optional[int]:
    """Convert flight_number to int. MongoDB stores it as int."""
    if flight_number is None or flight_number == "":
        return None
    if isinstance(flight_number, int):
        return flight_number
    try:
        return int(str(flight_number).strip())
    except (ValueError, TypeError):
        logger.warning(f"Could not normalize flight_number: {flight_number}")
        return None

def validate_date(date_str: str) -> Optional[str]:
    """
    Validate date_of_origin string. Accepts common formats.
    Returns normalized ISO date string YYYY-MM-DD if valid, else None.
    """
    if not date_str or date_str == "":
        return None
    
    # Handle common date formats
    formats = [
        "%Y-%m-%d",      # 2024-06-23
        "%d-%m-%Y",      # 23-06-2024
        "%Y/%m/%d",      # 2024/06/23
        "%d/%m/%Y",      # 23/06/2024
        "%B %d, %Y",     # June 23, 2024
        "%d %B %Y",      # 23 June 2024
        "%b %d, %Y",     # Jun 23, 2024
        "%d %b %Y"       # 23 Jun 2024
    ]
    
    for fmt in formats:
        try:
            dt = datetime.strptime(date_str, fmt)
            return dt.strftime("%Y-%m-%d")
        except ValueError:
            continue
    
    logger.warning(f"Could not parse date: {date_str}")
    return None

def make_query(carrier: str, flight_number: Optional[int], date_of_origin: str, startStation: Optional[str], endStation: Optional[str]) -> Dict:  # ✅ Changed type hint
    """
    Build MongoDB query matching the actual database schema.
    """
    query = {}
    
    if carrier:
        query["flightLegState.carrier"] = carrier
    
    # ✅ flight_number is now stored as STRING in MongoDB
    if flight_number is not None:
        query["flightLegState.flightNumber"] = flight_number  # Keep as string
    
    if date_of_origin:
        query["flightLegState.dateOfOrigin"] = date_of_origin
    if startStation:
        query["flightLegState.startStation"] = startStation
    if endStation:
        query["flightLegState.endStation"] = endStation
    
    logger.info(f"Built query: {json.dumps(query)}")
    return query

def response_ok(data: Any) -> str:
    """Return JSON string for successful response."""
    return json.dumps({"ok": True, "data": data}, indent=2, default=str)

def response_error(msg: str, code: int = 400) -> str:
    """Return JSON string for error response."""
    return json.dumps({"ok": False, "error": {"message": msg, "code": code}}, indent=2)

async def _fetch_one_async(query: dict, projection: dict,multiple:bool=False) -> str:          #  Point of concern
    """
    Consistent async DB fetch and error handling.
    Returns JSON string response.
    """
    try:
        _, _, col = await get_mongodb_client()
        logger.info(f"Executing query: {json.dumps(query)}")
        
        if multiple:
            cursor =col.find(query,projection)
            results=[]
            async for doc in cursor:
                if "_id" in doc:
                    doc.pop("_id")
                if "_class" in doc:
                    doc.pop("_class")
                results.append(doc)
            if not results:
                logger.warning(f"No documents found for query:{json.dumps(query)}")
                return response_error("No matching documents found", code=404)
            logger.info(f"Found {len(results)} documents")
            return response_ok({
                "document":results,
                "count":len(results),
                "query":query
            })
        else:
            result=await col.find_one(query,projection)
        
        if not result:
            logger.warning(f"No document found for query: {json.dumps(query)}")
            return response_error("No matching document found.", code=404)
        
        # Remove _id and _class to keep output clean
        if "_id" in result:
            result.pop("_id")
        if "_class" in result:
            result.pop("_class")
        
        logger.info(f"Query successful")
        return response_ok(result)
    except Exception as exc:
        logger.exception("DB query failed")
        return response_error(f"DB query failed: {str(exc)}", code=500)

async def _find_matching_doc_meta(query:dict,limit: int = 50)->dict:
    """
    Return a lightweight list of matching document (meta only) sorted by scheduledStartTime.
    Deduplicates based on carrier, flightNumber, dateOfOrigin, startStation, endStation.
    Returns: {"count": <int>, "documents": [{doc_id, startStation, endStation, scheduledStartTime, seqNumber, flightStatus},...]}
    """
    try:
        _, _, col=await get_mongodb_client()
        
        # ✅ Add debug logging
        logger.info(f"🔍 _find_matching_doc_meta query: {json.dumps(query, indent=2)}")
        
        total_matches = await col.count_documents(query)
        logger.info(f"📊 MongoDB matched {total_matches} documents before deduplication")
        
        count=await col.count_documents(query)

        proj={
            "_id":1,
            "flightLegState.carrier": 1,
            "flightLegState.flightNumber": 1,
            "flightLegState.dateOfOrigin": 1,
            "flightLegState.startStation":1,
            "flightLegState.endStation":1,
            "flightLegState.scheduledStartTime":1,
            "flightLegState.seqNumber":1,
            "flightLegState.flightStatus":1,
        }
        cursor = col.find(query,proj).sort("flightLegState.scheduledStartTime",1).limit(limit * 2)  # Fetch more to handle duplicates
        
        docs=[]
        seen = set()  # Track unique flights
        
        async for d in cursor:
            fl = d.get("flightLegState",{}) or {}
            
            # Create unique key for deduplication
            unique_key = (
                fl.get("carrier"),
                fl.get("flightNumber"),
                fl.get("dateOfOrigin"),
                fl.get("startStation"),
                fl.get("endStation")
            )
            
            # Skip if we've seen this exact flight
            if unique_key in seen:
                logger.debug(f"🔄 Skipping duplicate: {unique_key}, seqNumber={fl.get('seqNumber')}")
                continue
            
            seen.add(unique_key)
            docs.append({
                "doc_id": str(d["_id"]),
                "startStation": fl.get("startStation"),
                "endStation": fl.get("endStation"),
                "scheduledStartTime": fl.get("scheduledStartTime"),
                "seqNumber": fl.get("seqNumber"),
                "flightStatus": fl.get("flightStatus"),
            })
            
            # Stop if we have enough unique docs
            if len(docs) >= limit:
                break
        
        logger.info(f"📊 Found {count} total, returning {len(docs)} unique documents")
        return {"count":len(docs),"documents":docs}  # Return count of unique docs
        
    except Exception as e:
        logger.exception("Error in _find_matching_doc_meta")
        return {"count":0, "documents":[],"error":str(e)}

async def _get_document_by_id(doc_id: str, projection: dict = None) -> Optional[dict]:
    """
    Fetch document by _id. Handles both ObjectId and string _id types.
    """
    try:
        _, _, col = await get_mongodb_client()
        proj = projection or {}
        
        logger.info(f"🔍 _get_document_by_id called with doc_id={doc_id}, projection={proj}")
        
        # Try ObjectId first
        try:
            oid = ObjectId(doc_id)
            logger.debug(f"🔍 Trying ObjectId lookup: {oid}")
            result = await col.find_one({"_id": oid}, proj)
            if result:
                logger.info(f"✅ Found with ObjectId! Keys: {list(result.keys())}")
                result.pop("_id", None)
                result.pop("_class", None)
                return result
            else:
                logger.warning(f"❌ ObjectId query returned None")
        except Exception as e:
            logger.debug(f"ObjectId conversion failed: {e}")
        
        # Fallback to string _id
        logger.debug(f"🔍 Trying string _id lookup: {doc_id}")
        result = await col.find_one({"_id": doc_id}, proj)
        if result:
            logger.info(f"✅ Found with string _id! Keys: {list(result.keys())}")
            result.pop("_id", None)
            result.pop("_class", None)
            return result
        else:
            logger.warning(f"❌ String _id query also returned None")
        
        logger.error(f"❌ Document NOT FOUND for _id: {doc_id}")
        return None
        
    except Exception as e:
        logger.exception(f"💥 Exception in _get_document_by_id for {doc_id}")
        return None

        

# --- MCP Tools ---
@mcp.tool()
async def get_flight_by_id(doc_id:str, projection:str = "")->str:
    """
    Exposed tool to fetch a single document by its ObjectId string.
    projection: optional JOSN string for projection (e.g.'{"flightLegState.carrier":1,"_id":0}')
    """
    if not doc_id:
        return response_error("doc_id is requied",400)
    proj = None
    if projection:
        try:
            proj = json.loads(projection.replace("'",'"'))
        except Exception as e:
            return response_error(f"Invalid projection JSON:{e}",400)
    doc= await _get_document_by_id(doc_id,proj)
    if not doc:
        return response_error("Document not found",404)
    return response_ok(doc)
@mcp.tool()
async def health_check() -> str:
    """
    Simple health check for orchestrators and clients.
    Attempts a cheap DB ping.
    """
    try:
        _, _, col = await get_mongodb_client()
        doc = await col.find_one({}, {"_id": 1})
        return response_ok({"status": "ok", "db_connected": doc is not None})
    except Exception as e:
        logger.exception("Health check DB ping failed")
        return response_error("DB unreachable", code=503)
@mcp.tool()
async def casual_query(answer: str="")-> str:
    "The user just asked a general query answere it on your own behalf "
    return response_ok(answer)

@mcp.tool()
async def get_flight_basic_info(carrier: str = "", flight_number: str = "", date_of_origin: str = "",startStation: Optional[str] = "",endStation: Optional[str] = "") -> str:
    """
    Fetch basic flight information including carrier, flight number, date, stations, times, and status.
    
    Args:
        carrier: Airline carrier code (e.g., "6E", "AI")
        flight_number: Flight number as string (e.g., "215")
        date_of_origin: Date in YYYY-MM-DD format (e.g., "2024-06-23")
        startStation: Optional start station code (e.g., "DEL")
        endStation: Optional end station code (e.g., "BOM")
    """
    logger.info(f"get_flight_basic_info: carrier={carrier}, flight_number={flight_number}, date={date_of_origin}, startStation={startStation}, endStation={endStation}")
    
    # Normalize inputs
    fn = normalize_flight_number(flight_number) if flight_number else None
    dob = validate_date(date_of_origin) if date_of_origin else None
    
    if date_of_origin and not dob:
        return response_error("Invalid date_of_origin format. Expected YYYY-MM-DD or common date formats", 400)
    
    query = make_query(carrier, fn, dob, startStation, endStation)
    
    # Project basic flight information
    projection = {
        "flightLegState.carrier": 1,
        "flightLegState.flightNumber": 1,
        "flightLegState.suffix": 1,
        "flightLegState.dateOfOrigin": 1,
        "flightLegState.seqNumber": 1,
        "flightLegState.startStation": 1,
        "flightLegState.endStation": 1,
        "flightLegState.startStationICAO": 1,
        "flightLegState.endStationICAO": 1,
        "flightLegState.scheduledStartTime": 1,
        "flightLegState.scheduledEndTime": 1,
        "flightLegState.flightStatus": 1,
        "flightLegState.operationalStatus": 1,
        "flightLegState.flightType": 1,
        "flightLegState.blockTimeSch": 1,
        "flightLegState.blockTimeActual": 1,
        "flightLegState.flightHoursActual": 1,
        "flightLegState.isOTPFlight": 1,
        "flightLegState.isOTPAchieved": 1,
        "flightLegState.isOTPConsidered": 1,
        "flightLegState.isOTTFlight": 1,
        "flightLegState.isOTTAchievedFlight": 1,
        "flightLegState.turnTimeFlightBeforeActual": 1,
        "flightLegState.turnTimeFlightBeforeSch": 1
    }
    
        # return await _fetch_one_async(query, projection)
    meta= await _find_matching_doc_meta(query,limit=50)
    if meta.get("error"):
        return response_error(f"DB error: {meta['error']}",500)
    count = meta.get("count",0)
    if count == 0:
        return response_error("No matching document found.",404)
    elif count==1:
        doc_id=meta["documents"][0]["doc_id"]
        full_doc = await _get_document_by_id(doc_id, projection)  # ✅ Added projection
        if not full_doc:
            return response_error("Document not found",404)
        return response_ok(full_doc)
    else:
        return response_ok({
            "needs_route_selection":True,
            "count":count,
            "matches":meta["documents"],
            "original_query":query
        })

@mcp.tool()
async def get_operation_times(carrier: str = "", flight_number: str = "", date_of_origin: str = "", startStation: Optional[str] = None, endStation: Optional[str] = None) -> str:
    """
    Return estimated and actual operation times for a flight including takeoff, landing, block times,StartTimeOffset, EndTimeOffset.
    
    Args:
        carrier: Airline carrier code
        flight_number: Flight number as string
        date_of_origin: Date in YYYY-MM-DD format
    """
    logger.info(f"get_operation_times: carrier={carrier}, flight_number={flight_number}, date={date_of_origin}")
    
    fn = normalize_flight_number(flight_number) if flight_number else None
    dob = validate_date(date_of_origin) if date_of_origin else None
    
    if date_of_origin and not dob:
        return response_error("Invalid date format.", 400)
    
    query = make_query(carrier, fn, dob, startStation, endStation)
    projection = {
    
        "flightLegState.carrier": 1,
        "flightLegState.flightNumber": 1,
        "flightLegState.dateOfOrigin": 1,
        "flightLegState.startStation": 1,
        "flightLegState.endStation": 1,
        "flightLegState.scheduledStartTime": 1,
        "flightLegState.scheduledEndTime": 1,
        "flightLegState.startTimeOffset": 1,
        "flightLegState.endTimeOffset": 1,
        "flightLegState.operation.estimatedTimes": 1,
        "flightLegState.operation.actualTimes": 1,
        "flightLegState.taxiOutTime": 1,
        "flightLegState.taxiInTime": 1,
        "flightLegState.blockTimeSch": 1,
        "flightLegState.blockTimeActual": 1,
        "flightLegState.flightHoursActual": 1,
        
    }
        
        # return await _fetch_one_async(query, projection)
    meta= await _find_matching_doc_meta(query,limit=50)
    if meta.get("error"):
        return response_error(f"DB error: {meta['error']}",500)

    count = meta.get("count",0)
    if count == 0:
        return response_error("No matching document found.",404)
    elif count==1:
        doc_id=meta["documents"][0]["doc_id"]
        full_doc = await _get_document_by_id(doc_id, projection)  # ✅ Added projection
        if not full_doc:
            return response_error("Document not found",404)
        return response_ok(full_doc)
    else:
        return response_ok({
            "needs_route_selection":True,
            "count":count,
            "matches":meta["documents"],
            "original_query":query
        })

@mcp.tool()
async def get_equipment_info(carrier: str = "", flight_number: str = "", date_of_origin: str = "", startStation: Optional[str] = None, endStation: Optional[str] = None) -> str:
    """
    Get aircraft equipment details including aircraft type, registration (tail number), and configuration.
    
    Args:
        carrier: Airline carrier code
        flight_number: Flight number as string
        date_of_origin: Date in YYYY-MM-DD format
    """
    logger.info(f"get_equipment_info: carrier={carrier}, flight_number={flight_number}, date={date_of_origin}")
    
    fn = normalize_flight_number(flight_number) if flight_number else None
    dob = validate_date(date_of_origin) if date_of_origin else None
    
    query = make_query(carrier, fn, dob, startStation, endStation)
    
    projection = {
        
        "flightLegState.carrier": 1,
        "flightLegState.flightNumber": 1,
        "flightLegState.dateOfOrigin": 1,
        "flightLegState.equipment.plannedAircraftType": 1,
        "flightLegState.equipment.aircraft": 1,
        "flightLegState.equipment.aircraftConfiguration": 1,
        "flightLegState.equipment.aircraftRegistration": 1,
        "flightLegState.equipment.assignedAircraftTypeIATA": 1,
        "flightLegState.equipment.assignedAircraftTypeICAO": 1,
        "flightLegState.equipment.assignedAircraftTypeIndigo": 1,
        "flightLegState.equipment.assignedAircraftConfiguration": 1,
        "flightLegState.equipment.tailLock": 1,
        "flightLegState.equipment.onwardFlight": 1,
        "flightLegState.equipment.actualOnwardFlight": 1
    }
    
    meta = await _find_matching_doc_meta(query, limit=50)
    if meta.get("error"):
        return response_error(f"DB error: {meta['error']}", 500)
    
    count = meta.get("count", 0)
    if count == 0:
        return response_error("No matching document found.", 404)
    elif count == 1:
        doc_id = meta["documents"][0]["doc_id"]  # ✅ Already correct
        full_doc = await _get_document_by_id(doc_id, projection)
        if not full_doc:
            return response_error("Document not found", 404)
        return response_ok(full_doc)
    else:
        return response_ok({
            "needs_route_selection": True,
            "count": count,
            "matches": meta["documents"],
            "original_query": query
        })

@mcp.tool()
async def get_delay_summary(carrier: str = "", flight_number: str = "", date_of_origin: str = "",startStation: Optional[str] = None, endStation: Optional[str] = None) -> str:
    """
    Summarize delay reasons, durations, and total delay time for a specific flight.
    
    Args:
        carrier: Airline carrier code
        flight_number: Flight number as string
        date_of_origin: Date in YYYY-MM-DD format
    """
    logger.info(f"get_delay_summary: carrier={carrier}, flight_number={flight_number}, date={date_of_origin}")
    
    fn = normalize_flight_number(flight_number) if flight_number else None
    dob = validate_date(date_of_origin) if date_of_origin else None
    
    query = make_query(carrier, fn, dob, startStation, endStation)
    projection = {
   
        "flightLegState.carrier": 1,
        "flightLegState.flightNumber": 1,
        "flightLegState.dateOfOrigin": 1,
        "flightLegState.startStation": 1,
        "flightLegState.endStation": 1,
        "flightLegState.scheduledStartTime": 1,
        "flightLegState.operation.actualTimes.offBlock": 1,
        "flightLegState.delays.total": 1
    }
    
    meta = await _find_matching_doc_meta(query, limit=50)
    if meta.get("error"):
        return response_error(f"DB error: {meta['error']}", 500)
    
    count = meta.get("count", 0)
    if count == 0:
        # ✅ Better error message showing what was searched
        return response_error(
            f"No flight found for carrier={carrier}, flight_number={flight_number}, "
            f"date={date_of_origin}, route={startStation}→{endStation}. "
            f"Please verify the flight details or try without specifying the route.",
            404
        )
    elif count == 1:
        doc_id = meta["documents"][0]["doc_id"]  # ✅ Changed from meta["document"]
        full_doc = await _get_document_by_id(doc_id, projection)
        if not full_doc:
            return response_error("Document not found", 404)
        return response_ok(full_doc)
    else:
        return response_ok({
            "needs_route_selection": True,
            "count": count,
            "matches": meta["documents"],
            "original_query": query
        })

@mcp.tool()
async def get_fuel_summary(carrier: str = "", flight_number: str = "", date_of_origin: str = "", startStation: Optional[str] = None, endStation: Optional[str] = None) -> str:
    """
    Retrieve fuel summary including planned vs actual fuel for takeoff, landing, and total consumption.
    
    Args:
        carrier: Airline carrier code
        flight_number: Flight number as string
        date_of_origin: Date in YYYY-MM-DD format
    """
    logger.info(f"get_fuel_summary: carrier={carrier}, flight_number={flight_number}, date={date_of_origin}")
    
    fn = normalize_flight_number(flight_number) if flight_number else None
    dob = validate_date(date_of_origin) if date_of_origin else None
    
    query = make_query(carrier, fn, dob, startStation, endStation)
    
    projection = {
       
        "flightLegState.carrier": 1,
        "flightLegState.flightNumber": 1,
        "flightLegState.dateOfOrigin": 1,
        "flightLegState.startStation": 1,
        "flightLegState.endStation": 1,
        "flightLegState.operation.fuel": 1,
        "flightLegState.operation.flightPlan.offBlockFuel": 1,
        "flightLegState.operation.flightPlan.takeoffFuel": 1,
        "flightLegState.operation.flightPlan.landingFuel": 1,
        "flightLegState.operation.flightPlan.holdFuel": 1
    }
    
    meta = await _find_matching_doc_meta(query, limit=50)
    if meta.get("error"):
        return response_error(f"DB error: {meta['error']}", 500)
    
    count = meta.get("count", 0)
    if count == 0:
        return response_error("No matching document found.", 404)
    elif count == 1:
        doc_id = meta["documents"][0]["doc_id"]  # ✅ Already correct
        full_doc = await _get_document_by_id(doc_id, projection)
        if not full_doc:
            return response_error("Document not found", 404)
        return response_ok(full_doc)
    else:
        return response_ok({
            "needs_route_selection": True,
            "count": count,
            "matches": meta["documents"],
            "original_query": query
        })

@mcp.tool()
async def get_passenger_info(carrier: str = "", flight_number: str = "", date_of_origin: str = "", startStation: Optional[str] = None, endStation: Optional[str] = None) -> str:
    """
    Get passenger count and connection information for the flight. 
    Here pax is an object and passengerCount is an array object.
    
    Args:
        carrier: Airline carrier code
        flight_number: Flight number as string
        date_of_origin: Date in YYYY-MM-DD format
    """
    logger.info(f"get_passenger_info: carrier={carrier}, flight_number={flight_number}, date={date_of_origin}")
    
    fn = normalize_flight_number(flight_number) if flight_number else None
    dob = validate_date(date_of_origin) if date_of_origin else None
    
    query = make_query(carrier, fn, dob, startStation, endStation)
    
    projection = {
        
        "flightLegState.carrier": 1,
        "flightLegState.flightNumber": 1,
        "flightLegState.dateOfOrigin": 1,
        # "flightLegState.pax": 1,
        "flightLegState.pax.passengerCount.count": 1,
    }
    
    meta = await _find_matching_doc_meta(query, limit=50)
    if meta.get("error"):
        return response_error(f"DB error: {meta['error']}", 500)
    
    count = meta.get("count", 0)
    if count == 0:
        return response_error("No matching document found.", 404)
    elif count == 1:
        doc_id = meta["documents"][0]["doc_id"]  # ✅ Already correct
        full_doc = await _get_document_by_id(doc_id, projection)
        if not full_doc:
            return response_error("Document not found", 404)
        return response_ok(full_doc)
    else:
        return response_ok({
            "needs_route_selection": True,
            "count": count,
            "matches": meta["documents"],
            "original_query": query
        })

@mcp.tool()
async def get_crew_info(carrier: str = "", flight_number: str = "", date_of_origin: str = "", startStation: Optional[str] = None, endStation: Optional[str] = None) -> str:
    """
    Get crew connections and details for the flight.
    
    Args:
        carrier: Airline carrier code
        flight_number: Flight number as string
        date_of_origin: Date in YYYY-MM-DD format
    """
    logger.info(f"get_crew_info: carrier={carrier}, flight_number={flight_number}, date={date_of_origin}")
    
    fn = normalize_flight_number(flight_number) if flight_number else None
    dob = validate_date(date_of_origin) if date_of_origin else None
    
    query = make_query(carrier, fn, dob, startStation, endStation)
    
    projection = {
        
        "flightLegState.carrier": 1,
        "flightLegState.flightNumber": 1,
        "flightLegState.dateOfOrigin": 1,
        "flightLegState.crewConnections": 1
    }
    
    meta = await _find_matching_doc_meta(query, limit=50)
    if meta.get("error"):
        return response_error(f"DB error: {meta['error']}", 500)
    
    count = meta.get("count", 0)
    if count == 0:
        return response_error("No matching document found.", 404)
    elif count == 1:
        doc_id = meta["documents"][0]["doc_id"]  # ✅ Already correct
        full_doc = await _get_document_by_id(doc_id, projection)
        if not full_doc:
            return response_error("Document not found", 404)
        return response_ok(full_doc)
    else:
        return response_ok({
            "needs_route_selection": True,
            "count": count,
            "matches": meta["documents"],
            "original_query": query
        })

@mcp.tool()
async def raw_mongodb_query(query_json: str, projection: str = "", limit: int = 10) -> str:
    """
    Execute a raw MongoDB query (stringified JSON) with optional projection.

    Supports intelligent LLM-decided projections to reduce payload size based on query intent.

    Args:
        query_json: The MongoDB query (as stringified JSON).
        projection: Optional projection (as stringified JSON) for selecting fields.
        limit: Max number of documents to return (default 10, capped at 50).
    """

    def _safe_json_loads(text: str) -> dict:
        """Safely parse JSON, handling single quotes and formatting errors."""
        try:
            return json.loads(text)
        except json.JSONDecodeError:
            try:
                fixed = text.replace("'", '"')
                return json.loads(fixed)
            except Exception as e:
                raise ValueError(f"Invalid JSON: {e}")

    try:
        _, _, col = await get_mongodb_client()

        # --- Parse Query ---
        try:
            query = _safe_json_loads(query_json)
        except ValueError as e:
            return response_error(f"❌ Invalid query_json: {str(e)}", 400)

        # --- Parse Projection (optional) ---
        projection_dict = None
        if projection:
            try:
                projection_dict = _safe_json_loads(projection)
            except ValueError as e:
                return response_error(f"❌ Invalid projection JSON: {str(e)}", 400)

        # --- Validate types ---
        if not isinstance(query, dict):
            return response_error("❌ query_json must be a JSON object.", 400)
        if projection_dict and not isinstance(projection_dict, dict):
            return response_error("❌ projection must be a JSON object.", 400)

        # --- Safety guard ---
        forbidden_ops = ["$where", "$out", "$merge", "$accumulator", "$function"]
        for key in query.keys():
            if key in forbidden_ops or key.startswith("$"):
                return response_error(f"❌ Operator '{key}' is not allowed.", 400)

        limit = min(max(1, int(limit)), 50)

        # --- Fallback projection ---
        # If the LLM forgets to include projection, return a minimal safe set.
        if not projection_dict:
            projection_dict = {
                "_id": 0,
                "flightLegState.carrier": 1,
                "flightLegState.flightNumber": 1,
                "flightLegState.dateOfOrigin": 1
            }
            
        logger.info(f"Executing MongoDB query: {query} | projection={projection_dict} | limit={limit}")

        # --- Run query ---
        cursor = col.find(query, projection_dict).sort("flightLegState.dateOfOrigin", -1).limit(limit)
        docs = []
        async for doc in cursor:
            doc.pop("_id", None)
            doc.pop("_class", None)
            docs.append(doc)

        if not docs:
            return response_error("No documents found for the given query.", 404)

        return response_ok({
            "count": len(docs),
            "query": query,
            "projection": projection_dict,
            "documents": docs
        })

    except Exception as exc:
        logger.exception("❌ raw_mongodb_query failed")
        return response_error(f"Raw MongoDB query failed: {str(exc)}", 500)


@mcp.tool()
async def run_aggregated_query(
    query_type: str = "",
    carrier: str = "",
    field: str = "",
    start_date: str = "",
    end_date: str = "",
    filter_json: str = ""
) -> str:
    """
    Run statistical or comparative MongoDB aggregation queries.
 
    Args:
        query_type: "average", "sum", "min", "max", "count" (also accepts "avg", "total", "mean")
        carrier: Optional carrier filter.
        field: Field to aggregate, e.g. "flightLegState.pax.passengerCount.count".
        start_date: Optional start date (YYYY-MM-DD).
        end_date: Optional end date (YYYY-MM-DD).
        filter_json: Optional filter query (as JSON string).
    """
 
    _, _, col = await get_mongodb_client()
 
    # ✅ Normalize query_type aliases
    query_type_map = {
        "avg": "average",
        "mean": "average",
        "total": "sum",
        "cnt": "count",
        "minimum": "min",
        "maximum": "max"
    }
    
    query_type_normalized = query_type_map.get(query_type.lower(), query_type.lower())
    
    # ✅ Check if field is delays.total (cannot aggregate string fields)
    if "delays.total" in field and query_type_normalized in ["average", "sum", "min", "max"]:
        return response_error(
            f"Cannot compute {query_type} on 'delays.total' (it's a string field). "
            f"Use 'get_total_delay_aggregated' tool instead for delay calculations.",
            400
        )
 
    match_stage = {}
 
    # --- Optional filters ---
    if filter_json:
        try:
            parsed_filter = json.loads(filter_json.replace("'", '"'))
            
            # Check if this is a delay query without time filter
            has_delay_filter = any("delays.total" in k for k in parsed_filter.keys())
            has_time_filter = any("scheduledStartTime" in k or "dateOfOrigin" in k for k in parsed_filter.keys())
            
            if has_delay_filter and not has_time_filter and not start_date and not end_date:
                logger.warning("⚠️ Delay query without time/date filter - may count all historical data!")
                # Optionally add a default time filter (last 7 days)
                # parsed_filter["flightLegState.dateOfOrigin"] = {"$gte": "2025-01-22"}
            
            # Keep original value for flightNumber
            normalized_filter = {}
            for key, value in parsed_filter.items():
                if "flightNumber" in key:
                    # ✅ REMOVED: Don't convert to int anymore - keep as string
                    normalized_filter[key] = value  # Keep original value
                elif "delays.total" in key:
                    if isinstance(value, dict):
                        new_conditions = {}
                        for op, val in value.items():
                            if op == "$gt" and val == 0:
                                normalized_filter[key] = {"$nin": ["PT0H0M", "00:00", "", None]}
                                logger.info("🔧 Converted delay $gt:0 to string exclusion filter")
                                break
                            elif op == "$eq" and val == 0:
                                normalized_filter[key] = {"$in": ["PT0H0M", "00:00"]}
                                break
                            else:
                                new_conditions[op] = val
                        if new_conditions and key not in normalized_filter:
                            normalized_filter[key] = new_conditions
                    else:
                        normalized_filter[key] = value
                
                else:
                    normalized_filter[key] = value
            
            match_stage.update(normalized_filter)
            logger.info(f"🔧 Normalized filter: {json.dumps(match_stage, indent=2)}")
            
        except Exception as e:
            return response_error(f"Invalid filter_json: {e}", 400)
 
    if carrier:
        match_stage["flightLegState.carrier"] = carrier
    if start_date and end_date:
        match_stage["flightLegState.dateOfOrigin"] = {"$gte": start_date, "$lte": end_date}
 
    agg_map = {
        "average": {"$avg": f"${field}"},
        "sum": {"$sum": f"${field}"},
        "min": {"$min": f"${field}"},
        "max": {"$max": f"${field}"},
        "count": {"$sum": 1},
    }
 
    if query_type_normalized not in agg_map:
        return response_error(
            f"Unsupported query_type '{query_type}'. Use: average, sum, min, max, count",
            400
        )
 
    pipeline = [{"$match": match_stage}, {"$group": {"_id": None, "value": agg_map[query_type_normalized]}}]
 
    try:
        logger.info(f"Running aggregation pipeline: {pipeline}")
        docs = await col.aggregate(pipeline).to_list(length=10)
 
        return response_ok({"pipeline": pipeline, "results": docs})
    except Exception as e:
        logger.exception("Aggregation query failed")
        return response_error(f"Aggregation failed: {str(e)}", 500)

def parse_iso_duration_to_minutes(duration_str: str) -> int:
    """
    Parse ISO 8601 duration format (PT1H30M or PT45M) or HH:MM format to total minutes.
    Returns 0 if parsing fails.
    
    Examples:
        PT0H0M -> 0
        PT1H3M -> 63
        PT34M -> 34
        PT2H -> 120
        00:35 -> 35
        01:45 -> 105
        1:30 -> 90
    """
    if not duration_str or duration_str == "":
        return 0
    
    try:
        duration_str = duration_str.strip()
        
        # Check if it's HH:MM format (contains colon)
        if ':' in duration_str:
            parts = duration_str.split(':')
            if len(parts) == 2:
                hours = int(parts[0])
                minutes = int(parts[1])
                return (hours * 60) + minutes
            else:
                logger.warning(f"Invalid HH:MM format: {duration_str}")
                return 0
        
        # Otherwise, try ISO 8601 format (PT...)
        if duration_str.startswith('PT'):
            # Pattern: PT followed by optional hours (H) and optional minutes (M)
            pattern = r'PT(?:(\d+)H)?(?:(\d+)M)?'
            match = re.match(pattern, duration_str)
            
            if not match:
                logger.warning(f"Could not parse ISO duration: {duration_str}")
                return 0
            
            hours = int(match.group(1)) if match.group(1) else 0
            minutes = int(match.group(2)) if match.group(2) else 0
            
            return (hours * 60) + minutes
        
        # If neither format matches
        logger.warning(f"Unknown duration format: {duration_str}")
        return 0
        
    except Exception as e:
        logger.warning(f"Error parsing duration {duration_str}: {e}")
        return 0


def format_minutes_to_readable(total_minutes: int) -> str:
    """Convert total minutes to readable format: '2 hours 15 minutes'"""
    if total_minutes == 0:
        return "No delay"
    
    hours = total_minutes // 60
    minutes = total_minutes % 60
    
    parts = []
    if hours > 0:
        parts.append(f"{hours} hour{'s' if hours != 1 else ''}")
    if minutes > 0:
        parts.append(f"{minutes} minute{'s' if minutes != 1 else ''}")
    
    return " ".join(parts)

@mcp.tool()
async def get_total_delay_aggregated(
    carrier: str = "",
    flight_number: str = "",
    date_of_origin: str = "",
    start_date: str = "",
    end_date: str = "",
    startStation: Optional[str] = None,
    endStation: Optional[str] = None
) -> str:
    """
    Calculate total delay across all flight legs for a given flight.
    Supports both single date and date range queries.
    Parses ISO 8601 duration format and returns aggregated delay in minutes and readable format.
    
    Args:
        carrier: Airline carrier code (e.g., "6E")
        flight_number: Flight number as string (e.g., "215")
        date_of_origin: Single date in YYYY-MM-DD format (e.g., "2024-06-23")
        start_date: Start date for range query (YYYY-MM-DD)
        end_date: End date for range query (YYYY-MM-DD)
        startStation: Optional filter by start station
        endStation: Optional filter by end station
    
    Returns:
        JSON with total delay in minutes, readable format, and breakdown by leg/date
    """
    logger.info(f"get_total_delay_aggregated: carrier={carrier}, flight_number={flight_number}, date={date_of_origin}, start_date={start_date}, end_date={end_date}")
    
    # Normalize inputs
    fn = normalize_flight_number(flight_number) if flight_number else None
    
    # Build query based on date parameters
    query = {}
    
    if carrier:
        query["flightLegState.carrier"] = carrier
    if fn is not None:
        query["flightLegState.flightNumber"] = fn
    
    # Handle date logic: range takes precedence over single date
    if start_date and end_date:
        sd = validate_date(start_date)
        ed = validate_date(end_date)
        if not sd or not ed:
            return response_error("Invalid start_date or end_date format.", 400)
        query["flightLegState.dateOfOrigin"] = {"$gte": sd, "$lte": ed}
        date_range = f"{sd} to {ed}"
    elif date_of_origin:
        dob = validate_date(date_of_origin)
        if not dob:
            return response_error("Invalid date_of_origin format.", 400)
        query["flightLegState.dateOfOrigin"] = dob
        date_range = dob
    else:
        return response_error("Either date_of_origin or start_date + end_date must be provided.", 400)
    
    if startStation:
        query["flightLegState.startStation"] = startStation
    if endStation:
        query["flightLegState.endStation"] = endStation
    
    projection = {
        "flightLegState.carrier": 1,
        "flightLegState.flightNumber": 1,
        "flightLegState.dateOfOrigin": 1,
        "flightLegState.startStation": 1,
        "flightLegState.endStation": 1,
        "flightLegState.seqNumber": 1,
        "flightLegState.delays.total": 1,
        "flightLegState.scheduledStartTime": 1
    }
    
    try:
        _, _, col = await get_mongodb_client()
        logger.info(f"Executing delay aggregation query: {json.dumps(query)}")
        
        # Fetch all matching documents, sorted by date and sequence
        cursor = col.find(query, projection).sort([
            ("flightLegState.dateOfOrigin", 1),
            ("flightLegState.seqNumber", 1)
        ])
        
        flight_legs = []
        total_delay_minutes = 0
        dates_processed = set();
        
        async for doc in cursor:
            fl = doc.get("flightLegState", {}) or {}
            delay_str = fl.get("delays", {}).get("total", "PT0H0M")
            delay_minutes = parse_iso_duration_to_minutes(delay_str)
            
            total_delay_minutes += delay_minutes
            dates_processed.add(fl.get("dateOfOrigin"))
            
            flight_legs.append({
                "date": fl.get("dateOfOrigin"),
                "startStation": fl.get("startStation"),
                "endStation": fl.get("endStation"),
                "seqNumber": fl.get("seqNumber"),
                "delayRaw": delay_str,
                "delayMinutes": delay_minutes,
                "scheduledStartTime": fl.get("scheduledStartTime")
            })
        
        if not flight_legs:
            logger.warning(f"No flights found for query: {json.dumps(query)}")
            return response_error("No matching flights found.", 404)
        
        result = {
            "carrier": carrier,
            "flightNumber": fn,
            "dateRange": date_range,
            "datesProcessed": sorted(list(dates_processed)),
            "totalDelayMinutes": total_delay_minutes,
            "totalDelayReadable": format_minutes_to_readable(total_delay_minutes),
            "numberOfLegs": len(flight_legs),
            "legs": flight_legs,
            "query": query
        }
        
        logger.info(f"Delay aggregation successful: {total_delay_minutes} minutes across {len(flight_legs)} legs over {len(dates_processed)} dates")
        return response_ok(result)
        
    except Exception as exc:
        logger.exception("Delay aggregation query failed")
        return response_error(f"Delay aggregation failed: {str(exc)}", 500)

@mcp.tool()
async def list_delayed_flights(
    start_time: str = "",
    end_time: str = "",
    carrier: str = "",
    limit: int = 50
) -> str:
    """
    List all flights with delays (delay > 0) within a time range.
    
    Args:
        start_time: Start time in ISO format (e.g., "2025-01-28T10:00:00Z")
        end_time: End time in ISO format (e.g., "2025-01-29T10:00:00Z")
        carrier: Optional carrier filter
        limit: Max results (default 50)
    """
    query = {
        "flightLegState.delays.total": {"$nin": ["PT0H0M", "00:00", "", None]}
    }
    
    if start_time and end_time:
        query["flightLegState.scheduledStartTime"] = {"$gte": start_time, "$lte": end_time}
    elif start_time:
        query["flightLegState.scheduledStartTime"] = {"$gte": start_time}
    
    if carrier:
        query["flightLegState.carrier"] = carrier
    
    projection = {
        "_id": 0,
        "flightLegState.carrier": 1,
        "flightLegState.flightNumber": 1,
        "flightLegState.dateOfOrigin": 1,
        "flightLegState.startStation": 1,
        "flightLegState.endStation": 1,
        "flightLegState.scheduledStartTime": 1,
        "flightLegState.delays.total": 1
    }
    
    try:
        _, _, col = await get_mongodb_client()
        cursor = col.find(query, projection).sort("flightLegState.scheduledStartTime", -1).limit(limit)
        
        results = []
        async for doc in cursor:
            fl = doc.get("flightLegState", {})
            delay_str = fl.get("delays", {}).get("total", "PT0H0M")
            results.append({
                "carrier": fl.get("carrier"),
                "flightNumber": fl.get("flightNumber"),
                "date": fl.get("dateOfOrigin"),
                "route": f"{fl.get('startStation')} → {fl.get('endStation')}",
                "scheduledDeparture": fl.get("scheduledStartTime"),
                "delay": delay_str,
                "delayMinutes": parse_iso_duration_to_minutes(delay_str)
            })
        
        return response_ok({
            "count": len(results),
            "delayedFlights": results,
            "query": query
        })
    except Exception as e:
        logger.exception("list_delayed_flights failed")
        return response_error(str(e), 500)
@mcp.tool()
async def get_aircraft_rotation(
    carrier: str = "",
    flight_number: str = "",
    date_of_origin: str = "",
    aircraft_registration: str = "",
    startStation: Optional[str] = None,
    endStation: Optional[str] = None
) -> str:
    """
    Get the complete daily rotation (sequence of flights) for an aircraft.
    Can query by specific flight to find that aircraft's full rotation,
    or directly by aircraft tail number.
    
    Args:
        carrier: Airline carrier code (e.g., "6E")
        flight_number: Flight number to lookup aircraft (e.g., "215")
        date_of_origin: Date in YYYY-MM-DD format (e.g., "2024-06-23")
        aircraft_registration: Direct aircraft tail number (e.g., "VT-IFA" or "VTIVY")
        startStation: Optional start station to narrow down which flight 215
        endStation: Optional end station to narrow down which flight 215
    
    Returns:
        Complete rotation sequence with turnaround times
    """
    logger.info(f"get_aircraft_rotation: carrier={carrier}, flight={flight_number}, date={date_of_origin}, tail={aircraft_registration}, route={startStation}→{endStation}")
    
    _, _, col = await get_mongodb_client()
    
    # ✅ Normalize aircraft registration (handle both "VTIVY" and "VT-IVY" formats)
    if aircraft_registration:
        if aircraft_registration.startswith("VT") and "-" not in aircraft_registration:
            normalized_tail = aircraft_registration[:2] + "-" + aircraft_registration[2:]
            logger.info(f"🔧 Normalized tail number: {aircraft_registration} → {normalized_tail}")
            aircraft_registration = normalized_tail
    
    # Step 1: If flight number given, find the aircraft tail first
    if flight_number and not aircraft_registration:
        fn = normalize_flight_number(flight_number)
        dob = validate_date(date_of_origin)
        
        if not dob:
            return response_error("Invalid date_of_origin format.", 400)
        
        query = {
            "flightLegState.carrier": carrier,
            "flightLegState.flightNumber": fn,
            "flightLegState.dateOfOrigin": dob
        }
        
        if startStation:
            query["flightLegState.startStation"] = startStation
        if endStation:
            query["flightLegState.endStation"] = endStation
        
        logger.info(f"🔍 Looking for aircraft tail with query: {json.dumps(query)}")
        
        # Get the aircraft tail from this flight
        doc = await col.find_one(query, {
            "flightLegState.equipment.aircraftRegistration": 1,
            "flightLegState.startStation": 1,
            "flightLegState.endStation": 1,
            "flightLegState.carrier": 1,
            "flightLegState.flightNumber": 1,
            "flightLegState.dateOfOrigin": 1
        })
        
        if not doc:
            logger.warning(f"❌ No flight found with query: {json.dumps(query)}")
            
            # Try to find ANY flight with this number on this date
            fallback_query = {
                "flightLegState.carrier": carrier,
                "flightLegState.flightNumber": fn,
                "flightLegState.dateOfOrigin": dob
            }
            cursor = col.find(fallback_query, {
                "flightLegState.startStation": 1,
                "flightLegState.endStation": 1,
                "flightLegState.equipment.aircraftRegistration": 1
            }).limit(10)
            
            available_routes = []
            async for d in cursor:
                fl = d.get("flightLegState", {})
                tail = fl.get("equipment", {}).get("aircraftRegistration")
                available_routes.append({
                    "route": f"{fl.get('startStation')} → {fl.get('endStation')}",
                    "aircraft": tail if tail else "No equipment data"
                })
            
            if available_routes:
                return response_error(
                    f"Flight {carrier} {flight_number} exists on {dob} but not for route {startStation}→{endStation}. "
                    f"Available routes: {json.dumps(available_routes)}",
                    404
                )
            else:
                return response_error(
                    f"No flight {carrier} {flight_number} found on {dob}",
                    404
                )
        
        fl = doc.get("flightLegState", {})
        aircraft_registration = fl.get("equipment", {}).get("aircraftRegistration")
        
        # ✅ NEW: Handle missing equipment data
        if not aircraft_registration:
            # Check if ANY flights on this date have equipment data
            sample_doc = await col.find_one({
                "flightLegState.dateOfOrigin": dob,
                "flightLegState.equipment.aircraftRegistration": {"$exists": True, "$ne": None}
            }, {"flightLegState.equipment.aircraftRegistration": 1})
            
            if sample_doc:
                # Some flights have equipment, but not this one
                return response_error(
                    f"Aircraft registration not found for flight {carrier} {flight_number} "
                    f"from {fl.get('startStation')} to {fl.get('endStation')} on {dob}. "
                    f"The equipment data is missing for this specific flight, though other flights on this date have it. "
                    f"Cannot determine rotation without tail number.",
                    404
                )
            else:
                # NO flights on this date have equipment data
                return response_error(
                    f"No aircraft registration data available for ANY flights on {dob}. "
                    f"The database does not contain equipment information for this date. "
                    f"Rotation queries require aircraft tail numbers to work.",
                    404
                )
        
        logger.info(f"✅ Found aircraft: {aircraft_registration}")
    
    # Step 2: Get ALL flights for this aircraft on this date
    if not aircraft_registration:
        return response_error("Either flight_number or aircraft_registration must be provided", 400)
    
    dob = validate_date(date_of_origin) if date_of_origin else None
    if date_of_origin and not dob:
        return response_error("Invalid date format", 400)
    
    rotation_query = {
        "flightLegState.equipment.aircraftRegistration": aircraft_registration,
        "flightLegState.dateOfOrigin": dob
    }
    
    logger.info(f"🔍 Rotation query: {json.dumps(rotation_query)}")
    
    projection = {
        "_id": 0,
        "flightLegState.carrier": 1,
        "flightLegState.flightNumber": 1,
        "flightLegState.dateOfOrigin": 1,
        "flightLegState.startStation": 1,
        "flightLegState.endStation": 1,
        "flightLegState.scheduledStartTime": 1,
        "flightLegState.scheduledEndTime": 1,
        "flightLegState.operation.actualTimes.offBlock": 1,
        "flightLegState.operation.actualTimes.inBlock": 1,
        "flightLegState.blockTimeSch": 1,
        "flightLegState.blockTimeActual": 1,
        "flightLegState.flightStatus": 1,
        "flightLegState.seqNumber": 1,
        "flightLegState.equipment.aircraftRegistration": 1
    }
    
    try:
        cursor = col.find(rotation_query, projection).sort("flightLegState.scheduledStartTime", 1)
        
        flights = []
        total_flight_minutes = 0
        total_ground_minutes = 0
        
        async for doc in cursor:
            fl = doc.get("flightLegState", {})
            
            # Parse block time
            block_time = fl.get("blockTimeActual") or fl.get("blockTimeSch", "PT0H0M")
            block_minutes = parse_iso_duration_to_minutes(block_time)
            total_flight_minutes += block_minutes
            
            flights.append({
                "carrier": fl.get("carrier"),
                "flightNumber": fl.get("flightNumber"),
                "route": f"{fl.get('startStation')} → {fl.get('endStation')}",
                "scheduledDeparture": fl.get("scheduledStartTime"),
                "scheduledArrival": fl.get("scheduledEndTime"),
                "actualDeparture": fl.get("operation", {}).get("actualTimes", {}).get("offBlock"),
                "actualArrival": fl.get("operation", {}).get("actualTimes", {}).get("inBlock"),
                "blockTime": block_time,
                "blockMinutes": block_minutes,
                "status": fl.get("flightStatus")
            })
        
        if not flights:
            logger.warning(f"❌ No rotation found for query: {json.dumps(rotation_query)}")
            return response_error(
                f"No flights found for aircraft {aircraft_registration} on {date_of_origin}. "
                f"The aircraft may not have operated on this date, or it's registered under a different tail number format.",
                404
            )
        
        # Calculate turnaround times
        for i in range(len(flights) - 1):
            curr_arrival = flights[i]["scheduledArrival"]
            next_departure = flights[i + 1]["scheduledDeparture"]
            
            if curr_arrival and next_departure:
                try:
                    arr_time = datetime.fromisoformat(curr_arrival.replace("Z", "+00:00"))
                    dep_time = datetime.fromisoformat(next_departure.replace("Z", "+00:00"))
                    turnaround_minutes = int((dep_time - arr_time).total_seconds() / 60)
                    flights[i]["turnaroundMinutes"] = turnaround_minutes
                    total_ground_minutes += turnaround_minutes
                except:
                    flights[i]["turnaroundMinutes"] = None
        
        result = {
            "aircraftRegistration": aircraft_registration,
            "carrier": carrier,
            "date": dob,
            "numberOfFlights": len(flights),
            "totalFlightMinutes": total_flight_minutes,
            "totalFlightHours": round(total_flight_minutes / 60, 1),
            "totalGroundMinutes": total_ground_minutes,
            "flights": flights,
            "requestedFlight": {
                "flightNumber": flight_number,
                "route": f"{startStation} → {endStation}" if startStation and endStation else None
            }
        }
        
        logger.info(f"✅ Rotation found: {len(flights)} flights, {total_flight_minutes} min flying, {total_ground_minutes} min ground")
        return response_ok(result)
        
    except Exception as exc:
        logger.exception("get_aircraft_rotation failed")
        return response_error(f"Failed to get rotation: {str(exc)}", 500)
# --- Run MCP Server ---
if __name__ == "__main__":
    logger.info("Starting FlightOps MCP Server on %s:%s (transport=%s)", HOST, PORT, TRANSPORT)
    logger.info("MongoDB URL: %s, Database: %s, Collection: %s", MONGODB_URL, DATABASE_NAME, COLLECTION_NAME)
    mcp.run(transport="streamable-http")
