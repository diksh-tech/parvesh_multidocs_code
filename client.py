import os
import json
import logging
import asyncio
from typing import List, Dict, Any

from dotenv import load_dotenv
from openai import AzureOpenAI
from mcp import ClientSession
from mcp.client.streamable_http import streamablehttp_client

from tool_registry import TOOLS

# Load environment variables
load_dotenv()

MCP_SERVER_URL = os.getenv("MCP_SERVER_URL", "http://127.0.0.1:8000").rstrip("/")

# Azure OpenAI configuration
AZURE_OPENAI_KEY = os.getenv("AZURE_OPENAI_KEY")
AZURE_OPENAI_ENDPOINT = os.getenv("AZURE_OPENAI_ENDPOINT")
AZURE_OPENAI_DEPLOYMENT = os.getenv("AZURE_OPENAI_DEPLOYMENT_NAME", "gpt-4.1-nano")
AZURE_API_VERSION = os.getenv("AZURE_API_VERSION", "2024-12-01-preview")

if not AZURE_OPENAI_KEY:
    raise RuntimeError("❌ AZURE_OPENAI_KEY not set in environment")

logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
logger = logging.getLogger("FlightOps.MCPClient")

# Initialize Azure OpenAI client
client_azure = AzureOpenAI(
    api_key=AZURE_OPENAI_KEY,
    api_version=AZURE_API_VERSION,
    azure_endpoint=AZURE_OPENAI_ENDPOINT
)


def _build_tool_prompt() -> str:
    """Convert TOOLS dict into compact text to feed the LLM."""
    lines = []
    for name, meta in TOOLS.items():
        arg_str = ", ".join(meta["args"])
        lines.append(f"- {name}({arg_str}): {meta['desc']}")
    return "\n".join(lines)


SYSTEM_PROMPT_PLAN = f"""
You are an assistant that converts user questions into MCP tool calls.

**CRITICAL: Current database time is 2025-01-29T06:30:00Z. Use this as "now" for all time calculations.**

Available tools:
{_build_tool_prompt()}

### MANDATORY TIME FILTER RULES (READ FIRST!)
- For queries like "last N hours/days":
  1. Calculate start time: now - N hours (in UTC)
  2. ALWAYS add scheduledStartTime filter to filter_json
  3. Example: "last 10 hours" from 2025-01-29T06:30:00Z = start at 2025-01-28T20:30:00Z
  4. Must include: {{"flightLegState.scheduledStartTime": {{"$gte": "calculated-start-time"}}}}

### Tool selection logic

Tool selection rules (keep concise: use schema fields exactly):
-**CRITICAL: For LISTING/showing flights (even if query says "all"), use raw_mongodb_query, NOT run_aggregated_query**
-For numeric summaries ONLY (counts,avg,sum,min,max) use run_aggregated_query.
-For total delay of a specific flight (single date OR date range), use get_total_delay_aggregated.
-**IMPORTANT**: When querying a specific flight by number + date, you MUST also extract startStation and endStation from the user query if mentioned (e.g., "DEL to BOM" → startStation="DEL", endStation="BOM"). Multiple flights share the same number on the same day with different routes.
-For lists, filtered fetches, or flexible field selection use raw_mongodb_query.
-For single-flight lookups (carrier + flight_number + date) use get_flight_basic_info, get_operation_times, etc.

### CRITICAL DATA TYPE RULES:
1. **flightNumber**: ALWAYS use STRING ("215"), stored as string in MongoDB
2. **delays.total**: Stored as STRING ("PT1H30M" or "00:00"). To find delayed flights:
   - Use: {{"flightLegState.delays.total": {{"$nin": ["PT0H0M", "00:00", "", null]}}}}
   - NEVER use numeric comparisons like {{"$gt": 0}}
3. **dateOfOrigin**: Use string format "YYYY-MM-DD"
4. **scheduledStartTime**: ISO 8601 datetime string for comparison
5. **TIME RANGE QUERIES (CRITICAL)**:
   - Current database time: 2025-01-29T06:30:00Z (treat this as "now")
   - For "last N hours": calculate start_time = now - N hours
   - Example: "last 10 hours" → start_time = "2025-01-28T20:30:00Z"
   - ALWAYS include scheduledStartTime filter for time-based queries
   - Format: {{"flightLegState.scheduledStartTime": {{"$gte": "YYYY-MM-DDTHH:MM:SSZ"}}}}

### QUERY TYPE SELECTION:
- **"rotation of flight X"** → Use `get_aircraft_rotation` to show full daily sequence
- **"list/show/display delayed flights"** → Use `raw_mongodb_query` with delay filter
- **"count delayed flights"** or **"how many delayed"** → Use `run_aggregated_query` with delay filter
- **"count delayed flights in last N hours"** → Use `run_aggregated_query` with BOTH delay filter AND time filter
- **"list delayed flights in last N hours"** → Use `raw_mongodb_query` with BOTH delay filter AND time filter
- **"total delay"** or **"average delay"** → Use `get_total_delay_aggregated`
- **"average/sum/min/max of numeric fields"** → Use `run_aggregated_query`

     Example (delay status with route - CORRECT):
     {{{{
       "plan": [
         {{{{
           "tool": "get_delay_summary",
           "arguments": {{{{
             "carrier": "6E",
             "flight_number": "215",  # ✅ String now
             "date_of_origin": "2024-06-23",
             "startStation": "SXR",
             "endStation": "BOM"
           }}}}
         }}}}
       ]
     }}}}
     
     Example (delay status without route - will return multiple matches):
     {{{{
       "plan": [
         {{{{
           "tool": "get_delay_summary",
           "arguments": {{{{
             "carrier": "6E",
             "flight_number": 215,  # ✅ CHANGED: Integer
             "date_of_origin": "2024-06-23"
           }}}}
         }}}}
       ]
     }}}}
     Note: This will return needs_route_selection=True if multiple routes exist.

     Example (LIST delayed flights in last 10 hours - CORRECT):
     {{{{
       "plan": [
         {{{{
           "tool": "raw_mongodb_query",
           "arguments": {{{{
             "query_json": "{{{{'flightLegState.delays.total': {{{{'$nin': ['PT0H0M', '00:00', '', null]}}}} , 'flightLegState.scheduledStartTime': {{{{'$gte': '2025-01-28T20:30:00Z'}}}}}}}}",
             "projection": "{{{{'_id': 0, 'flightLegState.carrier': 1, 'flightLegState.flightNumber': 1, 'flightLegState.startStation': 1, 'flightLegState.endStation': 1, 'flightLegState.delays.total': 1, 'flightLegState.scheduledStartTime': 1}}}}",
             "limit": 50
           }}}}
         }}}}
       ]
     }}}}
     
     Example (COUNT delayed flights in last 10 hours - CORRECT):
     {{{{
       "plan": [
         {{{{
           "tool": "run_aggregated_query",
           "arguments": {{{{
             "query_type": "count",
             "field": "flightLegState.flightNumber",
             "filter_json": "{{{{'flightLegState.delays.total': {{{{'$nin': ['PT0H0M', '00:00', '', null]}}}} , 'flightLegState.scheduledStartTime': {{{{'$gte': '2025-01-28T20:30:00Z'}}}}}}}}"
           }}}}
         }}}}
       ]
     }}}}
     
     Example (count delayed flights without time filter - WRONG):
     {{{{
       "plan": [
         {{{{
           "tool": "run_aggregated_query",
           "arguments": {{{{
             "query_type": "count",
             "field": "flightLegState.flightNumber",
             "filter_json": "{{{{'flightLegState.delays.total': {{{{'$nin': ['PT0H0M', '00:00']}}}}}}}}"
           }}}}
         }}}}
       ]
     }}}}
     Note: This will count ALL delayed flights in database history (wrong for "last N hours").

### Schema summary (for projection guidance)

Flight documents contain(Schema):
    'carrier': 'flightLegState.carrier',
    'date_of_origin': 'flightLegState.dateOfOrigin',
    'flight_number': 'flightLegState.flightNumber',
    'suffix': 'flightLegState.suffix',
    'sequence_number': 'flightLegState.seqNumber',
    'origin': 'flightLegState.startStation',
    'destination': 'flightLegState.endStation',
    'scheduled_departure': 'flightLegState.scheduledStartTime',
    'scheduled_arrival': 'flightLegState.scheduledEndTime',
    'end_terminal': 'flightLegState.endTerminal',
    'operational_status': 'flightLegState.operationalStatus',
    'flight_status': 'flightLegState.flightStatus',
    'start_country': 'flightLegState.startCountry',
    'end_country': 'flightLegState.endCountry',
    'aircraft_registration': 'flightLegState.equipment.aircraftRegistration',
    'aircraft_type': 'flightLegState.equipment.assignedAircraftTypeIATA',
    'start_gate': 'flightLegState.startGate',
    'end_gate': 'flightLegState.endGate',
    'start_terminal': 'flightLegState.startTerminal',
    'delay': 'flightLegState.delays.total',
    'flight_type': 'flightLegState.flightType',
    'operations': 'flightLegState.operation',
    'estimated_times': 'flightLegState.operation.estimatedTimes',
    'off_block_time': 'flightLegState.operation.estimatedTimes.offBlock',
    'in_block_time': 'flightLegState.operation.estimatedTimes.inBlock',
    'takeoff_time': 'flightLegState.operation.estimatedTimes.takeoffTime',
    'landing_time': 'flightLegState.operation.estimatedTimes.landingTime',
    'actual_times': 'flightLegState.operation.actualTimes',
    'actual_off_block_time': 'flightLegState.operation.actualTimes.offBlock',
    'actual_in_block_time': 'flightLegState.operation.actualTimes.inBlock',
    'actual_takeoff_time': 'flightLegState.operation.actualTimes.takeoffTime',
    'actual_landing_time': 'flightLegState.operation.actualTimes.landingTime',
    'door_close_time': 'flightLegState.operation.estimatedTimes.doorClose',
    'fuel':'flightLegState.operation.fuel',
    'fuel_off_block':'flightLegState.operation.fuel.offBlock',
    'fuel_takeoff':'flightLegState.operation.fuel.takeoff',
    'fuel_landing':'flightLegState.operation.fuel.landing',
    'fuel_in_block':'flightLegState.operation.fuel.inBlock',
    'autoland':'flightLegState.operation.autoland',
    'flight_plan':'flightLegState.operation.flightPlan',
    'estimated_Elapsed_time':'flightLegState.operation.flightPlan.estimatedElapsedTime',
    'actual_Takeoff_time':'flightLegState.operation.flightPlan.acTakeoffWeight',
    'flight_plan_takeoff_fuel':'flightLegState.operation.flightPlan.takeoffFuel',
    'flight_plan_landing_fuel':'flightLegState.operation.flightPlan.landingFuel',
    'flight_plan_hold_fuel':'flightLegState.operation.flightPlan.holdFuel',
    'flight_plan_hold_time':'flightLegState.operation.flightPlan.holdTime',
    'flight_plan_route_distance':'flightLegState.operation.flightPlan.routeDistance',
    'start_country':'flightLegState.startCountry',
    'end_country':'flightLegState.endCountry',
    'ICAO_start_station':'flightLegState.startStationICAO',
    'ICAO_end_station':'flightLegState.endStationICAO',
    'Flight_otp_achieved':'flightLegState.isOTPAchieved',
    'Flight_otp_considered':'flightLegState.isOTPConsidered',
    'Flight_otp_status':'flightLegState.isOTPFlight',
    'Flight_type':'flightLegState.flightType',
    'scheduled_block_time':'flightLegState.blockTimeSch',
    'acutal_block_time':'flightLegState.blockTimeActual',
    'start_time_offset':'flightLegState.startTimeOffset',
    'end_time_offset':'flightLegState.endTimeOffset',
    'passenger_count':'flightLegState.pax.passengerCount.code'

---

### Projection rules for `raw_mongodb_query`
- Only include fields relevant to the question.
- Always exclude "_id".
- Examples:
  - "passenger" → include flightNumber, pax.passengerCount.code
  - "delay" or "reason" → include flightNumber, flightLegState.delays.total
  - "aircraft" or "tail" → include equipment.aircraftRegistration, aircraft.type
  - "station" or "sector" → include startStation, endStation, terminals
  - "crew" → include crewConnections.crew.givenName, position
  - "timing / departure / arrival / dep / arr" → include scheduledStartTime, scheduledEndTime, operation.actualTimes
  - "fuel" → include operation.fuel
  - "OTP" or "on-time" → include isOTPAchieved, flightStatus

---

### General rules
1. Always return valid JSON with a top-level "plan" key.
2. Use the correct tool type based on query intent.
3. Never invent field names — use schema fields only.
4. Never return "_id" in projections.
5. For numerical summaries → use run_aggregated_query.
6. For filtered listings → use raw_mongodb_query.
7. For StartTimeOffset and EndTimeOffset → use run_aggregated_query.
8. Latest date in the database is 2025-01-30 and consider it today's date.
9. Output ONLY JSON. Do not wrap in ```json fences.
10. Keys and all string values MUST use double quotes (valid JSON).
11. Never include comments.
12. Use run_aggregated_query for counts, sums, averages, mins, maxes.
13. Use raw_mongodb_query for listings / detailed retrieval.
14. For time range phrases like "last N hours":
   - Assume 'now' = 2025-01-29T06:30:00Z
   - Compute start = now - N hours (ISO UTC).   
15. Do NOT invent fields; use schema.
16. Never include "_id" in projections.
17. If you need count of flights with a constraint, set:
   "tool": "run_aggregated_query", "arguments": {{{{
       "query_type": "count",
       "field": "flightLegState.flightNumber",
       "filter_json": "<VALID JSON OBJECT STRING>"
   }}}}
18. filter_json must itself be a valid JSON object string (escaped correctly).
19. For total delay across multiple legs of same flight, use get_total_delay_aggregated.
Return ONLY the JSON plan.
"""


SYSTEM_PROMPT_SUMMARIZE = """
You are an assistant that summarizes tool outputs into a concise, readable answer.
Be factual, bullet points format and helpful. 
Give only bullet points and never use *(asteriks)
List all the details if multiple flights are available.
always convert times to local time format (e.g., "2025-01-15 14:30 Local Time").If UTC is present
"""


class FlightOpsMCPClient:
    def __init__(self, base_url: str = None):
        self.base_url = (base_url or MCP_SERVER_URL).rstrip("/")
        self.session: ClientSession = None
        self._client_context = None

    
    async def connect(self):
        try:
            logger.info(f"Connecting to MCP server at {self.base_url}")
            self._client_context = streamablehttp_client(self.base_url)
            read_stream, write_stream, _ = await self._client_context.__aenter__()
            self.session = ClientSession(read_stream, write_stream)
            await self.session.__aenter__()
            await self.session.initialize()
            logger.info("✅ Connected to MCP server successfully")
        except Exception as e:
            logger.error(f"Failed to connect to MCP server: {e}")
            raise

    async def disconnect(self):
        try:
            if self.session:
                await self.session.__aexit__(None, None, None)
            if self._client_context:
                await self._client_context.__aexit__(None, None, None)
            logger.info("Disconnected from MCP server")
        except Exception as e:
            logger.error(f"Error during disconnect: {e}")

    # -------------------- AZURE OPENAI WRAPPER -------------------------
    def _call_azure_openai(self, messages: list, temperature: float = 0.2) -> str:
        try:
            completion = client_azure.chat.completions.create(
                model=AZURE_OPENAI_DEPLOYMENT,
                messages=messages,
                temperature=temperature,
            )
            return completion.choices[0].message.content
        except Exception as e:
            logger.error(f"Azure OpenAI API error: {e}")
            return json.dumps({"error": str(e)})

    # -------------------- MCP TOOL CALLS -------------------------
    async def list_tools(self) -> dict:
        try:
            if not self.session:
                await self.connect()
            tools_list = await self.session.list_tools()
            tools_dict = {tool.name: {"description": tool.description, "inputSchema": tool.inputSchema} for tool in tools_list.tools}
            return {"tools": tools_dict}
        except Exception as e:
            logger.error(f"Error listing tools: {e}")
            return {"error": str(e)}

    async def invoke_tool(self, tool_name: str, args: dict) -> dict:
        try:
            if not self.session:
                await self.connect()
            logger.info(f"Calling tool: {tool_name} with args: {args}")
            result = await self.session.call_tool(tool_name, args)  

            if result.content:
                content_items = []
                for item in result.content:
                    if hasattr(item, 'text'):
                        try:
                            content_items.append(json.loads(item.text))
                        except json.JSONDecodeError:
                            content_items.append(item.text)
                if len(content_items) == 1:
                    return content_items[0]
                return {"results": content_items}

            return {"error": "No content in response"}
        except Exception as e:
            logger.error(f"Error invoking tool {tool_name}: {e}")
            return {"error": str(e)}

    # -------------------- LLM PLANNING & SUMMARIZATION -------------------------
    async def plan_tools(self, user_query: str) -> dict:
        """
        Ask the LLM to produce a valid JSON plan for which MCP tools to call.
        Cleans out Markdown-style fences (```json ... ```), which some models add.
        """
        messages = [
            {"role": "system", "content": SYSTEM_PROMPT_PLAN},
            {"role": "user", "content": user_query},
        ]

        content = await asyncio.to_thread(self._call_azure_openai,messages, temperature=0.1)
        if not content:
            logger.warning("⚠️ LLM returned empty response during plan generation.")
            return {"plan": []}
     
        cleaned = content.strip()
        if cleaned.startswith("```"):
            
            cleaned = cleaned.strip("`")
            if cleaned.lower().startswith("json"):
                cleaned = cleaned[4:].strip()
            
            cleaned = cleaned.replace("```", "").strip()

        
        if cleaned != content:
            logger.debug(f"🔍 Cleaned LLM plan output:\n{cleaned}")

       
        try:
            plan = json.loads(cleaned)
            if isinstance(plan, dict) and "plan" in plan:
                return plan
            else:
                logger.warning("⚠️ LLM output did not contain 'plan' key.")
                return {"plan": []}
        except json.JSONDecodeError:
            logger.warning(f"❌ Could not parse LLM plan output after cleaning:\n{cleaned}")
            return {"plan": []}


    def summarize_results(self, user_query: str, plan: list, results: list) -> dict:
        messages = [
            {"role": "system", "content": SYSTEM_PROMPT_SUMMARIZE},
            {"role": "user", "content": f"Question:\n{user_query}"},
            {"role": "assistant", "content": f"Plan:\n{json.dumps(plan, indent=2)}"},
            {"role": "assistant", "content": f"Results:\n{json.dumps(results, indent=2)}"},
        ]
        summary = self._call_azure_openai(messages, temperature=0.3)
        return {"summary": summary}

   
    async def run_query(self, user_query: str) -> dict:
        """
        Full flow:
        1. LLM plans which tools to call (including possible MongoDB query).
        2. Execute tools sequentially via MCP.
        3. Summarize results using LLM.
        """
        try:
            logger.info(f"User query: {user_query}")
            plan_data = await self.plan_tools(user_query)
            plan = plan_data.get("plan", [])
            if not plan:
                return {"error": "LLM did not produce a valid tool plan."}

            results = []
            for step in plan:
                tool = step.get("tool")
                args = step.get("arguments", {})

                # Clean up bad args
                args = {k: v for k, v in args.items() if v and str(v).strip().lower() != "unknown"}

                # Safety for MongoDB query
                if tool == "raw_mongodb_query":
                    query_json = args.get("query_json", "")
                    if not query_json:
                        results.append({"raw_mongodb_query": {"error": "Empty query_json"}})
                        continue
                    # Enforce safe default limit
                    args["limit"] = int(args.get("limit", 50))
                    logger.info(f"Executing raw MongoDB query: {query_json}")

                resp = await self.invoke_tool(tool, args)
                results.append({tool: resp})
                #short-circuit if the tool indicates multiple matching routes and needs selection.
                #This tells the caller (ag_ui adapter /UI) to ask the user which doc_id to pick.
                #We detect the server-style response shape: ("ok": True, "data":{"needs_route_selection":True, ...})
                try: 
                    if isinstance(resp,dict):
                        candidate= None
                        if resp.get("ok") and isinstance(resp.get("data"),dict):
                            candidate = resp["data"]
                        elif resp.get(tool) and isinstance(resp.get(tool),dict):
                            candidate = resp.get(tool)
                        else:
                            candidate=resp
                        if isinstance(candidate,dict) and candidate.get("needs_route_selection"):  # ✅ Fixed typo
                            return {"plan":plan, "results": results, "needs_route_selection":True}  # ✅ Also fixed here
                except:
                    pass

            summary = self.summarize_results(user_query, plan, results)
            return {"plan": plan, "results": results, "summary": summary}
        except Exception as e:
            logger.error(f"Error in run_query: {e}")
            return {"error": str(e)}


