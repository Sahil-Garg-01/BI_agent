import os
import logging
import time
import requests
from typing import Dict, List, Optional
from dotenv import load_dotenv

logger = logging.getLogger(__name__)

load_dotenv()

MONDAY_API_KEY = os.getenv("MONDAY_API_KEY", "").strip()
MONDAY_URL = "https://api.monday.com/v2"

# Retry configuration
MAX_RETRIES = 3
RETRY_DELAY = 1  # seconds
RETRY_BACKOFF = 2  # exponential multiplier


class MondayAPIError(Exception):
    """Custom exception for Monday.com API errors."""
    pass


class MondayAPITimeout(MondayAPIError):
    """Custom exception for Monday.com API timeout."""
    pass


class MondayAPIValidationError(MondayAPIError):
    """Custom exception for API response validation errors."""
    pass


def validate_api_key() -> None:
    """Validate that API key is configured."""
    if not MONDAY_API_KEY:
        raise MondayAPIError("MONDAY_API_KEY environment variable not set")


def run_query(query: str, timeout: int = 10, retries: int = MAX_RETRIES) -> Dict:
    """
    Execute GraphQL query against Monday.com API with retry logic.
    
    Features:
    - Exponential backoff retry on transient failures
    - Request/response logging for debugging
    - Timeout protection
    - Structured error messages
    
    Args:
        query: GraphQL query string
        timeout: Request timeout in seconds
        retries: Number of retry attempts
        
    Returns:
        Parsed JSON response dict
        
    Raises:
        MondayAPITimeout: If request times out
        MondayAPIError: If API returns error
        MondayAPIValidationError: If response is malformed
    """
    
    validate_api_key()
    
    if not query or not query.strip():
        raise MondayAPIError("Query cannot be empty")
    
    headers = {
        "Authorization": MONDAY_API_KEY,
        "Content-Type": "application/json"
    }
    
    attempt = 0
    last_exception = None
    
    while attempt < retries:
        attempt += 1
        
        try:
            logger.debug(f"Monday API call (attempt {attempt}/{retries}), query length: {len(query)}")
            
            response = requests.post(
                MONDAY_URL,
                json={"query": query},
                headers=headers,
                timeout=timeout
            )
            
            # Success
            if response.status_code == 200:
                data = response.json()
                
                # Check for GraphQL errors in response
                if "errors" in data and data["errors"]:
                    error_msg = str(data["errors"])
                    logger.error(f"GraphQL error: {error_msg}")
                    raise MondayAPIValidationError(f"GraphQL error: {error_msg}")
                
                logger.debug(f"Monday API success: {len(str(data))} bytes returned")
                return data
            
            # Rate limiting or transient error
            elif response.status_code in [429, 500, 502, 503]:
                error_msg = f"Monday API {response.status_code}"
                logger.warning(f"{error_msg}, retrying... (attempt {attempt}/{retries})")
                last_exception = MondayAPIError(error_msg)
                
                if attempt < retries:
                    wait_time = RETRY_DELAY * (RETRY_BACKOFF ** (attempt - 1))
                    logger.debug(f"Waiting {wait_time}s before retry")
                    time.sleep(wait_time)
                    continue
                else:
                    raise last_exception
            
            # Client error
            else:
                error_text = response.text[:200]
                logger.error(f"Monday API error {response.status_code}: {error_text}")
                raise MondayAPIError(f"Monday API error {response.status_code}: {error_text}")
        
        except requests.Timeout as e:
            logger.error(f"Monday API timeout after {timeout}s (attempt {attempt}/{retries})")
            last_exception = MondayAPITimeout(f"Timeout after {timeout}s")
            
            if attempt < retries:
                wait_time = RETRY_DELAY * (RETRY_BACKOFF ** (attempt - 1))
                logger.debug(f"Waiting {wait_time}s before retry")
                time.sleep(wait_time)
                continue
            else:
                raise last_exception
        
        except requests.RequestException as e:
            logger.error(f"Request error: {str(e)}")
            raise MondayAPIError(f"Request error: {str(e)}")
    
    # Should not reach here, but safety
    if last_exception:
        raise last_exception
    raise MondayAPIError("Unknown error after retries")


def get_board_id_by_name(board_name: str) -> str:
    """
    Fetch board ID by name with error handling.
    
    Args:
        board_name: Board display name
        
    Returns:
        Board ID string
        
    Raises:
        MondayAPIError: If board not found or API fails
    """
    
    if not board_name or not board_name.strip():
        raise MondayAPIError("Board name cannot be empty")
    
    query = """
    query {
      boards {
        id
        name
      }
    }
    """
    
    logger.info(f"Fetching board ID for: {board_name}")
    
    try:
        data = run_query(query)
    except MondayAPIError as e:
        logger.error(f"Failed to fetch boards: {str(e)}")
        raise
    
    if "data" not in data or "boards" not in data["data"]:
        raise MondayAPIValidationError("Unexpected response format from boards query")
    
    for board in data["data"]["boards"]:
        if board.get("name") == board_name:
            board_id = board.get("id")
            logger.info(f"Found board '{board_name}' with ID: {board_id}")
            return board_id
    
    error_msg = f"Board '{board_name}' not found. Available boards: {[b.get('name') for b in data['data']['boards']]}"
    logger.error(error_msg)
    raise MondayAPIError(error_msg)


def fetch_board_items(board_id: str, limit: int = 500) -> List[Dict]:
    """
    Fetch items from board with full column values.
    
    Args:
        board_id: Monday board ID
        limit: Maximum items to fetch (Monday API max: 500)
        
    Returns:
        List of item dicts with column values
        
    Raises:
        MondayAPIError: If fetch fails or invalid board_id
    """
    
    if not board_id or not board_id.strip():
        raise MondayAPIError("Board ID cannot be empty")
    
    if limit < 1 or limit > 500:
        logger.warning(f"limit {limit} out of range [1, 500], clamping to 500")
        limit = 500
    
    query = f"""
    query {{
      boards(ids: {board_id}) {{
        items_page(limit: {limit}) {{
          items {{
            id
            name
            column_values {{
              id
              text
            }}
          }}
        }}
      }}
    }}
    """
    
    logger.info(f"Fetching items from board {board_id} (limit: {limit})")
    
    try:
        data = run_query(query)
    except MondayAPIError as e:
        logger.error(f"Failed to fetch board items: {str(e)}")
        raise
    
    # Validate response structure
    if "data" not in data or "boards" not in data["data"]:
        raise MondayAPIValidationError("Unexpected response format from items query")
    
    boards = data["data"]["boards"]
    if not boards or len(boards) == 0:
        raise MondayAPIError(f"Board {board_id} not found or no items")
    
    items = boards[0].get("items_page", {}).get("items", [])
    logger.info(f"Fetched {len(items)} items from board")
    
    return items