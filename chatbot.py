from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import Optional
import logging
import psycopg2
import os
from azure.core.credentials import AzureKeyCredential
from azure.ai.language.questionanswering import QuestionAnsweringClient

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

router = APIRouter()

class ChatbotConfig:
    def __init__(self, 
                 qa_endpoint: str, 
                 qa_key: str, 
                 postgres_conn_str: str):
        """
        Initialize ChatbotConfig with direct values
        
        Args:
            qa_endpoint (str): Azure Language Service endpoint
            qa_key (str): Azure Language Service key
            postgres_conn_str (str): PostgreSQL connection string
        """
        self.qa_endpoint = qa_endpoint
        self.qa_key = qa_key
        self.postgres_conn_str = postgres_conn_str
        
        # Optional: Add basic validation
        self.validate_config()
    
    def validate_config(self):
        """Validate configuration settings"""
        if not self.qa_endpoint:
            raise ValueError("Azure Language Endpoint cannot be empty")
        if not self.qa_key:
            raise ValueError("Azure Language Key cannot be empty")
        if not self.postgres_conn_str:
            raise ValueError("Database connection string cannot be empty")

# Example usage:
config = ChatbotConfig(
    qa_endpoint="https://anomelychatbot.cognitiveservices.azure.com/",
    qa_key="1AwPooDRkhwadftxu9M1GVUTZskbF14pKY7DabdU366tNxKQurozJQQJ99BCACYeBjFXJ3w3AAAaACOGWekF",
    postgres_conn_str="postgresql://postgres:postgres@localhost:5432/cdc_data"
)
class UserQuery(BaseModel):
    text: str
    user_id: str
    channel: str  # "sms", "teams", or "web"

def create_language_client() -> QuestionAnsweringClient:
    """
    Create and return an Azure Language Service client
    
    Returns:
        QuestionAnsweringClient: Configured client for question answering
    """
    try:
        return QuestionAnsweringClient(
            endpoint=config.qa_endpoint, 
            credential=AzureKeyCredential(config.qa_key)
        )
    except Exception as e:
        logger.error(f"Failed to create Language Service client: {e}")
        raise HTTPException(status_code=500, detail="Failed to initialize chatbot service")

def classify_intent(client: QuestionAnsweringClient, query: str) -> str:
    """
    Classify intent using Azure Language Service
    
    Args:
        client (QuestionAnsweringClient): Configured Language Service client
        query (str): User's query text
    
    Returns:
        str: Classified intent
    """
    try:
        # Perform intent classification
        response = client.get_answers(
            question=query,
            project_name="cdc-chatbot",  # Exact project name from Azure
            deployment_name="production"
        )
        
        # Log classification details for debugging
        logger.info(f"Intent Classification Response: {response}")
        
        # Extract intent from response
        if response.answers and len(response.answers) > 0:
            # Assuming the first answer contains the intent
            return response.answers[0].intent
        
        # Default to unknown if no intent found
        return "unknown"
    
    except Exception as e:
        # Comprehensive error logging
        logger.error(f"Intent Classification Error: {type(e)}")
        logger.error(f"Detailed Error: {str(e)}")
        
        # Return default intent on failure
        return "unknown"

@router.post("/chatbot/query")
async def handle_chatbot_query(query: UserQuery):
    """
    Main endpoint for processing chatbot queries
    
    Args:
        query (UserQuery): User's query details
    
    Returns:
        dict: Processed response
    """
    try:
        # Create Language Service client
        qa_client = create_language_client()
        
        # Classify intent
        intent = classify_intent(qa_client, query.text)
        
        # Route to appropriate handler based on intent
        intent_handlers = {
            "historical": handle_historical_query,
            "anomaly": handle_anomaly_query,
            "prediction": handle_prediction_query
        }
        
        # Select handler or use default
        handler = intent_handlers.get(intent, lambda q: {"error": "Unrecognized query type"})
        response = handler(query.text)
        
        # Format response for specific channel
        return format_response(response, query.channel)
    
    except Exception as e:
        # Log and handle unexpected errors
        logger.error(f"Chatbot query processing error: {e}")
        raise HTTPException(status_code=500, detail="Chatbot processing failed")

def handle_historical_query(query: str) -> dict:
    """Process historical data queries"""
    try:
        with psycopg2.connect(config.postgres_conn_str) as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT COUNT(*), event_type 
                    FROM cdc_events 
                    WHERE timestamp > NOW() - INTERVAL '7 days'
                    GROUP BY event_type
                """)
                results = cursor.fetchall()
                
                return {
                    "summary": f"Found {sum(r[0] for r in results)} changes last week",
                    "details": [f"{count} {event_type}" for count, event_type in results]
                }
    except Exception as e:
        logger.error(f"Historical query error: {e}")
        return {"error": "Failed to retrieve historical data"}

def handle_anomaly_query(query: str) -> dict:
    """Process anomaly detection queries"""
    try:
        # Placeholder for actual anomaly detection logic
        return {
            "summary": "No anomalies detected",
            "anomalies": []
        }
    except Exception as e:
        logger.error(f"Anomaly query error: {e}")
        return {"error": "Failed to process anomaly query"}

def handle_prediction_query(query: str) -> dict:
    """Process prediction queries"""
    try:
        # Placeholder for actual prediction logic
        return {
            "prediction": "12 maintenance needs predicted",
            "confidence": 0.85,
            "timeframe": "next 30 days"
        }
    except Exception as e:
        logger.error(f"Prediction query error: {e}")
        return {"error": "Failed to generate predictions"}

def format_response(data: dict, channel: str) -> dict:
    """Format response based on communication channel"""
    if channel == "sms":
        return {"message": data.get("summary", "No response generated")}
    return data




