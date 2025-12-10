import json
import os
from typing import Any, Dict, Tuple

import requests
from dotenv import load_dotenv
from databricks.sdk import WorkspaceClient

# Load environment variables from .env file
load_dotenv()


class AIChat:
    def __init__(self, endpoint: str = "databricks-meta-llama-3-1-70b-instruct", workspace_host: str = None):
        """
        Initialize AI Chat with Databricks LLM endpoint.

        Args:
            endpoint: The Databricks serving endpoint name to use
            workspace_host: Databricks workspace host URL (e.g., "https://adb-xxx.azuredatabricks.net")
                           If not provided, will try to get from DATABRICKS_HOST env variable
        """
        self.endpoint = endpoint

        # Get workspace host from parameter or environment
        self.workspace_host = workspace_host or os.getenv("DATABRICKS_HOST")

        # Initialize Databricks SDK client for authentication
        try:
            self.workspace_client = WorkspaceClient()
            self.auth_token = None  # Will be fetched on demand
            print(f"AIChat initialized with Databricks LLM endpoint: {endpoint}")
            if self.workspace_host:
                print(f"Workspace: {self.workspace_host}")
            else:
                print("Using Databricks SDK authentication")
        except Exception as e:
            print(f"Warning: Could not initialize Databricks SDK client: {e}")
            self.workspace_client = None
            self.auth_token = None

        if self.workspace_host:
            # Remove trailing slash if present
            self.workspace_host = self.workspace_host.rstrip('/')
            self.api_url = f"{self.workspace_host}/serving-endpoints/{endpoint}/invocations"
        else:
            # Fallback to relative URL (when running on Databricks)
            self.api_url = f"/serving-endpoints/{endpoint}/invocations"

    def process_user_prompt(
        self, user_message: str, current_filters: Dict[str, Any]
    ) -> Tuple[str, Dict[str, Any]]:
        """
        Process user prompt and return response with structured filter suggestions.

        Args:
            user_message: User's chat input
            current_filters: Current filter state

        Returns:
            Tuple of (response_text, suggested_filters)
        """
        print(f"Processing user message: {user_message}")

        # Get structured response from AI
        ai_response = self._get_structured_ai_response(user_message, current_filters)

        # Parse the response to extract filters and message
        response_text, suggested_filters = self._parse_ai_response(
            ai_response, current_filters
        )

        return response_text, suggested_filters

    def _get_structured_ai_response(
        self, user_message: str, current_filters: Dict[str, Any]
    ) -> str:
        """
        Get structured AI response for car filtering using Databricks LLM.

        Args:
            user_message (str): User's message/question
            current_filters (Dict): Current filter state

        Returns:
            str: AI response in JSON format
        """
        # Create a structured prompt that requests JSON output
        system_prompt = """You are a car search assistant. Analyze the user's request and respond with a JSON object containing:
            1. "message": A helpful response to the user
            2. "filters": Suggested filter changes based on their request

            Available filter categories:
            - "brands": List of car brands (e.g., ["BMW", "Mercedes", "Audi"])
            - "vehicle_types": List of vehicle types (e.g., ["Personenauto", "Bedrijfsauto"])
            - "colors": List of colors (e.g., ["ZWART", "WIT", "BLAUW"])
            - "engine_range": [min_cc, max_cc] (e.g., [1000, 3000])
            - "weight_range": [min_kg, max_kg] (e.g., [1000, 2000])
            - "seats": List of seat options (e.g., ["2.0", "4.0", "5.0", "7+"])
            - "year_range": [min_year, max_year] (e.g., [2010, 2020])

            Only include filter categories that should change based on the user's request. Use null for filters that shouldn't change.

            Example response:
            {
            "message": "I'll help you find cars",
            "filters": {
                "brands": ["BMW"],
                "year_range": [2015, 2020],
                "vehicle_types": null,
                "colors": null,
                "engine_range": null,
                "weight_range": null,
                "seats": null
            }

            In addition, you should also be able to converse with the user to discuss cars in general, to find any car.
            Once the user agrees with the car selection, adjust the filters to find this car.
            }"""

        user_prompt = f"""Current filters: {json.dumps(current_filters)}
            User request: {user_message}

            Please respond with a JSON object as specified."""

        headers = {
            "Content-Type": "application/json"
        }

        # Get authentication token from Databricks SDK
        if self.workspace_client:
            try:
                # authenticate() returns a dict with Authorization header
                auth_headers = self.workspace_client.config.authenticate()
                if isinstance(auth_headers, dict) and "Authorization" in auth_headers:
                    headers["Authorization"] = auth_headers["Authorization"]
                else:
                    print(f"Unexpected auth response: {auth_headers}")
            except Exception as e:
                print(f"Warning: Could not get authentication token: {e}")

        payload = {
            "messages": [
                {
                    "role": "system",
                    "content": system_prompt
                },
                {
                    "role": "user",
                    "content": user_prompt
                }
            ],
            "max_tokens": 1000,
            "temperature": 0.3
        }

        print(f"Sending request to: {self.api_url}")
        print(f"Payload: {json.dumps(payload, indent=2)[:500]}...")  # Print first 500 chars

        try:
            response = requests.post(
                self.api_url,
                headers=headers,
                json=payload,
                timeout=60
            )
            response.raise_for_status()

            result = response.json()

            # Extract the response from Databricks LLM API
            if "choices" in result:
                content = result["choices"][0]["message"]["content"]
            elif "predictions" in result:
                content = result["predictions"][0]
            else:
                content = str(result)

            return content.strip()

        except requests.exceptions.RequestException as e:
            error_detail = str(e)
            try:
                # Try to get more details from response
                if hasattr(e, 'response') and e.response is not None:
                    error_detail = f"{str(e)} - Response: {e.response.text}"
            except:
                pass
            print(f"Error calling Databricks LLM API: {error_detail}")
            return (
                f'{{"message": "Error getting AI response: {str(e)}", "filters": {{}}}}'
            )
        except Exception as e:
            print(f"AI Response Exception: {e}")
            return (
                f'{{"message": "Error getting AI response: {str(e)}", "filters": {{}}}}'
            )

    def _parse_ai_response(
        self, ai_response: str, current_filters: Dict[str, Any]
    ) -> Tuple[str, Dict[str, Any]]:
        """
        Parse AI response to extract message and filter suggestions.

        Args:
            ai_response (str): Raw AI response
            current_filters (Dict): Current filter state

        Returns:
            Tuple of (message, suggested_filters)
        """
        try:
            # Try to extract JSON from the response
            response_text = ai_response.strip()

            # Find JSON content (sometimes AI adds extra text)
            start_idx = response_text.find("{")
            end_idx = response_text.rfind("}") + 1

            if start_idx >= 0 and end_idx > start_idx:
                json_text = response_text[start_idx:end_idx]
                parsed_response = json.loads(json_text)

                message = parsed_response.get("message", "I can help you find cars.")
                suggested_filters = parsed_response.get("filters", {})

                # Merge suggested filters with current filters
                final_filters = current_filters.copy()

                for filter_key, filter_value in suggested_filters.items():
                    if filter_value is not None:
                        final_filters[filter_key] = filter_value

                return message, final_filters
            else:
                # Fallback if JSON parsing fails
                return ai_response, current_filters

        except json.JSONDecodeError:
            print(f"Failed to parse JSON response: {ai_response}")
            return ai_response, current_filters
        except Exception as e:
            print(f"Error parsing AI response: {e}")
            return f"Error processing response: {str(e)}", current_filters
