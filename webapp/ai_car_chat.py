"""AI assistant for answering questions about specific cars."""
import os
import requests
from databricks.sdk import WorkspaceClient


class AICarChat:
    """AI assistant for car-specific questions using Databricks LLM."""

    def __init__(
        self,
        endpoint: str = "databricks-meta-llama-3-1-70b-instruct",
        workspace_host: str = None,
    ):
        """
        Initialize the AI car chat assistant.

        Args:
            endpoint: Databricks LLM serving endpoint name
            workspace_host: Databricks workspace host URL (from env if not provided)
        """
        self.endpoint = endpoint
        self.workspace_host = workspace_host or os.getenv("DATABRICKS_HOST")
        self.workspace_client = WorkspaceClient()
        self.api_url = (
            f"{self.workspace_host}/serving-endpoints/{endpoint}/invocations"
        )

    def ask_about_car(self, user_question: str, car_data: dict) -> str:
        """
        Ask a question about a specific car.

        Args:
            user_question: The user's question about the car
            car_data: Dictionary containing car information

        Returns:
            AI response as a string
        """
        # Format car data as context
        car_context = self._format_car_data(car_data)

        # Create system prompt for car-specific chat
        system_prompt = f"""You are a helpful AI assistant that answers questions about a specific vehicle.

You have access to the following vehicle information:

{car_context}

Your role is to:
- Answer user questions about this specific vehicle based on the provided data
- Provide clear, concise, and helpful responses
- If the user asks about information not available in the data, politely say you don't have that information
- Use natural language and be conversational
- Focus only on this vehicle - do not suggest filter changes or search for other vehicles

IMPORTANT: Do NOT output JSON or structured data. Simply respond naturally to the user's question."""

        # Prepare headers
        headers = {"Content-Type": "application/json"}

        # Get authentication token from Databricks SDK
        if self.workspace_client:
            try:
                auth_headers = self.workspace_client.config.authenticate()
                if isinstance(auth_headers, dict) and "Authorization" in auth_headers:
                    headers["Authorization"] = auth_headers["Authorization"]
                else:
                    print(f"Unexpected auth response: {auth_headers}")
            except Exception as e:
                print(f"Warning: Could not get authentication token: {e}")

        # Prepare payload
        payload = {
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_question},
            ],
            "max_tokens": 1024,
            "temperature": 0.3,
        }

        print(f"Sending car chat request to: {self.api_url}")

        try:
            # Call Databricks LLM API
            response = requests.post(
                self.api_url, headers=headers, json=payload, timeout=60
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
                if hasattr(e, "response") and e.response is not None:
                    error_detail = f"{str(e)} - Response: {e.response.text}"
            except:
                pass
            print(f"Error calling Databricks LLM API: {error_detail}")
            return f"I'm sorry, I encountered an error processing your question: {str(e)}"

        except Exception as e:
            print(f"AI Response Exception: {e}")
            return f"I'm sorry, I encountered an error processing your question: {str(e)}"

    def _format_car_data(self, car_data: dict) -> str:
        """Format car data as readable text for the AI."""
        lines = []
        for key, value in car_data.items():
            if value and str(value).strip() and value != "Niet geregistreerd":
                # Convert snake_case to Title Case
                readable_key = key.replace("_", " ").title()
                lines.append(f"- {readable_key}: {value}")

        return "\n".join(lines) if lines else "No detailed information available"
