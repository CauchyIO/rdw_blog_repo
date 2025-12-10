from databricks.connect import DatabricksSession
from pyspark.sql import SparkSession
import os
from pathlib import Path
from dotenv import load_dotenv

# Load .env file from project root (once at module import)
project_root = Path(__file__).parent.parent.parent.parent
env_file = project_root / ".env"
if env_file.exists():
    load_dotenv(env_file)

def get_spark_session() -> SparkSession:
    """Get or create a DatabricksSession.

    Returns a DatabricksSession for use with Databricks features.
    Uses the profile specified in the PROFILE environment variable from .env
    Uses serverless compute by default.
    """
    active = SparkSession.getActiveSession()
    if active is not None:
        return active

    # Get profile from environment variable
    profile = os.getenv("PROFILE")

    print(f"Using Databricks profile: {profile} (serverless)")

    # No active session, create a new one with serverless compute
    return DatabricksSession.builder.profile(profile).serverless(True).getOrCreate()