import os
from dotenv import load_dotenv
from google.cloud import aiplatform
# Load environment variables from .env file
load_dotenv()





try:
    # --- HARDCODE YOUR VALUES FOR THIS TEST ---
    # Paste them directly from the GCP console here.
    ENDPOINT_ID = os.getenv("ENDPOINT_ID")
    PROJECT_ID = os.getenv("PROJECT_ID")
    REGION = os.getenv("REGION")
    # ---------------------------------------------

    print("--- RUNNING WITH HARDCODED VALUES ---")
    print(f"PROJECT_ID: {PROJECT_ID}")
    print(f"REGION: {REGION}")
    print(f"ENDPOINT_ID: {ENDPOINT_ID}")
    print("-----------------------------------")

    aiplatform.init(project=PROJECT_ID, location=REGION)
    endpoint = aiplatform.Endpoint(endpoint_name=ENDPOINT_ID)
    print("Successfully connected to Vertex AI endpoint.")

except Exception as e:
    exit(f"Error configuring the Gemini API: {e}")

   