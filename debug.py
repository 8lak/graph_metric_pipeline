import os
from dotenv import load_dotenv

# This will explicitly tell you if it found the file.
# It returns True if the file was found and loaded, False otherwise.
env_path = os.path.join(os.path.dirname(__file__), '.env')
found = load_dotenv(dotenv_path=env_path)

print(f"Was the .env file found? {found}")

# Now, let's try to get the variable
api_key = os.getenv("LIBRARIES_IO_API_KEY")

print(f"The value for LIBRARIES_IO_API_KEY is: '{api_key}'")

if api_key:
    print("Success! The key was loaded.")
else:
    print("Failure. The key was NOT loaded. Check file location, name, and content.")