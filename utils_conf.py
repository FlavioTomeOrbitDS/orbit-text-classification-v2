from dotenv import load_dotenv, set_key
import os

def saveApiKey(api_name, api_key):    
    # Define the path to your .env file
    env_path = 'conf/.env'
    # Load existing environment variables file or create one if it doesn't exist
    load_dotenv(env_path)
    # API Key to be saved
    api_key = api_key

    # Save the API key in the .env file
    # This function will create the key if it does not exist or update the value if it does
    set_key(env_path, api_name, api_key)

def get_api_key(api_name):
    # Define the path to your .env file
    env_path = 'conf/.env'
    # Load the environment variables from the .env file
    load_dotenv(env_path)
    # Retrieve the API key
    api_key = os.getenv(api_name)

    return api_key

