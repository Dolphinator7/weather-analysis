import requests
import pandas as pd
from dotenv import load_dotenv
import os

# Load variables from .env file
load_dotenv()

# Access the environment variables
API_KEY = os.getenv("API_KEY")
BASE_URL = os.getenv("BASE_URL")

def fetch_weather(city):
    url = f"{BASE_URL}?q={city}&appid={API_KEY}&units=metric"
    response = requests.get(url)
    
    if response.status_code == 200:
        data = response.json()
        weather_info = {
            "City": city,
            "Temperature (°C)": data["main"]["temp"],
            "Humidity (%)": data["main"]["humidity"],
            "Condition": data["weather"][0]["description"],
        }
        return weather_info
    else:
        print(f"Failed to fetch weather data for {city}. Error: {response.status_code}")
        return None

def save_to_csv(weather_data, filename="weather_data.csv"):
    df = pd.DataFrame(weather_data)
    df.to_csv(filename, index=False)
    print(f"Data saved to {filename}")

if __name__ == "__main__":
    cities = ["Port Harcourt", "Lagos", "Abuja"]  # Add more cities if needed
    weather_data = []

    for city in cities:
        data = fetch_weather(city)
        if data:
            weather_data.append(data)
    
    save_to_csv(weather_data)
