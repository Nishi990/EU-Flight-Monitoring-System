import requests
import json
import mysql.connector
from datetime import datetime
import time
import os
from dotenv import load_dotenv

# Load environment variables from .env file 
load_dotenv()

# Configuration
DB_CONFIG = {
    'host': 'localhost',
    'user': os.getenv('DB_USER', 'root'),
    'password': os.getenv('DB_PASSWORD', ''),
    'database': 'eu_flight_monitoring'
}
# Simulated API key (in real implementation, store this securely)
API_KEY = os.getenv('AVIATION_API_KEY', '9e0c1f117551420d13a2fe88781c56d0')

def connect_to_database():
    """Establish connection to the MySQL database"""
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        return conn
    except mysql.connector.Error as e:
        print(f"Error connecting to database: {e}")
        return None

def fetch_flight_data(airport_code):
    """
    Simulate fetching flight data from an API
    In a real implementation, this would call an actual API like AviationStack
    """
    print(f"Fetching flight data for airport: {airport_code}")
    
    # Simulated API endpoint - in reality we would use something like:
    # url = f"http://api.aviationstack.com/v1/flights?access_key={API_KEY}&dep_iata={airport_code}"
    
    # For simulation, we'll create mock data instead of actual API call
    mock_response = {
        "data": [
            {
                "flight": {
                    "iata": f"LH{1000 + i}",
                    "icao": f"DLH{1000 + i}"
                },
                "airline": {
                    "name": "Lufthansa",
                    "iata": "LH",
                    "icao": "DLH"
                },
                "departure": {
                    "airport": "Frankfurt Airport",
                    "iata": airport_code,
                    "scheduled": "2025-03-21T08:00:00+00:00",
                    "actual": "2025-03-21T08:15:00+00:00",
                    "delay": 15
                },
                "arrival": {
                    "airport": "Munich Airport",
                    "iata": "MUC",
                    "scheduled": "2025-03-21T09:15:00+00:00",
                    "actual": "2025-03-21T09:40:00+00:00",
                    "delay": 25
                },
                "status": "active" if i % 3 != 0 else "delayed"
            }
            for i in range(5)  # Generate 5 mock flights
        ]
    }
    
    # In real implementation:
    # response = requests.get(url)
    # return response.json()
    
    return mock_response

def process_airline(cursor, airline_data):
    """Process airline data and insert if not exists"""
    airline_name = airline_data['name']
    airline_iata = airline_data['iata']
    airline_icao = airline_data['icao']
    
    # Check if airline exists
    cursor.execute("SELECT airline_id FROM airlines WHERE iata_code = %s", (airline_iata,))
    result = cursor.fetchone()
    
    if result:
        return result[0]
    else:
        # Insert new airline
        cursor.execute(
            "INSERT INTO airlines (name, iata_code, icao_code) VALUES (%s, %s, %s)",
            (airline_name, airline_iata, airline_icao)
        )
        return cursor.lastrowid

def process_airport(cursor, airport_name, airport_iata):
    """Process airport data and return airport_id"""
    cursor.execute("SELECT airport_id FROM airports WHERE iata_code = %s", (airport_iata,))
    result = cursor.fetchone()
    
    if result:
        return result[0]
    else:
        # In a real implementation, you would fetch full airport details
        # For this simulation, we'll insert with minimal info
        cursor.execute(
            "INSERT INTO airports (name, iata_code, country, city) VALUES (%s, %s, 'Unknown', 'Unknown')",
            (airport_name, airport_iata)
        )
        return cursor.lastrowid

def process_flight_data(flight_data):
    """Process flight data and store in database"""
    conn = connect_to_database()
    if not conn:
        return
    
    cursor = conn.cursor()
    
    try:
        for flight in flight_data['data']:
            # Process airline
            airline_id = process_airline(cursor, flight['airline'])
            
            # Process airports
            dep_airport_id = process_airport(cursor, flight['departure']['airport'], flight['departure']['iata'])
            arr_airport_id = process_airport(cursor, flight['arrival']['airport'], flight['arrival']['iata'])
            
            # Parse dates
            scheduled_departure = datetime.fromisoformat(flight['departure']['scheduled'].replace('Z', '+00:00'))
            scheduled_arrival = datetime.fromisoformat(flight['arrival']['scheduled'].replace('Z', '+00:00'))
            
            # Check if flight exists
            flight_number = flight['flight']['iata']
            cursor.execute("SELECT flight_id FROM flights WHERE flight_number = %s AND scheduled_departure = %s", 
                          (flight_number, scheduled_departure))
            flight_result = cursor.fetchone()
            
            if flight_result:
                flight_id = flight_result[0]
            else:
                # Insert new flight
                cursor.execute(
                    """
                    INSERT INTO flights 
                    (flight_number, airline_id, departure_airport_id, arrival_airport_id, 
                     scheduled_departure, scheduled_arrival)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    """,
                    (flight_number, airline_id, dep_airport_id, arr_airport_id, 
                     scheduled_departure, scheduled_arrival)
                )
                flight_id = cursor.lastrowid
            
            # Process flight status
            actual_departure = datetime.fromisoformat(flight['departure']['actual'].replace('Z', '+00:00'))
            actual_arrival = datetime.fromisoformat(flight['arrival']['actual'].replace('Z', '+00:00'))
            delay_minutes = flight['departure']['delay']
            
            status = 'Delayed' if delay_minutes > 0 else 'On Time'
            
            # Check if status exists
            cursor.execute("SELECT status_id FROM flight_status WHERE flight_id = %s", (flight_id,))
            status_result = cursor.fetchone()
            
            if status_result:
                # Update existing status
                cursor.execute(
                    """
                    UPDATE flight_status 
                    SET actual_departure = %s, actual_arrival = %s, status = %s, delay_minutes = %s
                    WHERE flight_id = %s
                    """,
                    (actual_departure, actual_arrival, status, delay_minutes, flight_id)
                )
            else:
                # Insert new status
                cursor.execute(
                    """
                    INSERT INTO flight_status 
                    (flight_id, actual_departure, actual_arrival, status, delay_minutes)
                    VALUES (%s, %s, %s, %s, %s)
                    """,
                    (flight_id, actual_departure, actual_arrival, status, delay_minutes)
                )
        
        # Commit the transaction
        conn.commit()
        print(f"Successfully processed {len(flight_data['data'])} flights")
        
    except Exception as e:
        conn.rollback()
        print(f"Error processing flight data: {e}")
    finally:
        cursor.close()
        conn.close()

def monitor_delayed_flights():
    """Identify and report flights delayed by more than 2 hours"""
    conn = connect_to_database()
    if not conn:
        return
    
    cursor = conn.cursor(dictionary=True)
    
    try:
        cursor.execute("""
            SELECT f.flight_number, al.name AS airline, a1.name AS departure_airport, 
                   a2.name AS arrival_airport, f.scheduled_departure, fs.actual_departure, 
                   fs.delay_minutes, fs.delay_reason
            FROM flights f
            JOIN airlines al ON f.airline_id = al.airline_id
            JOIN airports a1 ON f.departure_airport_id = a1.airport_id
            JOIN airports a2 ON f.arrival_airport_id = a2.airport_id
            JOIN flight_status fs ON f.flight_id = fs.flight_id
            WHERE fs.delay_minutes > 120
        """)
        
        delayed_flights = cursor.fetchall()
        
        if delayed_flights:
            print("\n===== FLIGHTS DELAYED BY MORE THAN 2 HOURS =====")
            for flight in delayed_flights:
                print(f"Flight: {flight['flight_number']} - {flight['airline']}")
                print(f"Route: {flight['departure_airport']} → {flight['arrival_airport']}")
                print(f"Scheduled: {flight['scheduled_departure']}")
                print(f"Actual: {flight['actual_departure']}")
                print(f"Delay: {flight['delay_minutes']} minutes")
                if flight['delay_reason']:
                    print(f"Reason: {flight['delay_reason']}")
                print("-" * 50)
        else:
            print("No flights delayed by more than 2 hours")
            
    except Exception as e:
        print(f"Error monitoring delayed flights: {e}")
    finally:
        cursor.close()
        conn.close()

def main():
    """Main function to demonstrate the flight data collection process"""
    
    # List of airports to monitor
    airports = ['FRA', 'MUC', 'BER', 'HAM', 'DUS']
    
    print("=== EU Flight Monitoring System ===\n")
    print("Starting data collection process...\n")
    
    for airport in airports:
        # Fetch flight data from API
        flight_data = fetch_flight_data(airport)
        
        # Process and store the data
        process_flight_data(flight_data)
        
        # Simulate time between API calls to avoid rate limiting
        time.sleep(1)
    
    # Check for delayed flights
    monitor_delayed_flights()
    
    print("\nData collection complete!")

if __name__ == "__main__":
    main()