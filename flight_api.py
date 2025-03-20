from flask import Flask, jsonify, request
import mysql.connector
from mysql.connector import Error
import os
from dotenv import load_dotenv
from datetime import datetime, timedelta
import logging

# Load environment variables
load_dotenv()

app = Flask(__name__)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Database configuration
DB_CONFIG = {
    'host': 'localhost',
    'user': os.getenv('DB_USER', 'root'),
    'password': os.getenv('DB_PASSWORD', ''),
    'database': 'eu_flight_monitoring'
}

def get_db_connection():
    """Create a database connection"""
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        return conn
    except Error as e:
        logger.error(f"Database connection error: {e}")
        return None

@app.route('/api/health', methods=['GET'])
def health_check():
    """API health check endpoint"""
    return jsonify({"status": "healthy", "timestamp": datetime.now().isoformat()})

@app.route('/api/airports', methods=['GET'])
def get_airports():
    """Get all airports or filter by country"""
    country = request.args.get('country')
    
    conn = get_db_connection()
    if not conn:
        return jsonify({"error": "Database connection failed"}), 500
    
    cursor = conn.cursor(dictionary=True)
    
    try:
        if country:
            cursor.execute("SELECT * FROM airports WHERE country = %s", (country,))
        else:
            cursor.execute("SELECT * FROM airports")
            
        airports = cursor.fetchall()
        return jsonify({"airports": airports})
    except Error as e:
        logger.error(f"Error fetching airports: {e}")
        return jsonify({"error": str(e)}), 500
    finally:
        cursor.close()
        conn.close()

@app.route('/api/flights', methods=['GET'])
def get_flights():
    """Get flights with various filter options"""
    # Get query parameters
    departure = request.args.get('departure')  # IATA code
    arrival = request.args.get('arrival')      # IATA code
    airline = request.args.get('airline')      # IATA code
    date = request.args.get('date')            # YYYY-MM-DD
    status = request.args.get('status')        # On Time, Delayed, Cancelled
    
    conn = get_db_connection()
    if not conn:
        return jsonify({"error": "Database connection failed"}), 500
    
    cursor = conn.cursor(dictionary=True)
    
    try:
        query = """
            SELECT f.flight_id, f.flight_number, 
                   al.name as airline_name, al.iata_code as airline_code,
                   a1.name as departure_airport, a1.iata_code as departure_code, 
                   a2.name as arrival_airport, a2.iata_code as arrival_code,
                   f.scheduled_departure, f.scheduled_arrival,
                   fs.status, fs.delay_minutes, fs.actual_departure, fs.actual_arrival
            FROM flights f
            JOIN airlines al ON f.airline_id = al.airline_id
            JOIN airports a1 ON f.departure_airport_id = a1.airport_id
            JOIN airports a2 ON f.arrival_airport_id = a2.airport_id
            LEFT JOIN flight_status fs ON f.flight_id = fs.flight_id
            WHERE 1=1
        """
        
        params = []
        
        if departure:
            query += " AND a1.iata_code = %s"
            params.append(departure)
            
        if arrival:
            query += " AND a2.iata_code = %s"
            params.append(arrival)
            
        if airline:
            query += " AND al.iata_code = %s"
            params.append(airline)
            
        if date:
            query += " AND DATE(f.scheduled_departure) = %s"
            params.append(date)
            
        if status:
            query += " AND fs.status = %s"
            params.append(status)
        
        cursor.execute(query, params)
        flights = cursor.fetchall()
        
        # Format datetime objects for JSON
        for flight in flights:
            for key, value in flight.items():
                if isinstance(value, datetime):
                    flight[key] = value.isoformat()
        
        return jsonify({"flights": flights, "count": len(flights)})
    except Error as e:
        logger.error(f"Error fetching flights: {e}")
        return jsonify({"error": str(e)}), 500
    finally:
        cursor.close()
        conn.close()

@app.route('/api/flights/delayed', methods=['GET'])
def get_delayed_flights():
    """Get flights delayed by more than 2 hours"""
    hours = request.args.get('hours', 2, type=int)
    
    conn = get_db_connection()
    if not conn:
        return jsonify({"error": "Database connection failed"}), 500
    
    cursor = conn.cursor(dictionary=True)
    
    try:
        query = """
            SELECT f.flight_id, f.flight_number, 
                   al.name as airline_name, al.iata_code as airline_code,
                   a1.name as departure_airport, a1.iata_code as departure_code, 
                   a2.name as arrival_airport, a2.iata_code as arrival_code,
                   f.scheduled_departure, f.scheduled_arrival,
                   fs.status, fs.delay_minutes, fs.actual_departure, fs.actual_arrival,
                   fs.delay_reason
            FROM flights f
            JOIN airlines al ON f.airline_id = al.airline_id
            JOIN airports a1 ON f.departure_airport_id = a1.airport_id
            JOIN airports a2 ON f.arrival_airport_id = a2.airport_id
            JOIN flight_status fs ON f.flight_id = fs.flight_id
            WHERE fs.delay_minutes > %s
            ORDER BY fs.delay_minutes DESC
        """
        
        cursor.execute(query, (hours * 60,))
        flights = cursor.fetchall()
        
        # Format datetime objects for JSON
        for flight in flights:
            for key, value in flight.items():
                if isinstance(value, datetime):
                    flight[key] = value.isoformat()
        
        return jsonify({
            "flights": flights, 
            "count": len(flights),
            "minimum_delay": f"{hours} hours"
        })
    except Error as e:
        logger.error(f"Error fetching delayed flights: {e}")
        return jsonify({"error": str(e)}), 500
    finally:
        cursor.close()
        conn.close()

@app.route('/api/flight/<flight_number>', methods=['GET'])
def get_flight_details(flight_number):
    """Get details for a specific flight number"""
    date = request.args.get('date')  # Optional date filter
    
    conn = get_db_connection()
    if not conn:
        return jsonify({"error": "Database connection failed"}), 500
    
    cursor = conn.cursor(dictionary=True)
    
    try:
        query = """
            SELECT f.flight_id, f.flight_number, 
                   al.name as airline_name, al.iata_code as airline_code,
                   a1.name as departure_airport, a1.iata_code as departure_code, 
                   a1.country as departure_country, a1.city as departure_city,
                   a2.name as arrival_airport, a2.iata_code as arrival_code,
                   a2.country as arrival_country, a2.city as arrival_city,
                   f.scheduled_departure, f.scheduled_arrival,
                   fs.status, fs.delay_minutes, fs.actual_departure, fs.actual_arrival,
                   fs.delay_reason
            FROM flights f
            JOIN airlines al ON f.airline_id = al.airline_id
            JOIN airports a1 ON f.departure_airport_id = a1.airport_id
            JOIN airports a2 ON f.arrival_airport_id = a2.airport_id
            LEFT JOIN flight_status fs ON f.flight_id = fs.flight_id
            WHERE f.flight_number = %s
        """
        
        params = [flight_number]
        
        if date:
            query += " AND DATE(f.scheduled_departure) = %s"
            params.append(date)
        
        cursor.execute(query, params)
        flight = cursor.fetchone()
        
        if not flight:
            return jsonify({"error": "Flight not found"}), 404
        
        # Format datetime objects for JSON
        for key, value in flight.items():
            if isinstance(value, datetime):
                flight[key] = value.isoformat()
        
        return jsonify({"flight": flight})
    except Error as e:
        logger.error(f"Error fetching flight details: {e}")
        return jsonify({"error": str(e)}), 500
    finally:
        cursor.close()
        conn.close()

@app.errorhandler(404)
def not_found(error):
    return jsonify({"error": "Endpoint not found"}), 404

@app.errorhandler(500)
def server_error(error):
    return jsonify({"error": "Internal server error"}), 500

if __name__ == '__main__':
    app.run(debug=True, port=5000)