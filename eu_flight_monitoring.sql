CREATE DATABASE eu_flight_monitoring;
USE eu_flight_monitoring;
-- Airports table
CREATE TABLE airports (
    airport_id INT PRIMARY KEY AUTO_INCREMENT,
    name VARCHAR(100) NOT NULL,
    iata_code CHAR(3) UNIQUE,
    icao_code CHAR(4) UNIQUE,
    country VARCHAR(50) NOT NULL,
    city VARCHAR(50) NOT NULL,
    latitude DECIMAL(10, 8),
    longitude DECIMAL(11, 8)
);
-- Airlines table
CREATE TABLE airlines (
    airline_id INT PRIMARY KEY AUTO_INCREMENT,
    name VARCHAR(100) NOT NULL,
    iata_code CHAR(2) UNIQUE,
    icao_code CHAR(3) UNIQUE
);
-- Flights table
CREATE TABLE flights (
    flight_id INT PRIMARY KEY AUTO_INCREMENT,
    flight_number VARCHAR(10) NOT NULL,
    airline_id INT,
    departure_airport_id INT,
    arrival_airport_id INT,
    scheduled_departure DATETIME,
    scheduled_arrival DATETIME,
    FOREIGN KEY (airline_id) REFERENCES airlines(airline_id),
    FOREIGN KEY (departure_airport_id) REFERENCES airports(airport_id),
    FOREIGN KEY (arrival_airport_id) REFERENCES airports(airport_id)
);
-- Flight status table
CREATE TABLE flight_status (
    status_id INT PRIMARY KEY AUTO_INCREMENT,
    flight_id INT,
    actual_departure DATETIME,
    actual_arrival DATETIME,
    status ENUM('On Time', 'Delayed', 'Cancelled', 'Diverted') DEFAULT 'On Time',
    delay_minutes INT DEFAULT 0,
    delay_reason VARCHAR(255),
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    FOREIGN KEY (flight_id) REFERENCES flights(flight_id)
);
-- Insert 5 German airports
INSERT INTO airports (name, iata_code, icao_code, country, city, latitude, longitude) VALUES
('Frankfurt Airport', 'FRA', 'EDDF', 'Germany', 'Frankfurt', 50.0379, 8.5622),
('Munich Airport', 'MUC', 'EDDM', 'Germany', 'Munich', 48.3537, 11.7860),
('Berlin Brandenburg Airport', 'BER', 'EDDB', 'Germany', 'Berlin', 52.3667, 13.5033),
('Hamburg Airport', 'HAM', 'EDDH', 'Germany', 'Hamburg', 53.6304, 10.0060),
('Düsseldorf Airport', 'DUS', 'EDDL', 'Germany', 'Düsseldorf', 51.2895, 6.7668);
-- Insert sample airlines
INSERT INTO airlines (name, iata_code, icao_code) VALUES
('Lufthansa', 'LH', 'DLH'),
('Eurowings', 'EW', 'EWG'),
('Air France', 'AF', 'AFR'),
('British Airways', 'BA', 'BAW');
-- Insert sample flights
INSERT INTO flights (flight_number, airline_id, departure_airport_id, arrival_airport_id, scheduled_departure, scheduled_arrival) VALUES
('LH1234', 1, 1, 2, '2025-03-20 08:00:00', '2025-03-20 09:00:00'),
('LH5678', 1, 2, 1, '2025-03-20 10:00:00', '2025-03-20 11:00:00'),
('EW2345', 2, 3, 4, '2025-03-20 09:30:00', '2025-03-20 10:30:00'),
('EW6789', 2, 4, 3, '2025-03-20 12:00:00', '2025-03-20 13:00:00'),
('LH8765', 1, 5, 1, '2025-03-20 14:00:00', '2025-03-20 15:00:00'),
('AF1122', 3, 1, 5, '2025-03-20 16:00:00', '2025-03-20 17:00:00'),
('BA3344', 4, 2, 3, '2025-03-20 18:00:00', '2025-03-20 19:00:00'),
('LH9900', 1, 3, 5, '2025-03-20 08:30:00', '2025-03-20 09:45:00'),
('EW1100', 2, 4, 2, '2025-03-20 11:15:00', '2025-03-20 12:30:00'),
('AF2233', 3, 5, 4, '2025-03-20 13:45:00', '2025-03-20 15:00:00');
-- Insert sample flight statuses (some on time, some delayed)
INSERT INTO flight_status (flight_id, actual_departure, actual_arrival, status, delay_minutes, delay_reason) VALUES
(1, '2025-03-20 08:05:00', '2025-03-20 09:10:00', 'On Time', 10, NULL),
(2, '2025-03-20 10:15:00', '2025-03-20 11:20:00', 'Delayed', 20, 'Air Traffic Control'),
(3, '2025-03-20 09:45:00', '2025-03-20 10:50:00', 'Delayed', 20, 'Technical Issue'),
(4, '2025-03-20 14:30:00', '2025-03-20 15:40:00', 'Delayed', 150, 'Weather Conditions'),
(5, '2025-03-20 14:10:00', '2025-03-20 15:15:00', 'On Time', 15, NULL),
(6, '2025-03-20 18:30:00', '2025-03-20 19:45:00', 'Delayed', 150, 'Aircraft Late Arrival'),
(7, '2025-03-20 18:10:00', '2025-03-20 19:15:00', 'On Time', 15, NULL),
(8, '2025-03-20 08:35:00', '2025-03-20 09:55:00', 'On Time', 10, NULL),
(9, '2025-03-20 11:20:00', '2025-03-20 12:40:00', 'On Time', 10, NULL),
(10, '2025-03-20 16:00:00', '2025-03-20 17:30:00', 'Delayed', 135, 'Crew Availability');

-- 1. Retrieve all flights from a specific airport (e.g., Frankfurt Airport)
SELECT f.flight_number, a1.name AS departure_airport, a2.name AS arrival_airport, 
       f.scheduled_departure, f.scheduled_arrival, fs.status, fs.delay_minutes
FROM flights f
JOIN airports a1 ON f.departure_airport_id = a1.airport_id
JOIN airports a2 ON f.arrival_airport_id = a2.airport_id
LEFT JOIN flight_status fs ON f.flight_id = fs.flight_id
WHERE a1.iata_code = 'FRA';
-- 2. Identify flights delayed by more than 2 hours (120 minutes)
SELECT f.flight_number, al.name AS airline, a1.name AS departure_airport, 
       a2.name AS arrival_airport, f.scheduled_departure, fs.actual_departure, 
       fs.delay_minutes, fs.delay_reason
FROM flights f
JOIN airlines al ON f.airline_id = al.airline_id
JOIN airports a1 ON f.departure_airport_id = a1.airport_id
JOIN airports a2 ON f.arrival_airport_id = a2.airport_id
JOIN flight_status fs ON f.flight_id = fs.flight_id
WHERE fs.delay_minutes > 120;
-- 3. Fetch flight details using the flight number
SELECT f.flight_number, al.name AS airline, a1.name AS departure_airport, 
       a1.iata_code AS departure_code, a2.name AS arrival_airport, 
       a2.iata_code AS arrival_code, f.scheduled_departure, f.scheduled_arrival, 
       fs.status, fs.delay_minutes, fs.delay_reason
FROM flights f
JOIN airlines al ON f.airline_id = al.airline_id
JOIN airports a1 ON f.departure_airport_id = a1.airport_id
JOIN airports a2 ON f.arrival_airport_id = a2.airport_id
LEFT JOIN flight_status fs ON f.flight_id = fs.flight_id
WHERE f.flight_number = 'LH1234';


