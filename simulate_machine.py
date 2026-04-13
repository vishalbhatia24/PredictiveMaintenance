import pyodbc
import random
from datetime import datetime
import os

# Load from environment variables (GitHub Secrets)
server = os.environ['DB_SERVER']
database = os.environ['DB_NAME']
username = os.environ['DB_USER']
password = os.environ['DB_PASS']

# Create connection
conn = pyodbc.connect(
    f"DRIVER={{ODBC Driver 17 for SQL Server}};"
    f"SERVER={server};"
    f"DATABASE={database};"
    f"UID={username};"
    f"PWD={password};"
    "Encrypt=yes;"
    "TrustServerCertificate=no;"
    "Connection Timeout=30;"
)

cursor = conn.cursor()

machines = ['M1', 'M2', 'M3']

def generate_data():
    return {
        "temp": random.uniform(20, 100),
        "vibration": random.uniform(0, 10),
        "pressure": random.uniform(1, 10)
    }

def check_failure(temp, vibration):
    if temp > 80 or vibration > 8:
        return "HIGH RISK", "Machine failure likely"
    elif temp > 60:
        return "MEDIUM RISK", "Monitor machine"
    else:
        return "LOW RISK", "Normal"

try:
    # Generate data
    m = random.choice(machines)
    data = generate_data()

    # Insert sensor data
    cursor.execute("""
        INSERT INTO MachineSensorData (MachineId, Temperature, Vibration, Pressure)
        VALUES (?, ?, ?, ?)
    """, m, data["temp"], data["vibration"], data["pressure"])

    # Check risk
    risk, msg = check_failure(data["temp"], data["vibration"])

    # Insert alert if needed
    if risk != "LOW RISK":
        cursor.execute("""
            INSERT INTO MachineAlerts (MachineId, AlertMessage, RiskLevel)
            VALUES (?, ?, ?)
        """, m, msg, risk)

    conn.commit()
    print("Data inserted successfully!")

except Exception as e:
    print("Error:", e)

finally:
    cursor.close()
    conn.close()
