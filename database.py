import logging
from dotenv import load_dotenv
import mysql.connector
from mysql.connector import pooling
import os


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)



# Database configuration for initial connection (without database)
load_dotenv()

INIT_CONFIG = {
    "host": "127.0.0.1",
    "port": 3306,
    "user": "root",
    "password": os.getenv("MYSQL_PASSWORD")
}

DB_CONFIG = {
    **INIT_CONFIG,
    "database": "voting_system",
    "pool_name": "mypool",
    "pool_size": 5
}


def initialize_database():
    """Create the database if it doesn't exist"""
    try:
        # Connect without database first
        conn = mysql.connector.connect(**INIT_CONFIG)
        cursor = conn.cursor()
        
        # Create database if it doesn't exist
        cursor.execute("CREATE DATABASE IF NOT EXISTS voting_system")
        conn.commit()
        
        logger.info("Database 'voting_system' initialized successfully")
    except mysql.connector.Error as err:
        logger.error(f"Failed to initialize database: {err}")
        raise
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()

def get_connection():
    """Get a connection from the connection pool"""
    try:
        if not hasattr(get_connection, 'pool'):
            get_connection.pool = mysql.connector.pooling.MySQLConnectionPool(**DB_CONFIG)
        return get_connection.pool.get_connection()
    except mysql.connector.Error as err:
        logger.error(f"Failed to connect to database: {err}")
        raise

def connect():
    """Initialize database tables if they don't exist"""
    connection = None
    try:
        connection = get_connection()
        cursor = connection.cursor()
        
        # Create admin table
        cursor.execute("""CREATE TABLE IF NOT EXISTS admin (
                            id INT AUTO_INCREMENT PRIMARY KEY,
                            registration_id VARCHAR(20) NOT NULL UNIQUE, 
                            name VARCHAR(50) NOT NULL,
                            aadhar BIGINT NOT NULL UNIQUE,
                            phone VARCHAR(10) NOT NULL,
                            gender VARCHAR(10) NOT NULL
                        )""")
        
        # Create voters table
        cursor.execute("""CREATE TABLE IF NOT EXISTS voters (
                            id INT AUTO_INCREMENT PRIMARY KEY,
                            voter_id VARCHAR(20) NOT NULL UNIQUE,
                            name VARCHAR(50) NOT NULL,
                            aadhar VARCHAR(12) NOT NULL UNIQUE,
                            phone VARCHAR(10) NOT NULL,
                            gender VARCHAR(10) NOT NULL
                        )""")
        
        # Create vote table
        cursor.execute("""CREATE TABLE IF NOT EXISTS vote (
                            id INT AUTO_INCREMENT PRIMARY KEY,
                            voter_id VARCHAR(20) NOT NULL UNIQUE,
                            poll VARCHAR(50) NOT NULL,
                            district VARCHAR(50) NOT NULL
                        )""")
        
        connection.commit()
        logger.info("Database tables initialized successfully")
        
    except mysql.connector.Error as err:
        logger.error(f"Failed to initialize database: {err}")
        raise
    finally:
        if connection:
            connection.close()

# Initialize database on module import
try:
    initialize_database()
except Exception as e:
    logger.error(f"Database initialization failed: {e}")

try:
    connect()
except Exception as e:
    logger.error(f"Failed to initialize database: {e}")


def findByAadhar(aadhar):
    try:
        connection = get_connection()
        cursor = connection.cursor(prepared=True)
        sql = "SELECT  * FROM voters WHERE aadhar=%s"
        cursor.execute(sql, (aadhar,))
        result = cursor.fetchone()
        connection.close()
        return result
    except mysql.connector.Error as err:
        logger.error(f"[ERROR] Database error: {err}")
        return None
    except Exception as e:
        logger.error(f"[ERROR] Unexpected error: {e}")
        return None


def findByVoterId(voterId):
    try:
        connection = get_connection()
        cursor = connection.cursor(prepared=True)
        sql = "SELECT  * FROM voters WHERE voter_id=%s"
        cursor.execute(sql, (voterId,))
        result = cursor.fetchone()
        connection.close()
        return result
    except mysql.connector.Error as err:
        logger.error(f"[ERROR] Database error: {err}")
        return None
    except Exception as e:
        logger.error(f"[ERROR] Unexpected error: {e}")
        return None


def addVoter(voterId, name, aadhar, phone, gender):
    try:
        connection = get_connection()
        cursor = connection.cursor(prepared=True)
        sql = "INSERT INTO voters(voter_id, name, aadhar, phone, gender) VALUES(%s, %s, %s, %s, %s)"
        cursor.execute(sql, (voterId, name, aadhar, phone, gender))
        connection.commit()
        connection.close()
        return True
    except mysql.connector.Error as err:
        logger.error(f"[ERROR] Database error: {err}")
        return False
    except Exception as e:
        logger.error(f"[ERROR] Unexpected error: {e}")
        return False


def submitVote(voterId, poll, district):
    try:
        connection = get_connection()
        cursor = connection.cursor(prepared=True)
        sql = "INSERT INTO vote(voter_id, poll, district) VALUES(%s, %s, %s)"
        cursor.execute(sql, (voterId, poll, district))
        connection.commit()
        connection.close()
        return True
    except mysql.connector.Error as err:
        logger.error(f"[ERROR] Database error: {err}")
        return False
    except Exception as e:
        logger.error(f"[ERROR] Unexpected error: {e}")
        return False


def findByVoterIdinVote(voterId):
    try:
        connection = get_connection()
        cursor = connection.cursor(prepared=True)
        sql = "SELECT  * FROM vote WHERE voter_id=%s"
        cursor.execute(sql, (voterId,))
        result = cursor.fetchone()
        connection.close()
        return result
    except mysql.connector.Error as err:
        logger.error(f"[ERROR] Database error: {err}")
        return None
    except Exception as e:
        logger.error(f"[ERROR] Unexpected error: {e}")
        return None


def findByRegId(regId):
    try:
        connection = get_connection()
        cursor = connection.cursor(prepared=True)
        sql = "SELECT  * FROM admin WHERE registration_id=%s"
        cursor.execute(sql, (regId,))
        result = cursor.fetchone()
        connection.close()
        return result
    except mysql.connector.Error as err:
        logger.error(f"[ERROR] Database error: {err}")
        return None
    except Exception as e:
        logger.error(f"[ERROR] Unexpected error: {e}")
        return None


def findByAadharinAdmin(aadhar):
    try:
        connection = get_connection()
        cursor = connection.cursor(prepared=True)
        sql = "SELECT  * FROM admin WHERE aadhar=%s"
        cursor.execute(sql, (aadhar,))
        result = cursor.fetchone()
        connection.close()
        return result
    except mysql.connector.Error as err:
        logger.error(f"[ERROR] Database error: {err}")
        return None
    except Exception as e:
        logger.error(f"[ERROR] Unexpected error: {e}")
        return None


def addAdmin(regId, name, aadhar, phone, gender):
    try:
        connection = get_connection()
        cursor = connection.cursor(prepared=True)
        sql = "INSERT INTO admin(registration_id, name, aadhar, phone, gender) VALUES(%s, %s, %s, %s, %s)"
        cursor.execute(sql, (regId, name, aadhar, phone, gender))
        connection.commit()
        connection.close()
        return True
    except mysql.connector.Error as err:
        logger.error(f"[ERROR] Database error: {err}")
        return False
    except Exception as e:
        logger.error(f"[ERROR] Unexpected error: {e}")
        return False


def getTotalCount():
    try:
        connection = get_connection()
        cursor = connection.cursor(prepared=True)
        sql = "SELECT count(*) FROM vote"
        cursor.execute(sql)
        result = cursor.fetchone()
        connection.close()
        return result
    except mysql.connector.Error as err:
        logger.error(f"[ERROR] Database error: {err}")
        return None
    except Exception as e:
        logger.error(f"[ERROR] Unexpected error: {e}")
        return None


def getTotalUserCount():
    try:
        connection = get_connection()
        cursor = connection.cursor(prepared=True)
        sql = "SELECT count(*) FROM voters"
        cursor.execute(sql)
        result = cursor.fetchone()
        connection.close()
        return result
    except mysql.connector.Error as err:
        logger.error(f"[ERROR] Database error: {err}")
        return None
    except Exception as e:
        logger.error(f"[ERROR] Unexpected error: {e}")
        return None


def getPartyCount(party):
    try:
        connection = get_connection()
        cursor = connection.cursor(prepared=True)
        sql = "SELECT count(*) FROM vote WHERE poll like %s"
        cursor.execute(sql, (f"%{party}%",))
        result = cursor.fetchall()
        connection.close()
        return result
    except mysql.connector.Error as err:
        logger.error(f"[ERROR] Database error: {err}")
        return None
    except Exception as e:
        logger.error(f"[ERROR] Unexpected error: {e}")
        return None


def getallVoters():
    try:
        connection = get_connection()
        cursor = connection.cursor(prepared=True)
        sql = """SELECT voters.name, voters.phone, voters.gender, vote.district
                FROM voters
                LEFT JOIN vote ON voters.voter_id=vote.voter_id"""
        cursor.execute(sql)
        result = cursor.fetchall()
        connection.close()
        return result
    except mysql.connector.Error as err:
        logger.error(f"[ERROR] Database error: {err}")
        return None
    except Exception as e:
        logger.error(f"[ERROR] Unexpected error: {e}")
        return None


def getUserByAadhar(aadhar):
    try:
        connection = get_connection()
        cursor = connection.cursor(prepared=True)
        sql = """SELECT voters.name, voters.phone, voters.gender, vote.district
                FROM voters
                LEFT JOIN vote ON voters.voter_id=vote.voter_id
                WHERE aadhar = %s"""
        cursor.execute(sql, (aadhar,))
        result = cursor.fetchone()
        connection.close()
        return result
    except mysql.connector.Error as err:
        logger.error(f"[ERROR] Database error: {err}")
        return None
    except Exception as e:
        logger.error(f"[ERROR] Unexpected error: {e}")
        return None


def updateUserByAadhar(name, phone, gender, aadhar):
    try:
        connection = get_connection()
        cursor = connection.cursor(prepared=True)
        sql = """UPDATE voters SET name=%s, phone=%s, gender=%s 
                WHERE aadhar=%s"""
        cursor.execute(sql, (name, phone, gender, aadhar))
        connection.commit()
        connection.close()
        return True
    except mysql.connector.Error as err:
        logger.error(f"[ERROR] Database error: {err}")
        return False
    except Exception as e:
        logger.error(f"[ERROR] Unexpected error: {e}")
        return False


def deleteUserByAadhar(aadhar):
    try:
        connection = get_connection()
        cursor = connection.cursor(prepared=True)
        sql = "DELETE FROM voters WHERE aadhar = %s"
        cursor.execute(sql, (aadhar,))
        affected_rows = cursor.rowcount
        connection.commit()
        connection.close()
        return affected_rows > 0
    except mysql.connector.Error as err:
        logger.error(f"[ERROR] Database error: {err}")
        return False
    except Exception as e:
        logger.error(f"[ERROR] Unexpected error: {e}")
        return False
