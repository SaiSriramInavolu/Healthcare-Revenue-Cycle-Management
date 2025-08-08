import os
from dotenv import load_dotenv
load_dotenv()
DB_CONFIG = {
    "hospital_a": f"mysql+mysqlconnector://{os.getenv("MYSQL_USER")}:{os.getenv("MYSQL_PASSWORD")}@localhost/hospital_a",
    "hospital_b": f"mysql+mysqlconnector://{os.getenv("MYSQL_USER")}:{os.getenv("MYSQL_PASSWORD")}@localhost/hospital_b"
}