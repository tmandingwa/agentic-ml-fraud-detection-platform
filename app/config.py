import os
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql+asyncpg://fraud:fraud@localhost:5432/fraudsim")
APP_BASE_URL = os.getenv("APP_BASE_URL", "http://127.0.0.1:8000")
SIM_ENABLED = os.getenv("SIM_ENABLED", "true").lower() == "true"
SIM_TPS = float(os.getenv("SIM_TPS", "2.0"))
CASE_PDF_DIR = os.getenv("CASE_PDF_DIR", "./case_files")
