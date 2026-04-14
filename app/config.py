# -*- coding: utf-8 -*-
from pydantic import BaseModel
from dotenv import load_dotenv
import os

load_dotenv()

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
DEFAULT_DB_PATH = os.path.join(PROJECT_ROOT, "instance", "workxetnghiem.sqlite3")
DEFAULT_UPLOAD_DIR = os.path.join(PROJECT_ROOT, "instance", "uploads")
DEFAULT_DB_URL = f"sqlite:///{DEFAULT_DB_PATH.replace(os.sep, '/')}"


class Settings(BaseModel):
    APP_NAME: str = os.getenv("APP_NAME", "Ứng dụng quản lý, điều hành - Khoa Xét nghiệm")
    COMPANY_NAME: str = os.getenv("COMPANY_NAME", "BỆNH VIỆN HÙNG VƯƠNG GIA LAI")
    SECRET_KEY: str = os.getenv("SECRET_KEY", "dev-secret-key-change-me")
    DATABASE_URL: str = os.getenv("DATABASE_URL", DEFAULT_DB_URL)
    UPLOAD_DIR: str = os.getenv("UPLOAD_DIR", DEFAULT_UPLOAD_DIR)
    MAX_FILE_SIZE_MB: int = int(os.getenv("MAX_FILE_SIZE_MB", "50"))
    PORT: int = int(os.getenv("PORT", "5004"))

    # Mã vị trí theo định hướng đã chốt
    SECRET_CODE_BGD: str = os.getenv("SECRET_CODE_BGD", "BGD-2026")
    SECRET_CODE_TRUONG_KHOA: str = os.getenv("SECRET_CODE_TRUONG_KHOA", "TK-2026")
    SECRET_CODE_QUAN_LY_CHUC_NANG: str = os.getenv("SECRET_CODE_QUAN_LY_CHUC_NANG", "QLCN-2026")
    SECRET_CODE_QUAN_LY_CONG_VIEC: str = os.getenv("SECRET_CODE_QUAN_LY_CONG_VIEC", "QLCV-2026")
    SECRET_CODE_TRUONG_NHOM: str = os.getenv("SECRET_CODE_TRUONG_NHOM", "TN-2026")


settings = Settings()
