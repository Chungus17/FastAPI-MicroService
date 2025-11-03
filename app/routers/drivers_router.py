# app/routes/reports.py

import json
import os
from fastapi import APIRouter, Query
from typing import List, Optional
from datetime import datetime

router = APIRouter(prefix="/api/driver", tags=["Drivers"])

@router.get("/test_api")
async def generate_driver_report():
    return {"message": "Driver API is working 🚀"}