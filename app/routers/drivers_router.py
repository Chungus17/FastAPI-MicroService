# app/routes/reports.py

from fastapi import APIRouter, HTTPException, Query
from typing import Literal
from app.utils.firebase_connection import db
from app.services.drivers.batchwise_AA import auto_allocation_batchwise
from app.services.drivers.oneByOne_AA import auto_allocation_one_by_one

router = APIRouter(prefix="/api/drivers", tags=["Drivers"])


@router.get("/auto-allocation")
def auto_allocation(
    pickup_lat: float,
    pickup_lng: float,
    type: Literal["one_by_one", "batchwise"] = Query("one_by_one"),
    max_radius: float = Query(15.0, ge=0.1),
    increment: float = Query(5.0, ge=0.1),
):
    if pickup_lat is None or pickup_lng is None:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="pickup_lat and pickup_lng are required",
        )

    if type == "one_by_one":
        return auto_allocation_one_by_one(
            pickup_lat=pickup_lat,
            pickup_lng=pickup_lng,
            max_radius=max_radius,
        )
    elif type == "batchwise":
        return auto_allocation_batchwise(
            pickup_lat=pickup_lat,
            pickup_lng=pickup_lng,
            max_radius=max_radius,
            increment=increment,
        )
    else:
        return("Wrong algorithm type!!")
