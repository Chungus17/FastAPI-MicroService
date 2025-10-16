import httpx
import os

VERDI_API_KEY = os.environ.get("VERDI_API_KEY")

async def getData(start_date, end_date, filter_by):
    # Example async API request
    url = "https://tryverdi.com/api/transaction_data"
    params = {"start_date": start_date, "end_date": end_date, "user_id": filter_by}
    headers = {"Authorization": f"Bearer {VERDI_API_KEY}"}

    async with httpx.AsyncClient(timeout=30.0) as client:
        response = await client.get(url, params=params, headers=headers)
        response.raise_for_status()
        return response.json()
