from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app.routers import reports_router

app = FastAPI(title="Analytics API", version="1.0")

# CORS (for frontend access)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Replace with specific domain(s) in production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include all routers
app.include_router(reports_router.router)

@app.get("/")
async def root():
    return {"message": "3PL Analytics API is running 🚀"}
