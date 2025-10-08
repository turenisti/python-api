from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from execution_engine.api import routes
import os
from dotenv import load_dotenv

load_dotenv()

app = FastAPI(
    title="Scheduling Report - Execution Engine",
    version="1.0.0",
    description="Report execution engine for processing and delivering scheduled reports"
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include routes
app.include_router(routes.router)

@app.get("/")
async def root():
    return {
        "service": "Scheduling Report - Execution Engine",
        "version": "1.0.0",
        "status": "running"
    }

@app.get("/health")
async def health_check():
    return {
        "status": "healthy",
        "service": "execution-engine",
        "version": "1.0.0"
    }

if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv('EXECUTION_API_PORT', 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
