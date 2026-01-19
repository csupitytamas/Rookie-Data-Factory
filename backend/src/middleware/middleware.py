from fastapi.middleware.cors import CORSMiddleware

origins = [
    "http://localhost:5173",  # Vue dev server
    "http://localhost",       # Electron
]

def add_middlewares(app):
    app.add_middleware(
        CORSMiddleware,
        allow_origins=origins,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )