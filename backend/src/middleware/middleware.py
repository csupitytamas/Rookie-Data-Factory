from fastapi.middleware.cors import CORSMiddleware

""" CORS (Cross-Origin Resource Sharing) middleware, amely lehetővé teszi a biztonságos kommunikációt a frontend backend között. """

origins = [
    "http://localhost:5173",  # Vue dev server (default)
    "http://localhost:5174",  # Vue dev server (port conflict alternative)
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