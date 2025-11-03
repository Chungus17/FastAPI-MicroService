import json
import os
import firebase_admin
from firebase_admin import credentials, firestore

# Initialize Firebase Admin once per process and expose a Firestore client.
def get_db():
    if not firebase_admin._apps:
        creds_json = os.environ.get("FIREBASE_CREDENTIALS")
        if not creds_json:
            raise RuntimeError("FIREBASE_CREDENTIALS env var is not set")
        cred_dict = json.loads(creds_json)
        cred = credentials.Certificate(cred_dict)
        firebase_admin.initialize_app(cred)
    return firestore.client()

# Convenience singleton
db = get_db()
