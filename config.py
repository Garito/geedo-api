from typing import Dict
from os import urandom, environ
from os.path import exists
from binascii import hexlify

class Config:
  HOST: str = "0.0.0.0"
  PUBLIC_HOST: str = HOST
  PORT: int = 8000
  SERVER_NAME: str = "http://{}:{}".format(PUBLIC_HOST, PORT)
  APP_URL: str = "http://localhost:8080"

  MONGO_URI = environ.get("MONGO_URI", "mongodb://localhost:27017")
  MONGO_DB = "gd"
  MONGO_GRIDFS = True

  ES_SERVERS = ["es1"]

  OA_INFO: Dict[str, str] = {
    "title": "Content management",
    "description": "Content management's REST API",
    "termsOfService": f"{SERVER_NAME}/terms/",
    "contact": {
      "name": "API support team",
      "url": f"{SERVER_NAME}/support/",
      "email": f"support@{PUBLIC_HOST}"
    },
    "license": {
      "name": "API license 1.0",
      "url": f"{SERVER_NAME}/license/"
    },
    "version": "0.0.1"
  }

  jwt_secret_file_path = "/run/secrets/jwt_secret"
  if exists(jwt_secret_file_path):
    with open(jwt_secret_file_path) as f:
      JWT_SECRET = f.read()
  else:
    JWT_SECRET = hexlify(urandom(32))

  SYSTEM_ROLES = {
    "Admin": {"description": "The administrator"},
    # "Invited": {"description": "The non registered user that has asked for an invitation", "system_only": True},
    # "Forgetful": {"description": "The user that has forgot the password", "system_only": True},
    "Owner": {"description": "The owner of the resource"},
    "Participant": {"description": "The one that finishes a phase"}
  }

  # OPEN_ENDPOINTS = ['Group/call', 'Group/auth', 'Group/get_permissions', 'Group/get_roles', 'Group/get_users']
  OPEN_ENDPOINTS = ['Group/auth', 'Group/get_permissions', 'Group/get_roles', 'Group/get_users']

  MAIL_SENDER = f"CM's butler <butler@example.net>"
  MAIL_SERVER = "smtp.gmail.com"
  MAIL_PORT = 465
  MAIL_ARGS = {"use_tls": True, "username": environ.get("MAIL_USER", ""), "password": environ.get("MAIL_PASSWORD", "")}

class Production(Config):
  OA_SERVER_DESCRIPTION: str = "Production server"

class Development(Config):
  DEBUG: bool = True
  # DEBUG_NOTIFICATIONS: bool = True

  # MONGO_URI = "mongodb://mongo:27017"

  OA_SERVER_DESCRIPTION: str = "Development server"

  JWT_SECRET = "ASuperSecretJWT_Secret"

class Testing(Development):
  TESTING: bool = True

  MONGO_DB = "Tests"

  OA_SERVER_DESCRIPTION: str = "Test server"
