# This file handles passwords and login tokens (JWT).
# Other files use these functions to check who a user is.

from passlib.context import CryptContext
from jose import jwt, JWTError
from datetime import datetime, timedelta
import os

# This turns plain passwords into scrambled (hashed) text, and checks them later.
pwd_context = CryptContext(schemes=['bcrypt'], deprecated = 'auto')

# The secret key signs every login token. Read it from the environment in production.
# The fallback value is only for local development.
SECRET_KEY = os.getenv("SECRET_KEY", "secret")
ALGORITHM = 'HS256'
ACCESS_TOKEN_EXPIRE_MINUTES = 30
REFRESH_TOKEN_EXPIRE_DAYS = 7


# Turn a plain password into a hashed password, so we never store the real password.
def hash_password(password:str)->str:
    return pwd_context.hash(password)

# Check if a plain password matches a hashed password.
def verify_password(plain_password:str, hashed_password:str)->bool:
    return pwd_context.verify(plain_password, hashed_password)

# Create a short-lived access token for a logged-in user.
def create_access_token(data:dict)->str:
    to_encode = data.copy()
    expire = datetime.utcnow() + timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    to_encode.update({'exp':expire,"type": "access"})
    return jwt.encode(to_encode,SECRET_KEY,algorithm=ALGORITHM)

# Create a long-lived refresh token, used to get a new access token later.
def create_refresh_token(data:dict)->str:
    to_encode = data.copy()
    expire = datetime.utcnow() + timedelta(days=REFRESH_TOKEN_EXPIRE_DAYS)
    to_encode.update({'exp':expire,"type": "refresh"})
    return jwt.encode(to_encode,SECRET_KEY,algorithm=ALGORITHM)

# Read a token and return its contents. Raises an error if the token is invalid or expired.
def decode_access_token(token:str)->dict:
    return jwt.decode(token,SECRET_KEY,algorithms=[ALGORITHM])