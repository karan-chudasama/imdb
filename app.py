import os
from functools import wraps
from bson import json_util
from bson.objectid import ObjectId
from sanic import Sanic, response, exceptions
from settings import DATABASE
from sanic.log import logger


app = Sanic(__name__)


# Create Database connection
@app.listener('before_server_start')
def init(app, loop):
    global db
    from motor.motor_asyncio import AsyncIOMotorClient
    logger.info(f"Opening the database connection for {app.name}")
    db = AsyncIOMotorClient(os.environ.get("DATABASE_URL", DATABASE["MONGO_DB_URL"]))["imdbdata"]


# Authentication Logic
def _is_authenticated(request):
    token = request.headers.get("Auth-token", None) or request.token
    logger.info(f"user token is {token}")
    return token == os.environ.get("SECRET_AUTH_KEY")


# Authecation decorator
def auth_required(original_function):
    @wraps(original_function)
    async def inner_func(request, *args, **kwargs):
        if not _is_authenticated(request):
            raise exceptions.Unauthorized("User is not Authrized for this action!!!")
        return await original_function(request, *args, **kwargs)
    return inner_func


# Supporting Function to get document object id
def get_object_id(id):
    if isinstance(id, (str, ObjectId)):
        return ObjectId(id) if ObjectId.is_valid(id) else id


@app.route('/', methods=['GET'])
async def index(request):
    logger.info(f"starting API connecting to DB {db}")
    docs = await db.movies.find().to_list(None)
    logger.info(f"waiting for response")
    return response.json(docs, dumps=json_util.dumps)


@app.route("/search", methods=["POST"])
async def search_movie(request):
    if request.method == "POST":
        movie_name = request.json["name"]
        # Searching text index for any word input instead of exact movie name
        movie = await db.movies.find({"$text": {"$search": movie_name}}).to_list(None)
        return response.json(movie, dumps=json_util.dumps)


@app.route("/add", methods=["POST"])
@auth_required
async def add_movie(request):
    if request.method == "POST":
        movie_data = request.json["data"]
        if not isinstance(movie_data, list):
            movie_data = [movie_data]
        added_movies = await db.movies.insert_many(movie_data)
        return response.json({"success": "true", "message": f"Added {len(added_movies.inserted_ids)} data!!"})


@app.route("/edit/<movie_id>", methods=["GET", "POST"])
@auth_required
async def edit_movie(request, movie_id):
    movie_id = get_object_id(movie_id)

    # Fetch the movie data using object id
    movie_data = await db.movies.find_one({"_id": movie_id})
    if request.method == "POST":
        update_data = request.json

        # Validate genre is List
        if update_data.get("genre") and not isinstance(update_data["genre"], list):
            return response.json({"status": "false", "message": "genre field must be a list of string"})

        # Updating the movie data requested
        await db.movies.update_one({"_id": movie_id}, {"$set": update_data})
        # Fetching updated movie data and retuning
        movie_data = await db.movies.find_one({"_id": movie_id})
    return response.json(movie_data, dumps=json_util.dumps)


@app.route("/remove/<movie_id>", methods=["GET"])
@auth_required
async def remove_movie(request, movie_id):
    movie_id = get_object_id(movie_id)
    result = await db.movies.delete_one({"_id": movie_id})
    if result.deleted_count:
        message = f"Successfully deleted {result.deleted_count} data!!"
    else:
        message = "No such data found, may be already removed!!"
    return response.json({"success": "true", "message": message})


if __name__ == "__main__":
    app.run(
        host='0.0.0.0',
        port=int(os.environ.get("PORT", 8000)),
        workers=int(os.environ.get("WEB_WORKER", 1)),
        debug=bool(os.environ.get("DEBUG", ""))
    )
