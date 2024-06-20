1. Create a `.env` file
2. Set `MONGO_URI` inside that file. You can get this from MongoAtlas
3. Run the script using this command: `env $(cat .env | xargs) poetry run python main.py`