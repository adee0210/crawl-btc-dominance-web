from pymongo import MongoClient
from src.configs.config_variable import MONGO_CONFIG


class MongoDBConfig:
    _instance = None
    _client = None

    def _init_config(self):
        self._config = {
            "host": MONGO_CONFIG.get("host"),
            "port": MONGO_CONFIG.get("port"),
            "user": MONGO_CONFIG.get("user"),
            "pass": MONGO_CONFIG.get("pass"),
            "auth": MONGO_CONFIG.get("auth"),
        }

    # Singleton instance
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._init_config()
        return cls._instance

    @property
    def get_config(self):
        return self._config

    # Singletion client
    @classmethod
    def get_client(cls):
        if cls._client is None:
            instance = cls()
            config = instance.get_config
            client_kwargs = {
                "host": config["host"],
            }
            # Only include port if it is provided (and an int)
            if config.get("port") is not None:
                client_kwargs["port"] = config.get("port")
            if config["user"]:
                client_kwargs["username"] = config["user"]
            if config["pass"]:
                client_kwargs["password"] = config["pass"]
            if config["auth"]:
                client_kwargs["authSource"] = config["auth"]
            cls._client = MongoClient(**client_kwargs)
        return cls._client

    @classmethod
    def client_close(cls):
        if cls._client:
            cls._client.close()
