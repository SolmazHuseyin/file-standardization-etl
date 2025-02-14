from dataclasses import dataclass

@dataclass
class DBConfig:
    host: str
    dbname: str
    user: str
    password: str
    port: int = 5432
    schema: str = "imip" 