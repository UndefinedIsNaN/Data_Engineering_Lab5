import pymongo
import msgpack

# Подключение к MongoDB
client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client["Lab5_database"]
collection = db["Lab5_1"]

# Функции для загрузки данных из файлов различных форматов
def load_msgpack(file_path):
    with open(file_path, "rb") as f:
        return msgpack.unpack(f, raw=False)

def load_pickle(file_path):
    with open(file_path, "rb") as f:
        return pickle.load(f)

def load_json(file_path):
    with open(file_path, "r", encoding="utf-8") as f:
        return json.load(f)

# Функция для загрузки данных в MongoDB
def insert_data(data):
    if isinstance(data, list):
        collection.insert_many(data)
    else:
        collection.insert_one(data)

# Запросы из задания 1
def query_1():
    return list(collection.find({}, {"_id": 0}).sort("salary", -1).limit(10))

def query_2():
    return list(
        collection.find({"age": {"$lt": 30}}, {"_id": 0})
        .sort("salary", -1)
        .limit(15)
    )

def query_3(city, professions):
    return list(
        collection.find(
            {"city": city, "job": {"$in": professions}}, {"_id": 0}
        ).sort("age", 1).limit(10)
    )

def query_4():
    return collection.count_documents(
        {
            "$and": [
                {"age": {"$gte": 25, "$lte": 40}},
                {"year": {"$gte": 2019, "$lte": 2022}},
                {
                    "$or": [
                        {"salary": {"$gt": 50000, "$lte": 75000}},
                        {"salary": {"$gt": 125000, "$lt": 150000}},
                    ]
                },
            ]
        }
    )

# Основной блок выполнения
if __name__ == "__main__":
    # Пример загрузки данных из файлов
    msgpack_data = load_msgpack("C:/Users/Mitya/Downloads/02/02/task_1_item.msgpack")
   # pickle_data = load_pickle("data2.pkl")
    #json_data = load_json("data3.json")

    # Загрузка данных в MongoDB
    insert_data(msgpack_data)
    #insert_data(pickle_data)
    #insert_data(json_data)

    # Выполнение запросов
    city = "Гранада"  # Пример произвольного города
    professions = ["Менеджер", "Бухгалтер", "Строитель"]  # Пример профессий

    print("Query 1:", query_1())
    print("Query 2:", query_2())
    print("Query 3:", query_3(city, professions))
    print("Query 4:", query_4())
