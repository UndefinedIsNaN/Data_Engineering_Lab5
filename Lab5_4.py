import pymongo
import msgpack
import pickle

# Подключение к MongoDB
client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client["practical_work"]
collection_white = db["white_wine"]
collection_red = db["red_wine"]

# Функции для загрузки данных
def load_msgpack(file_path):
    with open(file_path, "rb") as f:
        return msgpack.unpack(f, raw=False)

def load_pickle(file_path):
    with open(file_path, "rb") as f:
        return pickle.load(f)

# Функция для загрузки данных в MongoDB
def insert_data(collection, data):
    if isinstance(data, list):
        collection.insert_many(data)
    else:
        collection.insert_one(data)

# Запросы для выборки (задание 1)
def query_quality_above(collection, min_quality):
    return list(collection.find({"quality": {"$gt": min_quality}}, {"_id": 0}))

def query_alcohol_above(collection, min_alcohol):
    return list(collection.find({"alcohol": {"$gt": min_alcohol}}, {"_id": 0}))

def query_residual_sugar_range(collection, min_sugar, max_sugar):
    return list(collection.find({"residual sugar": {"$gte": min_sugar, "$lte": max_sugar}}, {"_id": 0}))

def query_citric_acid_below(collection, max_citric_acid):
    return list(collection.find({"citric acid": {"$lt": max_citric_acid}}, {"_id": 0}))

def query_density_range(collection, min_density, max_density):
    return list(collection.find({"density": {"$gte": min_density, "$lte": max_density}}, {"_id": 0}))

# Запросы для агрегации (задание 2)
def avg_alcohol_by_quality(collection):
    pipeline = [
        {"$group": {"_id": "$quality", "avg_alcohol": {"$avg": "$alcohol"}}},
        {"$sort": {"_id": 1}}
    ]
    return list(collection.aggregate(pipeline))

def count_by_quality(collection):
    pipeline = [
        {"$group": {"_id": "$quality", "count": {"$sum": 1}}},
        {"$sort": {"_id": 1}}
    ]
    return list(collection.aggregate(pipeline))

def avg_sugar_by_quality(collection):
    pipeline = [
        {"$group": {"_id": "$quality", "avg_sugar": {"$avg": "$residual sugar"}}},
        {"$sort": {"_id": 1}}
    ]
    return list(collection.aggregate(pipeline))

def min_max_alcohol_by_quality(collection):
    pipeline = [
        {"$group": {
            "_id": "$quality",
            "min_alcohol": {"$min": "$alcohol"},
            "max_alcohol": {"$max": "$alcohol"}
        }},
        {"$sort": {"_id": 1}}
    ]
    return list(collection.aggregate(pipeline))

def avg_density_by_quality(collection):
    pipeline = [
        {"$group": {"_id": "$quality", "avg_density": {"$avg": "$density"}}},
        {"$sort": {"_id": 1}}
    ]
    return list(collection.aggregate(pipeline))

# Запросы для обновления/удаления (задание 3)
def delete_low_quality(collection, max_quality):
    result = collection.delete_many({"quality": {"$lt": max_quality}})
    return result.deleted_count

def increment_quality(collection, increment_value):
    result = collection.update_many({}, {"$inc": {"quality": increment_value}})
    return result.modified_count

def update_alcohol_by_quality(collection, quality, percentage):
    result = collection.update_many(
        {"quality": quality},
        {"$mul": {"alcohol": 1 + percentage / 100}}
    )
    return result.modified_count

def delete_high_density(collection, max_density):
    result = collection.delete_many({"density": {"$gt": max_density}})
    return result.deleted_count

def increase_sugar_by_range(collection, min_quality, max_quality, percentage):
    result = collection.update_many(
        {"quality": {"$gte": min_quality, "$lte": max_quality}},
        {"$mul": {"residual sugar": 1 + percentage / 100}}
    )
    return result.modified_count

if __name__ == "__main__":
    # Пути к данным
    white_wine_msgpack = "C:/Users/Mitya/Downloads/02/02/winequality-white.msgpack"
    red_wine_pickle = "C:/Users/Mitya/Downloads/02/02/winequality-red.pkl"

    # Загрузка данных
    white_wine_data = load_msgpack(white_wine_msgpack)
    red_wine_data = load_pickle(red_wine_pickle)

    # Загрузка данных в MongoDB
    insert_data(collection_white, white_wine_data)
    insert_data(collection_red, red_wine_data)

    # Выполнение запросов
    # Выборка
    print("Белое вино, качество выше 6:", query_quality_above(collection_white, 6))
    print("Красное вино, алкоголь выше 10:", query_alcohol_above(collection_red, 10))
    print("Белое вино, сахар от 2 до 10:", query_residual_sugar_range(collection_white, 2, 10))
    print("Красное вино, лимонная кислота меньше 0.4:", query_citric_acid_below(collection_red, 0.4))
    print("Белое вино, плотность от 0.99 до 1.0:", query_density_range(collection_white, 0.99, 1.0))

    # Агрегация
    print("Средний алкоголь по качеству (белое):", avg_alcohol_by_quality(collection_white))
    print("Количество по качеству (красное):", count_by_quality(collection_red))
    print("Средний сахар по качеству (белое):", avg_sugar_by_quality(collection_white))
    print("Мин/Макс алкоголь по качеству (красное):", min_max_alcohol_by_quality(collection_red))
    print("Средняя плотность по качеству (белое):", avg_density_by_quality(collection_white))

    # Обновление/удаление
    print("Удалено низкого качества (белое):", delete_low_quality(collection_white, 4))
    print("Увеличено качество на 1 (красное):", increment_quality(collection_red, 1))
    print("Обновлен алкоголь на 5% для качества 6 (белое):", update_alcohol_by_quality(collection_white, 6, 5))
    print("Удалено с высокой плотностью (красное):", delete_high_density(collection_red, 1.002))
    print("Увеличено содержание сахара на 10% для качества от 5 до 7 (белое):", increase_sugar_by_range(collection_white, 5, 7, 10))
