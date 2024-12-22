import pymongo
import json

# Подключение к MongoDB
client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client["practical_work"]
collection = db["data"]

# Функция для загрузки данных из файла JSON
def load_json(file_path):
    with open(file_path, "r", encoding="utf-8") as f:
        return json.load(f)

# Функция для добавления данных в MongoDB
def insert_data(data):
    if isinstance(data, list):
        collection.insert_many(data)
    else:
        collection.insert_one(data)

# Запросы из задания 3
def delete_salary_outliers():
    result = collection.delete_many({"$or": [
        {"salary": {"$lt": 25000}},
        {"salary": {"$gt": 175000}}
    ]})
    return result.deleted_count

def increment_age():
    result = collection.update_many({}, {"$inc": {"age": 1}})
    return result.modified_count

def increase_salary_by_job(jobs, percentage):
    result = collection.update_many(
        {"job": {"$in": jobs}},
        {"$mul": {"salary": 1 + percentage / 100}}
    )
    return result.modified_count

def increase_salary_by_city(cities, percentage):
    result = collection.update_many(
        {"city": {"$in": cities}},
        {"$mul": {"salary": 1 + percentage / 100}}
    )
    return result.modified_count

def complex_salary_increase(city, jobs, age_range, percentage):
    result = collection.update_many(
        {
            "city": city,
            "job": {"$in": jobs},
            "age": {"$gte": age_range[0], "$lte": age_range[1]}
        },
        {"$mul": {"salary": 1 + percentage / 100}}
    )
    return result.modified_count

def delete_by_custom_predicate(predicate):
    result = collection.delete_many(predicate)
    return result.deleted_count

# Основной блок выполнения
if __name__ == "__main__":
    # Пример загрузки данных из файла JSON
    json_data = load_json("C:/Users/Mitya/Downloads/02/02/task_3_item.json")

    # Загрузка данных в MongoDB
    insert_data(json_data)

    # Выполнение запросов
    print("Deleted salary outliers:", delete_salary_outliers())
    print("Incremented age for all documents:", increment_age())

    jobs = ["Строитель", "Менеджер"]
    print("Increased salary for jobs:", increase_salary_by_job(jobs, 5))

    cities = ["Будапешт", "Гранада"]
    print("Increased salary for cities:", increase_salary_by_city(cities, 7))

    print("Complex salary increase:", complex_salary_increase("Москва", ["Учитель", "Менеджер"], [25, 35], 10))

    custom_predicate = {"city": "Гранада"}  # Пример произвольного предиката
    print("Deleted by custom predicate:", delete_by_custom_predicate(custom_predicate))
