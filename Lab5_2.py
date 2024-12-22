import pymongo
import pickle
import statistics

# Подключение к MongoDB
client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client["practical_work"]
collection = db["data"]

# Функция для загрузки данных из файла pickle
def load_pickle(file_path):
    with open(file_path, "rb") as f:
        return pickle.load(f)

# Функция для добавления данных в MongoDB
def insert_data(data):
    if isinstance(data, list):
        collection.insert_many(data)
    else:
        collection.insert_one(data)

# Запросы из задания 2
def salary_statistics():
    salaries = list(collection.find({}, {"_id": 0, "salary": 1}))
    salary_values = [doc["salary"] for doc in salaries]
    return {
        "min": min(salary_values),
        "avg": statistics.mean(salary_values),
        "max": max(salary_values),
    }

def job_count():
    pipeline = [
        {"$group": {"_id": "$job", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}},
    ]
    return list(collection.aggregate(pipeline))

def salary_by_city():
    pipeline = [
        {
            "$group": {
                "_id": "$city",
                "min_salary": {"$min": "$salary"},
                "avg_salary": {"$avg": "$salary"},
                "max_salary": {"$max": "$salary"},
            }
        },
        {"$sort": {"avg_salary": -1}},
    ]
    return list(collection.aggregate(pipeline))

def salary_by_job():
    pipeline = [
        {
            "$group": {
                "_id": "$job",
                "min_salary": {"$min": "$salary"},
                "avg_salary": {"$avg": "$salary"},
                "max_salary": {"$max": "$salary"},
            }
        },
        {"$sort": {"avg_salary": -1}},
    ]
    return list(collection.aggregate(pipeline))

def age_statistics_by_city():
    pipeline = [
        {
            "$group": {
                "_id": "$city",
                "min_age": {"$min": "$age"},
                "avg_age": {"$avg": "$age"},
                "max_age": {"$max": "$age"},
            }
        },
        {"$sort": {"avg_age": -1}},
    ]
    return list(collection.aggregate(pipeline))

# Основной блок выполнения
if __name__ == "__main__":
    # Пример загрузки данных из файла pickle
    pickle_data = load_pickle("C:/Users/Mitya/Downloads/02/02/task_2_item.pkl")

    # Загрузка данных в MongoDB
    insert_data(pickle_data)

    # Выполнение запросов
    print("Salary statistics:", salary_statistics())
    print("job count:", job_count())
    print("Salary by city:", salary_by_city())
    print("Salary by job:", salary_by_job())
    print("Age statistics by city:", age_statistics_by_city())
