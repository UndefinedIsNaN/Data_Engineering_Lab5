import pandas as pd
import msgpack
import pickle

# Пути к исходным CSV-файлам
white_wine_csv = "C:/Users/Mitya/Downloads/02/02/winequality-white.csv"
red_wine_csv = "C:/Users/Mitya/Downloads/02/02/winequality-red.csv"

# Пути для сохранения преобразованных файлов
white_wine_msgpack = "C:/Users/Mitya/Downloads/02/02/winequality-white.msgpack"
red_wine_pickle = "C:/Users/Mitya/Downloads/02/02/winequality-red.pkl"

# Чтение CSV и преобразование в DataFrame
def csv_to_dataframe(file_path):
    return pd.read_csv(file_path, sep=";")

# Сохранение в формат msgpack
def save_to_msgpack(df, file_path):
    with open(file_path, "wb") as f:
        msgpack.pack(df.to_dict(orient="records"), f)

# Сохранение в формат pickle
def save_to_pickle(df, file_path):
    with open(file_path, "wb") as f:
        pickle.dump(df.to_dict(orient="records"), f)

if __name__ == "__main__":
    # Обработка файла с белым вином
    white_wine_df = csv_to_dataframe(white_wine_csv)
    save_to_msgpack(white_wine_df, white_wine_msgpack)

    # Обработка файла с красным вином
    red_wine_df = csv_to_dataframe(red_wine_csv)
    save_to_pickle(red_wine_df, red_wine_pickle)

    print(f"Файл {white_wine_csv} преобразован в {white_wine_msgpack}")
    print(f"Файл {red_wine_csv} преобразован в {red_wine_pickle}")
