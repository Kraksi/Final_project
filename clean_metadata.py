import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns

# Функция для классификации возраста в группы
def age_group(age):
    if age < 18:
        return "child"           # дети (младше 18)
    elif age < 60:
        return "adult"           # взрослые (от 18 до 59)
    else:
        return "elderly"         # пожилые (от 60 и старше)
    
# Функция для унификации значений диагнозов
def unify_finding(finding):
    if "COVID" in finding:
        return "COVID-19"
    elif "PNEUMONIA" in finding:
        return "PNEUMONIA"
    else:
        return finding

# Чтение исходного CSV-файла в DataFrame
df = pd.read_csv("metadata.csv")

# Приведение всех значений в столбце finding к верхнему регистру
df["finding"] = df["finding"].str.upper()

# Упрощение вариантов названий диагнозов для COVID-19
df["finding"] = df["finding"].replace({
    "COVID-19 ARDS": "COVID-19",
    "COVID19": "COVID-19",
    "COVID-19 PNEUMONIA": "COVID-19",
    "COVID19 PNEUMONIA": "COVID-19"
})

# Удаляем строки с некорректным возрастом (за пределами 0-120 лет),
# или оставляем, если возраст отсутствует (NaN)
df = df[(df["age"].isnull()) | ((df["age"] >= 0) & (df["age"] <= 120))]

# Генерируем случайные значения для заполнения пропусков в возрасте (20-80 лет)
random_ages = np.random.randint(20, 81, size=df["age"].isna().sum())

# Заполняем пропуски случайными значениями
df.loc[df["age"].isna(), "age"] = random_ages

# Находим наиболее часто встречающееся значение пола
mode_sex = df["sex"].mode()[0]

# Заполняем пропуски в поле sex модой (наиболее частым значением)
df["sex"].fillna(mode_sex, inplace=True)

# Заполняем пропуски в диагнозах значением "UNKNOWN"
df["finding"].fillna("UNKNOWN", inplace=True)

# Удаляем полностью дублирующиеся строки
df.drop_duplicates(inplace=True)

# Создаем новый столбец с возрастными группами
df["age_group"] = df["age"].apply(age_group)

# Создаем новый столбец с унифицированными диагнозами
df["finding_grouped"] = df["finding"].apply(unify_finding)

# Сохраняем очищенный DataFrame в новый CSV-файл
df.to_csv("metadata_cleaned.csv", index=False)
