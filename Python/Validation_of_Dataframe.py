# Sample DataFrame declaration
data = [("Alice", 34, "value1"), ("Bob", 45, "value2"), ("Catherine", 29, "value1")]
columns = ["name", "age", "new_column"]

# For PySpark
df = spark.createDataFrame(data, columns)

# For Pandas
import pandas as pd
df = pd.DataFrame(data, columns=columns)

# 1. Schema Validation
expected_columns = ["name", "age", "new_column"]

# For PySpark
if set(df.columns) == set(expected_columns):
    print("Schema is valid.")
else:
    print("Schema is invalid.")

# For Pandas
if set(df.columns) == set(expected_columns):
    print("Schema is valid.")
else:
    print("Schema is invalid.")

# 2. Null Value Validation
# For PySpark
df.select([df[col].isNull().alias(col) for col in df.columns]).show()

# For Pandas
print(df.isnull().sum())

# 3. Data Type Validation
expected_dtypes = {"name": "string", "age": "int"}

# For PySpark
actual_dtypes = dict(df.dtypes)
if all(actual_dtypes[col] == expected_dtypes[col] for col in expected_dtypes):
    print("Data types are valid.")
else:
    print("Data types are invalid.")

# For Pandas
actual_dtypes = dict(df.dtypes)
if all(str(actual_dtypes[col]) == expected_dtypes[col] for col in expected_dtypes):
    print("Data types are valid.")
else:
    print("Data types are invalid.")

# 4. Range Validation (check age is between 0 and 120)
# For PySpark
df.filter((df["age"] < 0) | (df["age"] > 120)).show()

# For Pandas
invalid_age = df[(df["age"] < 0) | (df["age"] > 120)]
print(invalid_age)

# 5. Unique Value Validation
# For PySpark
df.groupBy("name").count().filter("count > 1").show()

# For Pandas
duplicate_check = df[df.duplicated(subset="name")]
print(duplicate_check)

# 6. Value Set Validation (ensure new_column contains only allowed values)
allowed_values = ["value1", "value2", "value3"]

# For PySpark
df.filter(~df["new_column"].isin(allowed_values)).show()

# For Pandas
invalid_values = df[~df["new_column"].isin(allowed_values)]
print(invalid_values)

# 7. Row Count Validation
# For PySpark
row_count = df.count()
if row_count > 0:
    print(f"DataFrame has {row_count} rows, validation passed.")
else:
    print("DataFrame is empty.")

# For Pandas
row_count = len(df)
if row_count > 0:
    print(f"DataFrame has {row_count} rows, validation passed.")
else:
    print("DataFrame is empty.")
