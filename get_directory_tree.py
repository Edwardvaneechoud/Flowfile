import os
import json


def get_directory_structure(rootdir, target_dirs=["flowfile_worker", "flowfile_core"], max_depth=4, exclude_dirs=["node_modules", ".git", "__pycache__", ".venv", "dist", "build", ".pytest_cache", ".idea", ".github"]):
    """
    Generates a directory structure with full exploration of target directories.
        """

    dir_structure = {}

    if not os.path.isdir(rootdir): #exits if rootdir does not exists
        return "Error: Invalid root directory path provided."

    def _scan_directory(path, depth, structure):
        if depth > max_depth:
            return


        for item in os.listdir(path):
          if item in exclude_dirs:
              continue #skip excluded directories

          item_path = os.path.join(path, item)
          if os.path.isdir(item_path):
                structure[item] = {}
                _scan_directory(item_path, depth + 1, structure[item])
          else:
              structure[item] = None #represents a file


    for item in os.listdir(rootdir):
      item_path = os.path.join(rootdir, item)
      if os.path.isdir(item_path):
          if item in target_dirs: # Full exploration for target directories
              dir_structure[item] = {}
              _scan_directory(item_path, 1, dir_structure[item])
          elif item not in exclude_dirs:
              dir_structure[item] = {} #only includes directories, no files


    return dir_structure



if __name__ == "__main__":
    root_directory_to_scan = "."
    directory_data = get_directory_structure(root_directory_to_scan)


    # Nicely formatted JSON output for easy review or parsing
    print(json.dumps(directory_data, indent=4))

    # Or, save it to a file:
    with open("directory_structure.json", "w") as f:
        json.dump(directory_data, f, indent=4)
    print("File saved to directory_structure.json")


from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, DoubleType, StringType
from posman.validations.table_validator import SparkField, Table

def get_spark_session():
    return SparkSession.builder \
        .appName("TestingFrameworkDemo") \
        .master("local[1]") \
        .getOrCreate()
class AgeTable(Table):
    """Sample table definition for age and price data"""
    age: SparkField
    price: SparkField

spark = get_spark_session()

age_field = SparkField(field_name="age", data_type=IntegerType(), nullable=False, min_value=18, max_value=59)
price_field = SparkField(field_name="price", data_type=DoubleType(), nullable=True, min_value=0.0, max_value=100.0)

age_table = AgeTable(fields=[age_field, price_field])
age_df = age_table.create_test_dataframe(spark, num_rows=3)
print('age_df:\n', age_df)
print('age_df selected:\n', age_df.select(age_table.price.cast(StringType())))



age_field = SparkField(field_name="age", data_type=IntegerType(), nullable=False, min_value=18, max_value=59)
...
print('age_df selected:\n', age_df.select(age_field.cast(StringType())))
