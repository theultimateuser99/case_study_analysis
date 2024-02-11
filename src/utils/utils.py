import yaml

def load_data_from_csv(spark, file_path):
    """
    Read csv data as a dataframe
    :param spark: spark instance
    :param file_path: path to the csv file
    :return: dataframe
    """
    try:
        return spark.read.csv(file_path, header=True, inferSchema=True)
    except Exception as e:
        raise e


def read_yaml(file_path):
    """
    Read config file in YAML format
    param file_path: file path to config.yaml
    return: config details
    """
    try:
        with open(file_path, "r") as f:
            return yaml.safe_load(f)
    except Exception as e:
        raise e


def write_output(df, file_path, write_format):
    """
    Write pyspark dataframe to csv as
    param df: output dataframe
    param file_path: output file path
    param write_format: output file format
    return: None
    """
    try:
        df.write.format(write_format).mode('overwrite').option("header", "true").save(file_path)
    except Exception as e:
        raise e