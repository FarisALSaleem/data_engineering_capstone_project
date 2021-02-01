from pyspark.sql.functions import when, count, col


def display_missing_values(df):
    """Takes a dataframe and creates a pandas table displaying the amount of nulls """

    # show nulls in a sparck dataframe was retrived from this article
    # https://towardsdatascience.com/data-prep-with-spark-dataframes-3629478a1041
    df_null = df.select([count(when(col(c).isNull(), c)).alias(c) for c in df.columns]).toPandas().T
    df_length = df.count()
    df_null['%'] = df_null[0].apply(lambda row: row / df_length * 100)

    return df_null


def get_sas():
    """Reads I94_SAS_Labels_Descriptions.SAS as a list of strings"""
    with open("data/I94_SAS_Labels_Descriptions.SAS") as file:
        sas_descriptions = file.readlines()
    sas_descriptions = [line.replace("'", "").lower().strip() for line in sas_descriptions]
    return sas_descriptions
