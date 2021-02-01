from pyspark.sql.types import *
from pyspark.sql.functions import udf, avg
from utilities import *
from sql_queries import *
import psycopg2


@udf(StringType())
def get_visa_category(visa_class):
    """Takes a visa class code and returns the category with the visa belongs to"""
    visa_category_list = [tid.split('=') for tid in get_sas()[1046:1049]]
    for visa_category in visa_category_list:
        if visa_class == float(visa_category[0]):
            return visa_category[1].strip()


@udf(StringType())
def get_transportation_method(transportation_id):
    """Takes a transportation_method_id and returns the name of the transportation_method"""
    transportation_method_list = [tid.split('=') for tid in get_sas()[972:976]]
    for transportation_method_id in transportation_method_list:
        if transportation_id == float(transportation_method_id[0]):
            return transportation_method_id[1].strip()
    return "not reported"


@udf(StringType())
def remove_zero(double):
    """Takes a double in a string format and remove to digits from the end"""
    return str(double[:-2])


@udf(StringType())
def get_country(country_code):
    """Takes a country code and returns a country name"""
    country_codes_list = [country.split('=') for country in get_sas()[9:298]]
    for country_codes_with_comma in country_codes_list:
        country_codes_without_comma = country_codes_with_comma[1].split(",")
        for country_code_without_comma in country_codes_without_comma:
            if country_code.lower().strip() == country_code_without_comma.strip():
                return country_codes_with_comma[0].strip()


def i94_cleaning(df):
    """Cleans the I94 Immigration dataframe"""
    df = df.drop(*['occup', 'entdepu', 'insnum'])
    df = df.withColumn('transportation_method', get_transportation_method("i94mode"))
    df = df.withColumn('visa_category', get_visa_category("i94visa"))

    for col in ['i94cit', 'i94res']:
        df = df.withColumn(col, df[col].cast("string"))
        df = df.withColumn(col, remove_zero(col))

    return df


def world_temperature_cleaning(df):
    """Cleans the World Temperature dataframe"""
    df = df.dropna(subset=['AverageTemperature'])
    df = df.drop_duplicates(subset=['dt', 'City', 'Country'])
    df = df.withColumn('country_code', get_country("Country"))
    return df


def demographics_cleaning(df):
    """Cleans the Demographics dataframe"""
    columns = ['Male Population', 'Female Population', 'Number of Veterans', 'Foreign-born', 'Average Household Size']

    for column in columns:
        mean = df.agg(avg(column).alias(column)).first().asDict()
        df = df.fillna(mean)

    df = df.drop_duplicates(subset=['City', 'State'])
    return df


def create_tables(user, password, dbname, host):
    """Connects to the database creates all of the needed tables"""
    conn = psycopg2.connect(host=host, dbname=dbname, user=user, password=password)
    cur = conn.cursor()

    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()

    for query in create_table_queries:
        cur.execute(query)
        conn.commit()

    conn.close()


def populate_tables(spark, df_clean_immigration, df_clean_demographics, df_clean_temperature,
                    user, password, dbname, host):
    """Connects to the database and populate the tables with data"""
    views = ["immigration", "demographics", "temperature"]
    dfs = [df_clean_immigration, df_clean_demographics, df_clean_temperature]

    for df, view in zip(dfs, views):
        df.createOrReplaceTempView(view)

    populate_queries = populate_build_queries(*views)

    mode = "append"
    url = f"jdbc:postgresql://{host}/{dbname}"
    properties = {"user": user, "password": password, "driver": "org.postgresql.Driver"}
    for query, table_name in zip(populate_queries, tables_names):
        df = spark.sql(query)
        df.write.jdbc(url=url, table=table_name, mode=mode, properties=properties)


def quality_checks(table_name, user, password, dbname, host):
    """Checks if there is any rows in the database"""
    conn = psycopg2.connect(host=host, dbname=dbname, user=user, password=password)
    cur = conn.cursor()

    cur.execute(quality_check_query.format(table_name))
    rows = cur.fetchone()[0]
    conn.close()

    if rows == 0:
        print(f"{table_name} failed quality check (zero records)")
    else:
        print(f"{table_name} passed quality check ({rows})")
    return 0
