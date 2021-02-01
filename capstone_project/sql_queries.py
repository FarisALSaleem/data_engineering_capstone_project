# TABLE NAME
dim_country = "dim_country"
dim_state = "dim_state"
dim_transportation = "dim_transportation"
dim_visa_category = "dim_visa_category"
dim_arrival_date = "dim_arrival_date"
dim_departure_date = "dim_departure_date"
dim_record_date = "dim_record_date"
dim_allowed_stay = "dim_allowed_stay"
fact_immigration = "fact_immigration"

# DROP TABLES
dim_country_table_drop = "DROP TABLE IF EXISTS dim_country;"
dim_state_table_drop = "DROP TABLE IF EXISTS dim_state;"
dim_transportation_table_drop = "DROP TABLE IF EXISTS dim_transportation;"
dim_visa_category_table_drop = "DROP TABLE IF EXISTS dim_visa_category;"
dim_arrival_date_table_drop = "DROP TABLE IF EXISTS dim_arrival_date;"
dim_departure_date_table_drop = "DROP TABLE IF EXISTS dim_departure_date;"
dim_record_date_table_drop = "DROP TABLE IF EXISTS dim_record_date;"
dim_allowed_stay_table_drop = "DROP TABLE IF EXISTS dim_allowed_stay;"
fact_immigration_table_drop = "DROP TABLE IF EXISTS fact_immigration;"

# CREATE TABLES

dim_country_table_create = ("""
    CREATE TABLE dim_country(
        country_code text PRIMARY KEY,
        country_name text,
        average_temperature float8
    );
""")

dim_state_table_create = ("""
    CREATE TABLE dim_state(
        state_code text PRIMARY KEY,
        state_name text,
        median_age float8,
        male_population bigint,
        female_population bigint,
        total_population bigint,
        veteran_population bigint,
        foreign_born_population bigint,
        average_hosehold_size float8
    );
""")

dim_transportation_table_create = ("""
    CREATE TABLE dim_transportation(
        transportation_method_id int PRIMARY KEY,
        transportation_method text
    );
""")

dim_visa_category_table_create = ("""
    CREATE TABLE dim_visa_category(
        visatype text PRIMARY KEY,
        purpose_of_travel text
    );
""")

dim_arrival_date_table_create = ("""
    CREATE TABLE dim_arrival_date(
        arrival_date date PRIMARY KEY,
        arrival_date_year int,
        arrival_date_month int,
        arrival_date_day int,
        arrival_date_week int,
        arrival_date_weekday int
    );
""")

dim_departure_date_table_create = ("""
    CREATE TABLE dim_departure_date(
        departure_date date PRIMARY KEY,
        departure_date_year int,
        departure_date_month int,
        departure_date_day int,
        departure_date_week int,
        departure_date_weekday int
    );
""")

dim_record_date_table_create = ("""
    CREATE TABLE dim_record_date(
        record_date date PRIMARY KEY,
        record_date_year int,
        record_date_month int,
        record_date_day int,
        record_date_week int,
        record_date_weekday int
    );
""")

dim_allowed_stay_table_create = ("""
    CREATE TABLE dim_allowed_stay(
        allowed_stay date PRIMARY KEY,
        allowed_stay_year int,
        allowed_stay_month int,
        allowed_stay_day int,
        allowed_stay_week int,
        allowed_stay_weekday int
    );
""")

fact_immigration_table_create = ("""
    CREATE TABLE fact_immigration(
        immigrant_id bigint PRIMARY KEY,
        visa_category text references dim_visa_category(visatype),
        birth_country_code text references dim_country(country_code),
        birth_year int,
        immigrant_age int,
        immigrant_gender text,
        residential_country_code text references dim_country(country_code),
        admission_port text,
        arrival_state_code text references dim_state(state_code),
        transportation_method_id int references dim_transportation(transportation_method_id),
        arrival_date date references dim_arrival_date(arrival_date),
        departure_date date references dim_departure_date(departure_date),
        record_date date references dim_record_date(record_date),
        arrival_status text,
        departure_status text,
        allowed_stay date references dim_allowed_stay(allowed_stay),
        flight_number text,
        airline text,
        admission_number bigint,
        num_year int,
        num_month int
    );
""")

# Populate data

dim_country_table_populate = ("""
    SELECT DISTINCT
      Country AS country_name,
      MAX(country_code) AS country_code,
      ROUND(AVG(AverageTemperature), 2) AS average_temperature
    FROM (
        SELECT * from {}
        ORDER BY dt DESC
        ) t1
    WHERE
        country_code IS NOT NULL
    GROUP BY
      country_name;
""")

dim_state_table_populate = ("""
    SELECT 
        `State Code` AS state_code,
        MAX(State) AS state_name,
        ROUND(AVG(`Median Age`), 2) AS median_age,
        SUM(`Male Population`) AS male_population,
        SUM(`Female Population`) AS female_population,
        SUM(`Total Population`) AS total_population,
        SUM(`Number of Veterans`) AS veteran_population,
        SUM(`Foreign-born`) AS foreign_born_population,
        ROUND(AVG(`Average Household Size`), 2) AS average_hosehold_size
    FROM {}
    WHERE `State Code` IS NOT NULL
    GROUP BY
      `State Code`;
""")

dim_transportation_table_populate = ("""
    SELECT DISTINCT
        IFNULL(i94mode, FLOAT(9)) as transportation_method_id,
        transportation_method
    FROM {};
""")

dim_visa_category_table_populate = ("""
    SELECT DISTINCT 
        visatype as visatype,
        visa_category as purpose_of_travel
    FROM {};
""")

dim_arrival_date_table_populate = ("""
    SELECT
        arrival_date,
        EXTRACT(year from arrival_date) as arrival_date_year,
        EXTRACT(month from arrival_date) as arrival_date_month,
        EXTRACT(day from arrival_date) as arrival_date_day,
        EXTRACT(week from arrival_date) as arrival_date_week,
        EXTRACT(dow  from arrival_date) as arrival_date_weekday
    FROM ( 
        SELECT DISTINCT 
            DATE_ADD('1960-01-01', CAST(arrdate as smallint)) as arrival_date
        FROM {}
        WHERE arrdate IS NOT NULL
    ) t1;
""")

dim_departure_date_table_populate = ("""
    SELECT
        departure_date,
        EXTRACT(year from departure_date) as departure_date_year,
        EXTRACT(month from departure_date) as departure_date_month,
        EXTRACT(day from departure_date) as departure_date_day,
        EXTRACT(week from departure_date) as departure_date_week,
        EXTRACT(dow  from departure_date) as departure_date_weekday
    FROM ( 
        SELECT DISTINCT 
            DATE_ADD('1960-01-01', CAST(depdate as smallint)) as departure_date
        FROM {}
        WHERE depdate IS NOT NULL
    ) t1;
""")

dim_record_date_table_populate = ("""
    SELECT
        record_date,
        EXTRACT(year from record_date) as record_date_year,
        EXTRACT(month from record_date) as record_date_month,
        EXTRACT(day from record_date) as record_date_day,
        EXTRACT(week from record_date) as record_date_week,
        EXTRACT(dow  from record_date) as record_date_weekday
    FROM ( 
        SELECT DISTINCT 
            TO_DATE(dtadfile,'yyyyMMdd') as record_date
        FROM {}
        WHERE dtadfile IS NOT NULL
    ) t1
    WHERE record_date IS NOT NULL;
""")

dim_allowed_stay_table_populate = ("""
    SELECT
        allowed_stay,
        EXTRACT(year from allowed_stay) as allowed_stay_year,
        EXTRACT(month from allowed_stay) as allowed_stay_month,
        EXTRACT(day from allowed_stay) as allowed_stay_day,
        EXTRACT(week from allowed_stay) as allowed_stay_week,
        EXTRACT(dow  from allowed_stay) as allowed_stay_weekday
    FROM ( 
        SELECT DISTINCT 
            TO_DATE(dtaddto,'MMddyyyy') as allowed_stay
        FROM {}
        WHERE dtaddto IS NOT NULL
    ) t1 
    WHERE allowed_stay IS NOT NULL ;
""")

fact_immigration_table_populate = ("""
    SELECT
        cicid AS immigrant_id,
        visatype AS visa_category,
        i94cit AS birth_country_code,
        biryear AS birth_year,
        i94bir AS immigrant_age,
        gender AS immigrant_gender,
        i94res AS residential_country_code,
        i94port AS admission_port,
        i94addr AS arrival_state_code,
        i94mode AS transportation_method_id,
        DATE_ADD('1960-01-01', CAST(arrdate AS smallint)) AS arrival_date,
        DATE_ADD('1960-01-01', CAST(depdate AS smallint)) AS departure_date,
        TO_DATE(dtadfile,'yyyyMMdd') AS record_date,
        entdepa AS arrival_status,
        entdepd AS departure_status,
        TO_DATE(dtaddto,'MMddyyyy') AS allowed_stay,
        fltno AS flight_number,
        airline AS airline,
        admnum AS admission_number,
        i94yr AS num_year,
        i94mon AS num_month
    FROM {} i
        WHERE 
            i94cit IN (SELECT country_code FROM {}) AND
            i94res IN (SELECT country_code FROM {}) AND
            i94addr IN (SELECT `State Code` FROM  {});
""")

# quality checks
quality_check_query = ("""
    SELECT COUNT(*) FROM {};
""")

# QUERY LISTS
create_table_queries = [dim_country_table_create, dim_state_table_create,
                        dim_transportation_table_create, dim_visa_category_table_create,
                        dim_arrival_date_table_create, dim_departure_date_table_create,
                        dim_record_date_table_create, dim_allowed_stay_table_create,
                        fact_immigration_table_create]

drop_table_queries = [fact_immigration_table_drop, dim_allowed_stay_table_drop,
                      dim_record_date_table_drop, dim_departure_date_table_drop,
                      dim_arrival_date_table_drop, dim_visa_category_table_drop,
                      dim_transportation_table_drop, dim_state_table_drop,
                      dim_country_table_drop]

tables_names = [dim_country, dim_state,
                dim_transportation, dim_visa_category,
                dim_arrival_date, dim_departure_date,
                dim_record_date, dim_allowed_stay,
                fact_immigration]


def populate_build_queries(immigration, demographics, temperature):
    immigration_queries = [dim_transportation_table_populate, dim_visa_category_table_populate,
                           dim_arrival_date_table_populate, dim_departure_date_table_populate,
                           dim_record_date_table_populate, dim_allowed_stay_table_populate]

    immigration_format_queries = [query.format(immigration) for query in immigration_queries]
    temperature_format_queries = [dim_country_table_populate.format(temperature)]
    demographics_format_queries = [dim_state_table_populate.format(demographics)]
    fact_format_queries = [fact_immigration_table_populate.format(immigration, temperature, temperature, demographics)]

    format_queries = temperature_format_queries + demographics_format_queries
    format_queries = format_queries + immigration_format_queries + fact_format_queries

    return format_queries
