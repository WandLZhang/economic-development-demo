from google.cloud import bigquery
from google.cloud import automl_v1beta1
from google.oauth2 import service_account
import global_variables

# Construct a BigQuery client object.
bq = bigquery.Client(project=global_variables.project)

# Create staging dataset
staging_dataset_id = "{}.{}".format(bq.project,global_variables.staging_dataset_name)
staging_dataset = bigquery.Dataset(staging_dataset_id)
# Raises google.api_core.exceptions.Conflict if the Dataset already
# exists within the project.
try:
    staging_dataset = bq.create_dataset(staging_dataset, timeout=30)  # Make an API request.
    print("Created dataset {}.{}".format(bq.project, staging_dataset.dataset_id))
except:
    pass

# Create median_income_diff_by_tract table
table_id = staging_dataset_id + ".median_income_diff_by_tract"
base_query_string_1 = """CREATE OR REPLACE TABLE `{0}` AS(
  WITH
    acs_2018 AS (
    SELECT
      geo_id,
      median_income AS median_income_2018
    FROM
      `bigquery-public-data.census_bureau_acs.censustract_2018_5yr` ),
    acs_2015 AS (
    SELECT
      geo_id,
      median_income AS median_income_2015
    FROM
      `bigquery-public-data.census_bureau_acs.censustract_2015_5yr` ),
    acs_diff AS (
    SELECT
      a18 .geo_id,
      a18.median_income_2018,
      a15.median_income_2015,
      (a18.median_income_2018 - a15.median_income_2015) AS median_income_diff,
    FROM
      acs_2018 a18
    JOIN
      acs_2015 a15
    ON
      a18.geo_id = a15.geo_id )
  SELECT
    *
  FROM
    acs_diff
  WHERE
    median_income_diff IS NOT NULL )"""
updated_query_string_2 = base_query_string_1.format(table_id)

print("Loading {0}".format(table_id))
bq.query(updated_query_string_2).result() # Wait for the query to complete
print("Query results loaded to the table {0}".format(table_id))

# Create census_zip table
table_id = staging_dataset_id + ".census_zip"
ref_table_id = staging_dataset_id + ".median_income_diff_by_zipcode"
base_query_string_1 = """CREATE OR REPLACE TABLE `{0}` AS(
  WITH base_census AS (SELECT
geo.state_name,
census.*,
i.median_income_diff,
employed_wholesale_trade * 0.38423645320197042 as employed_wholesale_trade_vulnerable,
occupation_natural_resources_construction_maintenance * 	
0.48071410777129553 as occupation_natural_resources_construction_maintenance_vulnerable,
employed_arts_entertainment_recreation_accommodation_food * 0.89455676291236841 as employed_arts_entertainment_recreation_accommodation_food_vulnerable,
employed_information * 	
0.31315240083507306 as employed_information_vulernable,
employed_retail_trade * 0.51 as employed_retail_trade_vulnerable,
employed_public_administration * 0.039299298394228743 as employed_public_administration_vulnerable,
occupation_services * 	
0.36555534476489654 as occupation_services_vulnerable,
employed_education_health_social * 	
0.20323178400562944 as employed_education_health_social_vulnerable,
employed_transportation_warehousing_utilities * 0.3680506593618087 as employed_transportation_warehousing_utilities_vulnerable,
employed_manufacturing * 0.40618955512572535 as employed_manufacturing_vulnerable,
ST_SIMPLIFY(geo.zip_code_geom,2100) AS zip_geom,
FROM `bigquery-public-data.census_bureau_acs.zip_codes_2017_5yr` as census
JOIN `{1}` i ON cast(census.geo_id as string) = i.geo_id
JOIN `bigquery-public-data.geo_us_boundaries.zip_codes` geo ON census.geo_id = geo.zip_code)

SELECT 
base_census.*,
occupation_natural_resources_construction_maintenance_vulnerable + 
employed_arts_entertainment_recreation_accommodation_food_vulnerable +	
employed_information_vulernable+	
employed_retail_trade_vulnerable +	
employed_public_administration_vulnerable	+
occupation_services_vulnerable	+	
employed_education_health_social_vulnerable	+
employed_transportation_warehousing_utilities_vulnerable+	
employed_manufacturing_vulnerable+
employed_wholesale_trade_vulnerable as total_vulnerable
FROM base_census )"""
updated_query_string_2 = base_query_string_1.format(table_id, ref_table_id)

print("Loading {0}".format(table_id))
bq.query(updated_query_string_2).result() # Wait for the query to complete
print("Query results loaded to the table {0}".format(table_id))

# Create census_zip_xjoin_industries table
table_id = staging_dataset_id + ".census_zip_xjoin_industries"
ref_table_id = staging_dataset_id + ".census_zip"
base_query_string_1 = """CREATE OR REPLACE TABLE `{0}` AS(
  SELECT 
*,
FROM `economic-development-demo.external.vulnerable_industries`
CROSS JOIN `{1}`)"""
updated_query_string_2 = base_query_string_1.format(table_id, ref_table_id)

print("Loading {0}".format(table_id))
bq.query(updated_query_string_2).result() # Wait for the query to complete
print("Query results loaded to the table {0}".format(table_id))

# Create census_zip_vuln_workers table
table_id = staging_dataset_id + ".census_zip_vuln_workers"
ref_table_id = staging_dataset_id + ".census_zip_xjoin_industries"
base_query_string_1 = """CREATE OR REPLACE TABLE `{0}` AS(
  SELECT 
*,
CASE when occupation = "Retail Trade" then employed_retail_trade * percent_jobs_vulnerable
    when occupation = "Financial Activities" then employed_finance_insurance_real_estate * percent_jobs_vulnerable
    when occupation = "Mining, Logging, and Construction" then occupation_natural_resources_construction_maintenance * 	percent_jobs_vulnerable
    when occupation = "Manufacturing" then employed_manufacturing * percent_jobs_vulnerable
    when occupation = "Other Services" then employed_other_services_not_public_admin * percent_jobs_vulnerable
    when occupation = "Wholesale Trade" then employed_wholesale_trade * percent_jobs_vulnerable
    when occupation  = "Transportation and Utilities" then employed_transportation_warehousing_utilities * percent_jobs_vulnerable
    when occupation = "Education and Health Services" then employed_education_health_social * percent_jobs_vulnerable
    when occupation = "Leisure and Hospitality" then employed_arts_entertainment_recreation_accommodation_food * percent_jobs_vulnerable
    when occupation = "Professional and Business Services" then occupation_services * percent_jobs_vulnerable
    when occupation = "Government" then employed_public_administration * percent_jobs_vulnerable
    when occupation = "Information" then employed_information * percent_jobs_vulnerable
 end as vulnerable_employees,
 RAND()*100 as num_claims 
FROM `{1}`)"""
updated_query_string_2 = base_query_string_1.format(table_id, ref_table_id)

print("Loading {0}".format(table_id))
bq.query(updated_query_string_2).result() # Wait for the query to complete
print("Query results loaded to the table {0}".format(table_id))

# Create curated dataset
curated_dataset_id = "{}.{}".format(bq.project,global_variables.curated_dataset_name)
curated_dataset = bigquery.Dataset(curated_dataset_id)
# Raises google.api_core.exceptions.Conflict if the Dataset already
# exists within the project.
try:
    curated_dataset = bq.create_dataset(curated_dataset, timeout=30)  # Make an API request.
    print("Created dataset {}.{}".format(bq.project, curated_dataset.dataset_id))
except:
    pass

# Create census_zip_non_labor table
table_id = curated_dataset_id + ".census_zip_non_labor"
ref_table_id = staging_dataset_id + ".census_zip_vuln_workers"
base_query_string_1 = """CREATE OR REPLACE TABLE `{0}` AS(
SELECT 
* ,
CASE 
    when unemployed_pop
    +not_in_labor_force #non labor force above 16 years old
    -group_quarters #incarcerated
    -(in_school - male_under_5-male_5_to_9-male_10_to_14-male_15_to_17-
    female_under_5-female_5_to_9-female_10_to_14-female_15_to_17) #students above 17 years old
    -(male_65_to_66+male_67_to_69+male_70_to_74+male_75_to_79+male_80_to_84+	
    male_85_and_over+female_62_to_64+female_65_to_66+female_67_to_69+
    female_70_to_74+female_75_to_79+female_80_to_84) #median retirement age
    < 0 then 0
    when unemployed_pop
    +not_in_labor_force #non labor force above 16 years old
    -group_quarters #incarcerated
    -(in_school - male_under_5-male_5_to_9-male_10_to_14-male_15_to_17-
    female_under_5-female_5_to_9-female_10_to_14-female_15_to_17) #students above 17 years old
    -(male_65_to_66+male_67_to_69+male_70_to_74+male_75_to_79+male_80_to_84+	
    male_85_and_over+female_62_to_64+female_65_to_66+female_67_to_69+
    female_70_to_74+female_75_to_79+female_80_to_84) #median retirement age 
    >= 0 then unemployed_pop
    +not_in_labor_force #non labor force above 16 years old
    -group_quarters #incarcerated
    -(in_school - male_under_5-male_5_to_9-male_10_to_14-male_15_to_17-
    female_under_5-female_5_to_9-female_10_to_14-female_15_to_17) #students above 17 years old
    -(male_65_to_66+male_67_to_69+male_70_to_74+male_75_to_79+male_80_to_84+	
    male_85_and_over+female_62_to_64+female_65_to_66+female_67_to_69+
    female_70_to_74+female_75_to_79+female_80_to_84) #median retirement age
end as prime_age_not_in_labor_force,
CASE
    when SAFE_DIVIDE(unemployed_pop
    +not_in_labor_force #non labor force above 16 years old
    -group_quarters #incarcerated
    -(in_school - male_under_5-male_5_to_9-male_10_to_14-male_15_to_17-
    female_under_5-female_5_to_9-female_10_to_14-female_15_to_17) #students above 17 years old
    -(male_65_to_66+male_67_to_69+male_70_to_74+male_75_to_79+male_80_to_84+	
    male_85_and_over+female_62_to_64+female_65_to_66+female_67_to_69+
    female_70_to_74+female_75_to_79+female_80_to_84) #median retirement age
    , total_pop) < 0 then 0
    when SAFE_DIVIDE(unemployed_pop
    +not_in_labor_force #non labor force above 16 years old
    -group_quarters #incarcerated
    -(in_school - male_under_5-male_5_to_9-male_10_to_14-male_15_to_17-
    female_under_5-female_5_to_9-female_10_to_14-female_15_to_17) #students above 17 years old
    -(male_65_to_66+male_67_to_69+male_70_to_74+male_75_to_79+male_80_to_84+	
    male_85_and_over+female_62_to_64+female_65_to_66+female_67_to_69+
    female_70_to_74+female_75_to_79+female_80_to_84) #median retirement age
    , total_pop) >= 0 then SAFE_DIVIDE(unemployed_pop
    +not_in_labor_force #non labor force above 16 years old
    -group_quarters #incarcerated
    -(in_school - male_under_5-male_5_to_9-male_10_to_14-male_15_to_17-
    female_under_5-female_5_to_9-female_10_to_14-female_15_to_17) #students above 17 years old
    -(male_65_to_66+male_67_to_69+male_70_to_74+male_75_to_79+male_80_to_84+	
    male_85_and_over+female_62_to_64+female_65_to_66+female_67_to_69+
    female_70_to_74+female_75_to_79+female_80_to_84) #median retirement age
    , total_pop)
END as percent_not_in_labor_force
FROM `{1}`)"""
updated_query_string_2 = base_query_string_1.format(table_id, ref_table_id)

print("Loading {0}".format(table_id))
bq.query(updated_query_string_2).result() # Wait for the query to complete
print("Query results loaded to the table {0}".format(table_id))


# Upload state FIPS reference table
filename = "state_fips.csv"
table_id = 'state_fips'
dataset_ref = bq.dataset(global_variables.staging_dataset_name)
table_ref = dataset_ref.table(table_id)
job_config = bigquery.LoadJobConfig()
job_config.source_format = bigquery.SourceFormat.CSV
job_config.schema = schema=[
        bigquery.SchemaField("state", "STRING"),
        bigquery.SchemaField("postal_code", "STRING"),
        bigquery.SchemaField("fips", "STRING")
]
job_config.skip_leading_rows = 1
job_config.write_disposition = bigquery.job.WriteDisposition.WRITE_TRUNCATE

# load the csv into bigquery
with open(filename, "rb") as source_file:
    job = bq.load_table_from_file(source_file, table_ref, job_config=job_config)

job.result()  # Waits for table load to complete.

print("Loaded {} rows into {}:{}.".format(job.output_rows, global_variables.staging_dataset_name, table_id))

# Create census_tract_training table
table_id = staging_dataset_id + ".census_tract_training"
ref_table_id = staging_dataset_id + ".median_income_diff_by_tract"
ref_table_id_2 = staging_dataset_id + ".state_fips"
base_query_string_1 = """CREATE OR REPLACE TABLE `{0}` AS(
SELECT
sf.postal_code as state,
census.* except(do_date,civilian_labor_force,pop_in_labor_force,pop_16_over,employed_pop,unemployed_pop,
pop_25_years_over,population_1_year_and_over,population_3_years_over,total_pop,pop_25_64,workers_16_and_over,not_in_labor_force),
i.median_income_diff,
CASE 
    when unemployed_pop
    +not_in_labor_force #non labor force above 16 years old
    -group_quarters #incarcerated
    -(in_school - male_under_5-male_5_to_9-male_10_to_14-male_15_to_17-
    female_under_5-female_5_to_9-female_10_to_14-female_15_to_17) #students above 17 years old
    -(male_65_to_66+male_67_to_69+male_70_to_74+male_75_to_79+male_80_to_84+	
    male_85_and_over+female_62_to_64+female_65_to_66+female_67_to_69+
    female_70_to_74+female_75_to_79+female_80_to_84) #median retirement age
    < 0 then 0
    when unemployed_pop
    +not_in_labor_force #non labor force above 16 years old
    -group_quarters #incarcerated
    -(in_school - male_under_5-male_5_to_9-male_10_to_14-male_15_to_17-
    female_under_5-female_5_to_9-female_10_to_14-female_15_to_17) #students above 17 years old
    -(male_65_to_66+male_67_to_69+male_70_to_74+male_75_to_79+male_80_to_84+	
    male_85_and_over+female_62_to_64+female_65_to_66+female_67_to_69+
    female_70_to_74+female_75_to_79+female_80_to_84) #median retirement age 
    >= 0 then unemployed_pop
    +not_in_labor_force #non labor force above 16 years old
    -group_quarters #incarcerated
    -(in_school - male_under_5-male_5_to_9-male_10_to_14-male_15_to_17-
    female_under_5-female_5_to_9-female_10_to_14-female_15_to_17) #students above 17 years old
    -(male_65_to_66+male_67_to_69+male_70_to_74+male_75_to_79+male_80_to_84+	
    male_85_and_over+female_62_to_64+female_65_to_66+female_67_to_69+
    female_70_to_74+female_75_to_79+female_80_to_84) #median retirement age
end as prime_age_not_in_labor_force,
CASE
    when SAFE_DIVIDE(unemployed_pop
    +not_in_labor_force #non labor force above 16 years old
    -group_quarters #incarcerated
    -(in_school - male_under_5-male_5_to_9-male_10_to_14-male_15_to_17-
    female_under_5-female_5_to_9-female_10_to_14-female_15_to_17) #students above 17 years old
    -(male_65_to_66+male_67_to_69+male_70_to_74+male_75_to_79+male_80_to_84+	
    male_85_and_over+female_62_to_64+female_65_to_66+female_67_to_69+
    female_70_to_74+female_75_to_79+female_80_to_84) #median retirement age
    , total_pop) < 0 then 0
    when SAFE_DIVIDE(unemployed_pop
    +not_in_labor_force #non labor force above 16 years old
    -group_quarters #incarcerated
    -(in_school - male_under_5-male_5_to_9-male_10_to_14-male_15_to_17-
    female_under_5-female_5_to_9-female_10_to_14-female_15_to_17) #students above 17 years old
    -(male_65_to_66+male_67_to_69+male_70_to_74+male_75_to_79+male_80_to_84+	
    male_85_and_over+female_62_to_64+female_65_to_66+female_67_to_69+
    female_70_to_74+female_75_to_79+female_80_to_84) #median retirement age
    , total_pop) >= 0 then SAFE_DIVIDE(unemployed_pop
    +not_in_labor_force #non labor force above 16 years old
    -group_quarters #incarcerated
    -(in_school - male_under_5-male_5_to_9-male_10_to_14-male_15_to_17-
    female_under_5-female_5_to_9-female_10_to_14-female_15_to_17) #students above 17 years old
    -(male_65_to_66+male_67_to_69+male_70_to_74+male_75_to_79+male_80_to_84+	
    male_85_and_over+female_62_to_64+female_65_to_66+female_67_to_69+
    female_70_to_74+female_75_to_79+female_80_to_84) #median retirement age
    , total_pop)
END as percent_not_in_labor_force
FROM `bigquery-public-data.census_bureau_acs.censustract_2017_5yr` as census
JOIN `{1}` i ON cast(census.geo_id as string) = i.geo_id
JOIN `{2}` sf ON census.geo_id LIKE concat(sf.fips,"%")
)"""
updated_query_string_2 = base_query_string_1.format(table_id, ref_table_id, ref_table_id_2)

print("Loading {0}".format(table_id))
bq.query(updated_query_string_2).result() # Wait for the query to complete
print("Query results loaded to the table {0}".format(table_id))

# Create census_tract_all table
table_id = staging_dataset_id + ".census_tract_all"
ref_table_id = staging_dataset_id + ".median_income_diff_by_tract"
ref_table_id_2 = staging_dataset_id + ".state_fips"
base_query_string_1 = """CREATE OR REPLACE TABLE `{0}` AS(
SELECT
sf.postal_code as state,
census.*,
i.median_income_diff,
CASE 
    when unemployed_pop
    +not_in_labor_force #non labor force above 16 years old
    -group_quarters #incarcerated
    -(in_school - male_under_5-male_5_to_9-male_10_to_14-male_15_to_17-
    female_under_5-female_5_to_9-female_10_to_14-female_15_to_17) #students above 17 years old
    -(male_65_to_66+male_67_to_69+male_70_to_74+male_75_to_79+male_80_to_84+	
    male_85_and_over+female_62_to_64+female_65_to_66+female_67_to_69+
    female_70_to_74+female_75_to_79+female_80_to_84) #median retirement age
    < 0 then 0
    when unemployed_pop
    +not_in_labor_force #non labor force above 16 years old
    -group_quarters #incarcerated
    -(in_school - male_under_5-male_5_to_9-male_10_to_14-male_15_to_17-
    female_under_5-female_5_to_9-female_10_to_14-female_15_to_17) #students above 17 years old
    -(male_65_to_66+male_67_to_69+male_70_to_74+male_75_to_79+male_80_to_84+	
    male_85_and_over+female_62_to_64+female_65_to_66+female_67_to_69+
    female_70_to_74+female_75_to_79+female_80_to_84) #median retirement age 
    >= 0 then unemployed_pop
    +not_in_labor_force #non labor force above 16 years old
    -group_quarters #incarcerated
    -(in_school - male_under_5-male_5_to_9-male_10_to_14-male_15_to_17-
    female_under_5-female_5_to_9-female_10_to_14-female_15_to_17) #students above 17 years old
    -(male_65_to_66+male_67_to_69+male_70_to_74+male_75_to_79+male_80_to_84+	
    male_85_and_over+female_62_to_64+female_65_to_66+female_67_to_69+
    female_70_to_74+female_75_to_79+female_80_to_84) #median retirement age
end as prime_age_not_in_labor_force,
CASE
    when SAFE_DIVIDE(unemployed_pop
    +not_in_labor_force #non labor force above 16 years old
    -group_quarters #incarcerated
    -(in_school - male_under_5-male_5_to_9-male_10_to_14-male_15_to_17-
    female_under_5-female_5_to_9-female_10_to_14-female_15_to_17) #students above 17 years old
    -(male_65_to_66+male_67_to_69+male_70_to_74+male_75_to_79+male_80_to_84+	
    male_85_and_over+female_62_to_64+female_65_to_66+female_67_to_69+
    female_70_to_74+female_75_to_79+female_80_to_84) #median retirement age
    , total_pop) < 0 then 0
    when SAFE_DIVIDE(unemployed_pop
    +not_in_labor_force #non labor force above 16 years old
    -group_quarters #incarcerated
    -(in_school - male_under_5-male_5_to_9-male_10_to_14-male_15_to_17-
    female_under_5-female_5_to_9-female_10_to_14-female_15_to_17) #students above 17 years old
    -(male_65_to_66+male_67_to_69+male_70_to_74+male_75_to_79+male_80_to_84+	
    male_85_and_over+female_62_to_64+female_65_to_66+female_67_to_69+
    female_70_to_74+female_75_to_79+female_80_to_84) #median retirement age
    , total_pop) >= 0 then SAFE_DIVIDE(unemployed_pop
    +not_in_labor_force #non labor force above 16 years old
    -group_quarters #incarcerated
    -(in_school - male_under_5-male_5_to_9-male_10_to_14-male_15_to_17-
    female_under_5-female_5_to_9-female_10_to_14-female_15_to_17) #students above 17 years old
    -(male_65_to_66+male_67_to_69+male_70_to_74+male_75_to_79+male_80_to_84+	
    male_85_and_over+female_62_to_64+female_65_to_66+female_67_to_69+
    female_70_to_74+female_75_to_79+female_80_to_84) #median retirement age
    , total_pop)
END as percent_not_in_labor_force
FROM `bigquery-public-data.census_bureau_acs.censustract_2017_5yr` as census
JOIN `{1}` i ON cast(census.geo_id as string) = i.geo_id
JOIN `{2}` sf ON census.geo_id LIKE concat(sf.fips,"%")
)"""
updated_query_string_2 = base_query_string_1.format(table_id, ref_table_id, ref_table_id_2)

print("Loading {0}".format(table_id))
bq.query(updated_query_string_2).result() # Wait for the query to complete
print("Query results loaded to the table {0}".format(table_id))

# Create client for AutoML Tables
automl = automl_v1beta1.TablesClient(
    credentials=service_account.Credentials.from_service_account_file('token.json'),
    project=global_variables.project,region=global_variables.region)

# Create and import AutoML dataset
automl_dataset = automl.create_dataset(dataset_display_name='economic_development')
bq_automl_dataset_uri='bq://' + global_variables.project + '.' + global_variables.staging_dataset_name + '.census_tract_training'

response = automl.import_data(dataset=automl_dataset,
    bigquery_input_uri=bq_automl_dataset_uri)

def callback(operation_future):
   result = operation_future.result()

response.add_done_callback(callback)