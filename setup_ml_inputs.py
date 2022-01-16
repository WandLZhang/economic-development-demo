from google.cloud import bigquery
import global_variables
import pandas as pd

# Construct a BigQuery client object.
bq = bigquery.Client(project=global_variables.project)

# Set staging dataset variables
staging_dataset_id = "{}.{}".format(bq.project,global_variables.staging_dataset_name)
staging_dataset = bigquery.Dataset(staging_dataset_id)

# Create table for census tracts with most disengaged labor force, per state
table_id = staging_dataset_id + ".max_not_in_labor_per_state"
ref_table_id = staging_dataset_id + ".census_tract_all"
ref_table_id_2 = staging_dataset_id + ".state_fips"
base_query_string_1 = """CREATE OR REPLACE TABLE
  `{0}` AS (
  WITH
    max_tracts AS (
    SELECT
      CAST(fips.fips AS string) AS fips,
      MAX(prime_age_not_in_labor_force) AS prime_age_not_in_labor_force
    FROM
      `{1}` AS census
    JOIN
      `{2}` fips
    ON
      LEFT(census.geo_id,2) = fips.fips
    GROUP BY
      fips)
  SELECT
    sf.postal_code AS state,
    geo_id,
    max_tracts.prime_age_not_in_labor_force,
    percent_not_in_labor_force
  FROM
    `{1}` AS census
  INNER JOIN
    max_tracts
  ON
    LEFT(census.geo_id,2) = max_tracts.fips
    AND census.prime_age_not_in_labor_force = max_tracts.prime_age_not_in_labor_force
  INNER JOIN
    `{2}` sf
  ON
    sf.fips = max_tracts.fips)
"""
updated_query_string_2 = base_query_string_1.format(table_id, ref_table_id, ref_table_id_2)

print("Loading {0}".format(table_id))
bq.query(updated_query_string_2).result() # Wait for the query to complete
print("Query results loaded to the table {0}".format(table_id))

# Create input table for economic impact ML model
table_id = staging_dataset_id + ".ml_input_econ_impact"
ref_table_id = staging_dataset_id + ".max_not_in_labor_per_state"
ref_table_id_2 = "models.bqml_economic_impact"
base_query_string_1 = """CREATE OR REPLACE TABLE
  `{0}` AS (
  WITH
    input_table AS (
    SELECT
      geo_id,
      percent_not_in_labor_force
    FROM
      `{1}`)
  SELECT
    state,
    a.geo_id,
    a.percent_not_in_labor_force,
    prime_age_not_in_labor_force,
    predicted_label AS baseline_avg_income
  FROM
    ML.PREDICT(MODEL `{2}`,
      TABLE input_table) a
  JOIN
    `{1}` b
  ON
    a.geo_id = b.geo_id )
"""
updated_query_string_2 = base_query_string_1.format(table_id, ref_table_id, ref_table_id_2)

print("Loading {0}".format(table_id))
bq.query(updated_query_string_2).result() # Wait for the query to complete
print("Query results loaded to the table {0}".format(table_id))

# Create input table for AutoML baseline (for feature importance demo)
table_id = staging_dataset_id + ".ml_input_automl_baseline"
ref_table_id = staging_dataset_id + ".census_tract_all"
base_query_string_1 = """CREATE OR REPLACE TABLE
  `{0}` AS (
WITH
  max_tracts AS (
  SELECT
    state,
    geo_id,
    commuters_by_public_transportation,
    vacant_housing_units,
    vacant_housing_units_for_rent,
    vacant_housing_units_for_sale,
    percent_income_spent_on_rent,
    families_with_young_children,
    commute_10_14_mins,
    commute_15_19_mins,
    commute_20_24_mins,
    commute_25_29_mins,
    commute_30_34_mins,
    commute_45_59_mins,
    renter_occupied_housing_units_paying_cash_median_gross_rent,
    mobile_homes,
    housing_built_2005_or_later,
    commute_5_9_mins,
    commute_35_39_mins,
    commute_40_44_mins,
    commute_60_89_mins,
    commute_90_more_mins,
    commuters_by_bus,
    commuters_by_carpool,
    commuters_by_subway_or_elevated,
    commuters_drove_alone,
    employed_agriculture_forestry_fishing_hunting_mining,
    employed_arts_entertainment_recreation_accommodation_food,
    employed_construction,
    employed_education_health_social,
    employed_finance_insurance_real_estate,
    employed_information,
    employed_manufacturing,
    employed_other_services_not_public_admin,
    employed_public_administration,
    employed_retail_trade,
    employed_science_management_admin_waste,
    employed_transportation_warehousing_utilities,
    employed_wholesale_trade,
    high_school_including_ged,
    households_public_asst_or_food_stamps,
    in_school,
    in_undergrad_college,
    male_45_64_associates_degree,
    male_45_64_bachelors_degree,
    male_45_64_graduate_degree,
    male_45_64_less_than_9_grade,
    male_45_64_grade_9_12,
    male_45_64_high_school,
    male_45_64_some_college,
    management_business_sci_arts_employed,
    not_us_citizen_pop,
    occupation_management_arts,
    occupation_natural_resources_construction_maintenance,
    occupation_production_transportation_material,
    occupation_sales_office,
    occupation_services,
    pop_determined_poverty_status,
    poverty,
    sales_office_employed,
    walked_to_work,
    worked_at_home,
    associates_degree,
    bachelors_degree,
    high_school_diploma,
    less_one_year_college,
    one_year_more_college,
    commute_35_44_mins,
    commute_60_more_mins,
    commute_less_10_mins,
    commuters_16_over,
    median_income_diff,
    ROW_NUMBER() OVER (PARTITION BY state ORDER BY prime_age_not_in_labor_force DESC) AS pop_rank
  FROM
    `{1}` AS census )
SELECT
  * EXCEPT(pop_rank)
FROM
  max_tracts
WHERE
  pop_rank <= 2 
  )
"""
updated_query_string_2 = base_query_string_1.format(table_id, ref_table_id)

print("Loading {0}".format(table_id))
bq.query(updated_query_string_2).result() # Wait for the query to complete
print("Query results loaded to the table {0}".format(table_id))

# Create staging table for AutoML scenario input
table_id = staging_dataset_id + ".max_automl_scenario"
ref_table_id = staging_dataset_id + ".ml_input_automl_baseline"
ref_table_id_2 = staging_dataset_id + ".max_not_in_labor_per_state"
base_query_string_1 = """CREATE OR REPLACE TABLE `{0}` AS 
SELECT input.*
FROM `{1}` input 
INNER JOIN `{2}` max_tracts on max_tracts.geo_id = input.geo_id
"""
updated_query_string_2 = base_query_string_1.format(table_id, ref_table_id, ref_table_id_2)

print("Loading {0}".format(table_id))
bq.query(updated_query_string_2).result() # Wait for the query to complete
print("Query results loaded to the table {0}".format(table_id))

# Load states into array to prepare for AutoML scenario generation
state_csv = pd.read_csv('state_fips.csv',dtype=str)
states = state_csv['postal_code'].values 

## Create scenarios of increasing high school graduate population (by appending rows)
for i in range(len(states)):
    for j in range(6):
        query = """SELECT * except (high_school_including_ged), high_school_including_ged*POW(1.5,{0}) as high_school_including_ged FROM `{1}.max_automl_scenario` 
        WHERE state = "{2}" 
        """
        updated_query = query.format(j, staging_dataset_id,states[i])
        table_id = staging_dataset_id + ".ml_input_automl_scenarios"
        job_config = bigquery.QueryJobConfig(destination=table_id)
        job_config.write_disposition = 'WRITE_APPEND'
        query_job = bq.query(updated_query,job_config=job_config)
        query_job.result()  # Waits for job to complete.
        print("Query results loaded to the table {0}, State: {1}, scenario {2}".format(table_id, states[i], j))