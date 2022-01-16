from google.cloud import bigquery
from google.cloud import automl_v1beta1
from google.oauth2 import service_account
from time import sleep
import global_variables

# Construct a BigQuery client object.
bq = bigquery.Client(project=global_variables.project)

# Create models dataset
staging_dataset_id = "{}.{}".format(bq.project,global_variables.staging_dataset_name)
models_dataset_id = "{}.{}".format(bq.project,global_variables.models_dataset_name)
models_dataset = bigquery.Dataset(models_dataset_id)

# Raises google.api_core.exceptions.Conflict if the Dataset already
# exists within the project.
try:
    models_dataset = bq.create_dataset(models_dataset, timeout=30)  # Make an API request.
    print("Created dataset {}.{}".format(bq.project, models_dataset.dataset_id))
except:
    pass

# Create automl_labor_scenarios model
table_id = models_dataset_id + ".automl_labor_scenarios"
ref_table_id = staging_dataset_id + ".census_tract_training"
base_query_string_1 = """CREATE OR REPLACE MODEL `{0}`
OPTIONS(model_type='AUTOML_REGRESSOR',
               budget_hours=2.0) AS
SELECT
  percent_not_in_labor_force as label,
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
    median_income_diff
FROM
  `{1}`"""
updated_query_string_2 = base_query_string_1.format(table_id,ref_table_id)

query_job = bq.query(
    updated_query_string_2,
    # The client libraries automatically generate a job ID. Override the
    # generated ID with either the job_id_prefix or job_id parameters.
    job_id_prefix = "automl_labor_scenarios_",
)

print("Started job: {}".format(query_job.job_id))

# Create bqml_economic_impact model
table_id = models_dataset_id + ".bqml_economic_impact"
ref_table_id = staging_dataset_id + ".census_tract_training"
base_query_string_1 = """CREATE OR REPLACE MODEL `{0}`
OPTIONS(model_type='linear_reg') AS 
SELECT 
    geo_id,
    percent_not_in_labor_force,
    income_per_capita as label
FROM `{1}`"""
updated_query_string_2 = base_query_string_1.format(table_id,ref_table_id)

query_job = bq.query(
    updated_query_string_2,
    # The client libraries automatically generate a job ID. Override the
    # generated ID with either the job_id_prefix or job_id parameters.
    job_id_prefix = "bqml_economic_impact_",
)

print("Started job: {}".format(query_job.job_id))

# Create client for AutoML Tables
automl = automl_v1beta1.TablesClient(
    credentials=service_account.Credentials.from_service_account_file('token.json'),
    project=global_variables.project,region=global_variables.region)

# Set target column. If dataset hasn't imported, operation will retry with backoff
result = None
sleep_time = 2

while result is None:
    try:
        result = automl.set_target_column(dataset_display_name='economic_development',
        column_spec_display_name='percent_not_in_labor_force')
    except:
        print("Retrying setting AutoML target column in {} seconds".format(sleep_time))
        sleep(sleep_time) # wait before trying operation again
        sleep_time *= 2   # exponential backoff

# change geo_id to Numeric to Categorical
automl.update_column_spec(dataset_display_name='economic_development',
    column_spec_display_name='geo_id', type_code=10)

# Train ML model
model_name = 'labor_force_prediction'

response = automl.create_model(
    model_name,
    dataset_display_name='economic_development',
    train_budget_milli_node_hours=2000,
    include_column_spec_names=['state','geo_id','commuters_by_public_transportation','vacant_housing_units','vacant_housing_units_for_rent','vacant_housing_units_for_sale','percent_income_spent_on_rent','families_with_young_children','commute_10_14_mins','commute_15_19_mins','commute_20_24_mins','commute_25_29_mins','commute_30_34_mins','commute_45_59_mins','renter_occupied_housing_units_paying_cash_median_gross_rent','mobile_homes','housing_built_2005_or_later','commute_5_9_mins','commute_35_39_mins','commute_40_44_mins','commute_60_89_mins','commute_90_more_mins','commuters_by_bus','commuters_by_carpool','commuters_by_subway_or_elevated','commuters_drove_alone','employed_agriculture_forestry_fishing_hunting_mining','employed_arts_entertainment_recreation_accommodation_food','employed_construction','employed_education_health_social','employed_finance_insurance_real_estate','employed_information','employed_manufacturing','employed_other_services_not_public_admin','employed_public_administration','employed_retail_trade','employed_science_management_admin_waste','employed_transportation_warehousing_utilities','employed_wholesale_trade','high_school_including_ged','households_public_asst_or_food_stamps','in_school','in_undergrad_college','male_45_64_associates_degree','male_45_64_bachelors_degree','male_45_64_graduate_degree','male_45_64_less_than_9_grade','male_45_64_grade_9_12','male_45_64_high_school','male_45_64_some_college','management_business_sci_arts_employed','not_us_citizen_pop','occupation_management_arts','occupation_natural_resources_construction_maintenance','occupation_production_transportation_material','occupation_sales_office','occupation_services','pop_determined_poverty_status','poverty','sales_office_employed','walked_to_work','worked_at_home','associates_degree','bachelors_degree','high_school_diploma','less_one_year_college','one_year_more_college','commute_35_44_mins','commute_60_more_mins','commute_less_10_mins','commuters_16_over','median_income_diff']
)

def callback(operation_future):
   result = operation_future.result()

print("Training model...")
print("Training operation: {}".format(response.operation))
print("Training operation name: {}".format(response.operation.name))

response.add_done_callback(callback)