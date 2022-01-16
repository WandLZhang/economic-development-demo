# How to setup an economic development demo
Create all the GCP resources required to present an analytics demo on workforce development for state governments

<p align="center">
  <img src="https://storage.googleapis.com/github-economic-development-demo/github-1.png" width="600px"/>
</p>

***Technical notes:*** the following capabilities are scripted programmatically
- Create a GCP project, attach the billing account, enable APIs, and download service account JSON token
- Use service accounts for BigQuery and AutoML operations
- Submitting asynchronous jobs to BigQuery and AutoML
- Use exponential backoff when retrying operation that is waiting on something to finish
- Almost got this: Exporting BigQuery tables to Google Sheets (you can’t do it through gcloud, you need to use dataframes)

### Steps

##### Part 1: Setup tables and models

1. In a terminal, clone the repository code:

`git clone https://github.com/WandLZhang/economic-development-demo.git`

2. In the `config.sh` file, set variable values for `USER_EMAIL`, `NEW_PROJECT_ID`, and `BILLING_ACCOUNT_ID`.

3. In your terminal, run the following to setup the GCP project, attach your billing ID, enable APIs, and setup a Service Account:

`sh ./setup_project.sh`

4. In the `global_variables.py` file, set variable value for `project`, and optionally set dataset names and region.

5. Run this Python script to create the starter tables in BigQuery and AutoML Tables:

`python3 setup_tables.py`

6. Run this Python script to start long-running jobs to create ML models in BQ and AutoML Tables:

`python3 setup_models.py`

##### Part 2 (after models from Part 1 complete): Find feature importance

7. Optional for live demos: deploy the AutoML Tables model, `labor_force_prediction`, which takes predicts labor force engagement based on 71 features, by running this script:

`python3 deploy_automl.py`

8. To prepare inputs for prediction, run this Python script:

`python3 setup_ml_inputs.py`

9. We’re going to use the `labor_force_prediction` AutoML Tables model to first get a baseline of the most important features for the 2 most disengaged census tracts in each state. Run the Python script below (it will take many minutes):

`python3 automl_baseline.py`

10. Install the following packages (not pip3). While not needed, for the current directions, the idea was to use gspread and dataframes to port the BigQuery tables in step 12 into Sheets automatically.

```
pip install pyarrow
pip install gspread
pip install df2gspread
pip install –upgrade argparse
pip install --upgrade google-api-python-client 
pip install --upgrade 'google-cloud-bigquery[pandas]'
```

11. Run the following script to get the feature importances for the top 2 disengaged tracts per state:

`python get_feature_importance.py`

Your tables should look like this afterwards:

<p align="center">
  <img src="https://storage.googleapis.com/github-economic-development-demo/github-2.png" width="600px"/>
</p>

#### Part 3: Use BQ ML to run predictions

12. The regression ML model, `bqml_economic_impact`, outputs an average income, given a census tract and % not in the labor force as inputs. Replace `<state_abbreviation>` when running this query:

```
WITH
 predicted_income AS (
 WITH
   input_table AS (
   SELECT
     geo_id,
     percent_not_in_labor_force - 0.01 AS percent_not_in_labor_force
   FROM
     `staging.ml_input_econ_impact`)
 SELECT
   predicted_label AS new_income,
   geo_id
 FROM
   ML.PREDICT(MODEL `models.bqml_economic_impact`,
     TABLE input_table))
SELECT
 state,
 predicted_income.geo_id,
 CAST(.01*safe_divide(prime_age_not_in_labor_force,
     percent_not_in_labor_force) AS int) AS new_labor_force,
 ROUND(new_income,2) AS new_income,
 CAST((new_income - baseline_avg_income) * safe_divide(prime_age_not_in_labor_force,
     percent_not_in_labor_force) AS int) AS net_income_increase
FROM
 predicted_income
JOIN
 `staging.ml_input_econ_impact` census
ON
 census.geo_id = predicted_income.geo_id
 # WHERE state = <state_abbreviation>
 ```
 
13. The AutoML Regressor model `automl_labor_scenarios` outputs predicted non-labor force percentage based on 71 features. Replace `<state_abbreviation>` when running this query:

```
WITH
 input_table AS (SELECT * FROM `staging.ml_input_automl_scenarios`
 # WHERE state = <state_abbreviation> )
SELECT
 cast(high_school_including_ged as int) as high_school_including_ged,
 predicted_label AS percent_not_in_labor_force,
FROM
 ML.PREDICT(MODEL `models.automl_labor_scenarios`,
   TABLE input_table)
ORDER BY
 high_school_including_ged ASC
 ```
