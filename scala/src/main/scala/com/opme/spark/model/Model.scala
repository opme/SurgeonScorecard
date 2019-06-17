package com.opme.spark.model

import org.apache.spark.sql.types._

//
// define types for omop schema
//
class Model() {

    //
    // define omop schema here
    // Pre-defining the schema will have better performance then use the cvs importer
    // to try to infer the schema which causes an extra scan of the data.
    // The date fields are read in as string.  They can be converted to date
    // objects if necessary
    // OMOP V5.0
    //
    var model_schema = collection.mutable.Map[String, StructType]()
		
		
    model_schema += ("care_site" -> StructType( 
            StructField("CARE_SITE_ID", IntegerType, false) ::
            StructField("LOCATION_ID", IntegerType, true) ::
            StructField("ORGANIZATION_ID", IntegerType, true) ::
            StructField("PLACE_OF_SERVICE_CONCEPT_ID", IntegerType, true) ::
            StructField("CARE_SITE_SOURCE_VALUE", StringType, true) ::
            StructField("PLACE_OF_SERVICE_SOURCE_VALUE", StringType, true) :: Nil))

    model_schema += ("cohort" -> StructType(
            StructField("COHORT_ID", IntegerType, false) ::
            StructField("COHORT_CONCEPT_ID", IntegerType, false) ::
            StructField("COHORT_START_DATE", StringType, false) ::
            StructField("COHORT_END_DATE", StringType, true) ::
            StructField("SUBJECT_ID", IntegerType, false) ::
            StructField("STOP_REASON", StringType, true) :: Nil))

    model_schema += ("condition_era" -> StructType(
            StructField("CONDITION_ERA_ID", IntegerType, false) ::
            StructField("PERSON_ID", IntegerType, false) ::
            StructField("CONDITION_CONCEPT_ID", IntegerType, false) ::
            StructField("CONDITION_ERA_START_DATE", StringType, false) ::
            StructField("CONDITION_ERA_END_DATE", StringType, true) ::
            StructField("CONDITION_OCCURRENCE_COUNT", IntegerType, true) :: Nil))

    model_schema += ("condition_occurrence" -> StructType(
            StructField("CONDITION_OCCURRENCE_ID", IntegerType, false) ::
            StructField("PERSON_ID", IntegerType, false) ::
            StructField("CONDITION_CONCEPT_ID", IntegerType, false) ::
            StructField("CONDITION_START_DATE", StringType, false) ::
            StructField("CONDITION_END_DATE", StringType, true) ::
            StructField("CONDITION_TYPE_CONCEPT_ID", IntegerType, false) ::
            StructField("STOP_REASON", StringType, true) ::
            StructField("PROVIDER_ID", IntegerType, true) ::
            StructField("VISIT_OCCURRENCE_ID", IntegerType, true) ::
            StructField("CONDITION_SOURCE_VALUE", StringType, true) ::
            StructField("CONDITION_SOURCE_CONCEPT_ID", IntegerType, true) :: Nil))

    model_schema += ("death" -> StructType(
            StructField("PERSON_ID", IntegerType, false) ::
            StructField("DEATH_DATE", StringType, false) ::
            StructField("DEATH_TYPE_CONCEPT_ID", IntegerType, false) ::
            StructField("CAUSE_CONCEPT_ID", IntegerType, true) ::
            StructField("CAUSE_SOURCE_VALUE", StringType, true) ::
            StructField("CAUSE_SOURCE_CONCEPT_ID", IntegerType, true) :: Nil))

     model_schema += ("device_exposure" -> StructType(
            StructField("DEVICE_EXPOSURE_ID", IntegerType, false) ::
            StructField("PERSON_ID", IntegerType, false) ::
            StructField("DEVICE_CONCEPT_ID", IntegerType, false) ::
            StructField("DEVICE_EXPOSURE_START_DATE", StringType, false) ::
            StructField("DEVICE_EXPOSURE_END_DATE", StringType, true) ::
            StructField("DEVICE_TYPE_CONCEPT_ID", IntegerType, true) ::
            StructField("UNIQUE_DEVICE_ID", IntegerType, true) ::
            StructField("QUANTITY", IntegerType, true) ::
            StructField("PROVIDER_ID", IntegerType, true) ::
            StructField("VISIT_OCCURRENCE_ID", IntegerType, true) ::
            StructField("DEVICE_SOURCE_VALUE", StringType, true) ::
            StructField("DEVICE_SOURCE_CONCEPT_ID", IntegerType, true) :: Nil))

    model_schema += ("drug_cost" -> StructType(
            StructField("DRUG_COST_ID", IntegerType, false) ::
            StructField("DRUG_EXPOSURE_ID", IntegerType, false) ::
            StructField("PAID_COPAY", FloatType, true) ::
            StructField("PAID_COINSURANCE", FloatType, true) ::
            StructField("PAID_TOWARD_DEDUCTIBLE", FloatType, true) ::
            StructField("PAID_BY_PAYER", FloatType, true) ::
            StructField("PAID_BY_COORDINATION_BENEFITS", FloatType, true) ::
            StructField("TOTAL_OUT_OF_POCKET", FloatType, true) ::
            StructField("TOTAL_PAID", FloatType, true) ::
            StructField("INGREDIENT_COST", FloatType, true) ::
            StructField("DISPENSING_FEE", FloatType, true) ::
            StructField("AVERAGE_WHOLESALE_PRICE", FloatType, true) ::
            StructField("PAYER_PLAN_PERIOD_ID", IntegerType, true) :: Nil))

    model_schema += ("drug_era" -> StructType(
            StructField("DRUG_ERA_ID", IntegerType, false) ::
            StructField("PERSON_ID", IntegerType, false) ::
            StructField("DRUG_CONCEPT_ID", IntegerType, false) ::
            StructField("DRUG_ERA_START_DATE", StringType, false) ::
            StructField("DRUG_ERA_END_DATE", StringType, true) ::
            StructField("DRUG_TYPE_CONCEPT_ID", IntegerType, false) ::
            StructField("DRUG_EXPOSURE_COUNT", IntegerType, true) :: Nil))

    model_schema += ("drug_exposure" -> StructType(
            StructField("DRUG_EXPOSURE_ID", IntegerType, false) ::
            StructField("PERSON_ID", IntegerType, false) ::
            StructField("DRUG_CONCEPT_ID", IntegerType, false) ::
            StructField("DRUG_EXPOSURE_START_DATE", StringType, false) ::
            StructField("DRUG_EXPOSURE_END_DATE", StringType, true) ::
            StructField("DRUG_TYPE_CONCEPT_ID", IntegerType, false) ::
            StructField("STOP_REASON", StringType, true) ::
            StructField("REFILLS", IntegerType, true) ::
            StructField("QUANTITY", FloatType, true) ::
            StructField("DAYS_SUPPLY", IntegerType, true) ::
            StructField("SIG", StringType, true) ::
            StructField("ROUTE_CONCEPT_ID", IntegerType, true) ::
            StructField("EFFECTIVE_DRUG_DOSE", FloatType, true) ::
            StructField("DOSE_UNIT_CONCEPT_ID", IntegerType, true) ::
            StructField("LOT_NUMBER", StringType, true) ::
            StructField("PROVIDER_ID", IntegerType, true) ::
            StructField("VISIT_OCCURRENCE_ID", IntegerType, true) ::
            StructField("DRUG_SOURCE_VALUE", StringType, true) ::
            StructField("DRUG_SOURCE_CONCEPT_ID", IntegerType, true) ::
            StructField("ROUTE_SOURCE_VALUE", StringType, true) ::
            StructField("DOSE_UNIT_SOURCE_VALUE", StringType, true) :: Nil))

    model_schema += ("location" -> StructType(
            StructField("LOCATION_ID", IntegerType, false) ::
            StructField("ADDRESS_1", StringType, true) ::
            StructField("ADDRESS_2", StringType, true) ::
            StructField("CITY", StringType, true) ::
            StructField("STATE", StringType, true) ::
            StructField("ZIP", StringType, true) ::
            StructField("COUNTY", StringType, true) ::
            StructField("LOCATION_SOURCE_VALUE", StringType, true) :: Nil))

    model_schema += ("measurement" -> StructType(
            StructField("MEASUREMENT_ID", IntegerType, false) ::
            StructField("PERSON_ID", IntegerType, false) ::
            StructField("MEASUREMENT_CONCEPT_ID", IntegerType, false) ::
            StructField("MEASUREMENT_DATE", StringType, false) ::
            StructField("MEASUREMENT_TIME", StringType, true) ::
            StructField("MEASUREMENT_TYPE_CONCEPT_ID", IntegerType, false) ::
            StructField("OPERATOR_CONCEPT_ID", IntegerType, true) ::
            StructField("VALUE_AS_NUMBER", FloatType, true) ::
            StructField("VALUE_AS_CONCEPT_ID", IntegerType, true) ::
            StructField("UNIT_CONCEPT_ID", IntegerType, true) ::
            StructField("RANGE_LOW", FloatType, true) ::
            StructField("RANGE_HIGH", FloatType, true) ::
            StructField("PROVIDER_ID", IntegerType, true) ::
            StructField("VISIT_OCCURRENCE_ID", IntegerType, true) ::
            StructField("MEASUREMENT_SOURCE_VALUE", StringType, true) ::
            StructField("MEASUREMENT_SOURCE_CONCEPT_ID", IntegerType, true) ::
            StructField("UNIT_SOURCE_VALUE", StringType, true) ::
            StructField("VALUE_SOURCE_VALUE", StringType, true) :: Nil))

    model_schema += ("observation" -> StructType(
            StructField("OBSERVATION_ID", IntegerType, false) ::
            StructField("PERSON_ID", IntegerType, false) ::
            StructField("OBSERVATION_CONCEPT_ID", IntegerType, false) ::
            StructField("OBSERVATION_DATE", StringType, false) ::
            StructField("OBSERVATION_TIME", StringType, true) ::
            StructField("OBSERVATION_TYPE_CONCEPT_ID", IntegerType, false) ::
            StructField("VALUE_AS_NUMBER", FloatType, true) ::
            StructField("VALUE_AS_STRING", StringType, true) ::
            StructField("VALUE_AS_CONCEPT_ID", IntegerType, true) ::
            StructField("QUALIFIER_CONCEPT_ID", IntegerType, true) ::
            StructField("UNIT_CONCEPT_ID", IntegerType, true) ::
            StructField("PROVIDER_ID", IntegerType, true) ::
            StructField("VISIT_OCCURRENCE_ID", IntegerType, true) ::
            StructField("OBSERVATION_SOURCE_VALUE", StringType, true) ::
            StructField("OBSERVATION_SOURCE_CONCEPT_ID", IntegerType, true) ::
            StructField("UNIT_SOURCE_VALUE", StringType, true) ::
            StructField("QUALIFIER_SOURCE_VALUE", StringType, true) :: Nil))

    model_schema += ("observation_period" -> StructType(
            StructField("OBSERVATION_PERIOD_ID", IntegerType, false) ::
            StructField("PERSON_ID", IntegerType, false) ::
            StructField("OBSERVATION_PERIOD_START_DATE", StringType, false) ::
            StructField("OBSERVATION_PERIOD_END_DATE", StringType, false) :: Nil))

    model_schema += ("organization" -> StructType(
            StructField("ORGANIZATION_ID", IntegerType, false) ::
            StructField("PLACE_OF_SERVICE_CONCEPT_ID", IntegerType, true) ::
            StructField("LOCATION_ID", IntegerType, true) ::
            StructField("ORGANIZATION_SOURCE_VALUE", StringType, true) ::
            StructField("PLACE_OF_SERVICE_SOURCE_VALUE", StringType, true):: Nil))

    model_schema += ("payer_plan_period" -> StructType(
            StructField("PAYER_PLAN_PERIOD_ID", IntegerType, false) ::
            StructField("PERSON_ID", IntegerType, false) ::
            StructField("PAYER_PLAN_PERIOD_START_DATE", StringType, false) ::
            StructField("PAYER_PLAN_PERIOD_END_DATE", StringType, false) ::
            StructField("PAYER_SOURCE_VALUE", StringType, true) ::
            StructField("PLAN_SOURCE_VALUE", StringType, true) ::
            StructField("FAMILY_SOURCE_VALUE", StringType, true) :: Nil))

    model_schema += ("person" -> StructType(
            StructField("PERSON_ID", IntegerType, false) ::
            StructField("GENDER_CONCEPT_ID", IntegerType, false) ::
            StructField("YEAR_OF_BIRTH", IntegerType, false) ::
            StructField("MONTH_OF_BIRTH", IntegerType, true) ::
            StructField("DAY_OF_BIRTH", IntegerType, true) ::
            StructField("TIME_OF_BIRTH", StringType, true) ::
            StructField("RACE_CONCEPT_ID", IntegerType, true) ::
            StructField("ETHNICITY_CONCEPT_ID", IntegerType, true) ::
            StructField("LOCATION_ID", IntegerType, true) ::
            StructField("PROVIDER_ID", IntegerType, true) ::
            StructField("CARE_SITE_ID", IntegerType, true) ::
            StructField("PERSON_SOURCE_VALUE", StringType, true) ::
            StructField("GENDER_SOURCE_VALUE", StringType, true) ::
            StructField("GENDER_SOURCE_CONCEPT_ID", IntegerType, true) ::
            StructField("RACE_SOURCE_VALUE", StringType, true) ::
            StructField("RACE_SOURCE_CONCEPT_ID", IntegerType, true) ::
            StructField("ETHNICITY_SOURCE_VALUE", StringType, true) ::
            StructField("ETHNICITY_SOURCE_CONCEPT_ID", IntegerType, true) :: Nil))

    model_schema += ("procedure_cost" -> StructType(
            StructField("PROCEDURE_COST_ID", IntegerType, false) ::
            StructField("PROCEDURE_OCCURRENCE", IntegerType, false) ::
            StructField("PAID_COPAY", FloatType, true) ::
            StructField("PAID_COINSURANCE", FloatType, true) ::
            StructField("PAID_TOWARD_DEDUCTIBLE", FloatType, true) ::
            StructField("PAID_BY_PAYER", FloatType, true) ::
            StructField("PAID_BY_COORDINATION_BENEFITS", FloatType, true) ::
            StructField("TOTAL_OUT_OF_POCKET", FloatType, true) ::
            StructField("TOTAL_PAID", FloatType, true) ::
            StructField("DISEASE_CLASS_CONCEPT_ID", IntegerType, true) ::
            StructField("REVENUE_CODE_CONCEPT_ID", IntegerType, true) ::
            StructField("PAYER_PLAN_PERIOD_ID", IntegerType, true) ::
            StructField("DISEASE_CLASS_SOURCE_VALUE", StringType, true) ::
            StructField("REVENUE_CODE_SOURCE_VALUE", StringType, true) :: Nil))

    model_schema += ("procedure_occurrence" -> StructType(
            StructField("PROCEDURE_OCCURRENCE_ID", IntegerType, false) ::
            StructField("PERSON_ID", IntegerType, false) ::
            StructField("PROCEDURE_CONCEPT_ID", IntegerType, false) ::
            StructField("PROCEDURE_DATE", StringType, false) ::
            StructField("PROCEDURE_TYPE_CONCEPT_ID", IntegerType, false) ::
            StructField("MODIFIER_CONCEPT_ID", IntegerType, true) ::
            StructField("QUANTITY", IntegerType, false) ::
            StructField("PROVIDER_ID", IntegerType, true) ::
            StructField("VISIT_OCCURRENCE_ID", IntegerType, true) ::
            StructField("PROCEDURE_SOURCE_VALUE", StringType, true) ::
            StructField("PROCEDURE_SOURCE_CONCEPT_ID", IntegerType, true) ::
            StructField("QUALIFIER_SOURCE_VALUE", StringType, true) :: Nil))

    model_schema += ("provider" -> StructType(
            StructField("PROVIDER_ID", IntegerType, false) ::
            StructField("PROVIDER_NAME", StringType, true) ::
            StructField("NPI", StringType, true) ::
            StructField("DEA", StringType, true) ::
            StructField("SPECIALTY_CONCEPT_ID", IntegerType, true) ::
            StructField("CARE_SITE_ID", IntegerType, true) ::
            StructField("YEAR_OF_BIRTH", IntegerType, true) ::
            StructField("GENDER_CONCEPT_ID", IntegerType, true) ::
            StructField("PROVIDER_SOURCE_VALUE", StringType, false) ::
            StructField("SPECIALTY_SOURCE_VALUE", StringType, true) ::
            StructField("SPECIALTY_SOURCE_CONCEPT_ID", IntegerType, true) ::
            StructField("GENDER_SOURCE_VALUE", StringType, false) ::
            StructField("GENDER_SOURCE_CONCEPT_ID", IntegerType, true) :: Nil))

    model_schema += ("visit_occurrence" -> StructType(
            StructField("VISIT_OCCURRENCE_ID", IntegerType, false) ::
            StructField("PERSON_ID", IntegerType, false) ::
            StructField("VISIT_CONCEPT_ID", IntegerType, false) ::
            StructField("VISIT_START_DATE", StringType, false) ::
            StructField("VISIT_START_TIME", StringType, true) ::
            StructField("VISIT_END_DATE", StringType, false) ::
            StructField("VISIT_END_TIME", StringType, true) ::
            StructField("VISIT_TYPE_CONCEPT_ID", IntegerType, false) ::
            StructField("PROVIDER_ID", IntegerType, true) ::
            StructField("CARE_SITE_ID", IntegerType, true) ::
            StructField("VISIT_SOURCE_VALUE", StringType, true) ::
            StructField("VISIT_SOURCE_CONCEPT_ID", IntegerType, true) :: Nil))
}

