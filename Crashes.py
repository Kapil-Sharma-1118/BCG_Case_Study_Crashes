# Import required packages
import pyspark
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, rank, sum, lower
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, TimestampType
import os

# Creating spark context and spark session
sp_conf = pyspark.SparkConf().setAppName('crashes').setMaster('local[*]')
sc = pyspark.SparkContext(conf = sp_conf)
spark = SparkSession(sc)

# create variables for csv file paths (file paths are relative)
primary_person_filepath = "./Data/Primary_Person_use.csv"
restrict_filepath = "./Data/Restrict_use.csv"
units_filepath = "./Data/Units_use.csv"
charges_filepath = "./Data/Charges_use.csv"
damages_filepath = "./Data/Damages_use.csv"
endorse_filepath = "./Data/Endorse_use.csv"
result_filePath = "./Alaytics_Results"

# Read csv files in dataframes
## 1. create schema for each dataframe
### PrimaryPerson
primary_person_schema = StructType([StructField('CRASH_ID', IntegerType(), True), \
                                    StructField('UNIT_NBR', IntegerType(), True), \
                                    StructField('PRSN_NBR', IntegerType(), True), \
                                    StructField('PRSN_TYPE_ID', StringType(), True), \
                                    StructField('PRSN_OCCPNT_POS_ID', StringType(), True), \
                                    StructField('PRSN_INJRY_SEV_ID', StringType(), True), \
                                    StructField('PRSN_AGE', IntegerType(), True), \
                                    StructField('PRSN_ETHNICITY_ID', StringType(), True), \
                                    StructField('PRSN_GNDR_ID', StringType(), True), \
                                    StructField('PRSN_EJCT_ID', StringType(), True), \
                                    StructField('PRSN_REST_ID', StringType(), True), \
                                    StructField('PRSN_AIRBAG_ID', StringType(), True), \
                                    StructField('PRSN_HELMET_ID', StringType(), True), \
                                    StructField('PRSN_SOL_FL', StringType(), True), \
                                    StructField('PRSN_ALC_SPEC_TYPE_ID', StringType(), True), \
                                    StructField('PRSN_ALC_RSLT_ID', StringType(), True), \
                                    StructField('PRSN_BAC_TEST_RSLT', DoubleType(), True), \
                                    StructField('PRSN_DRG_SPEC_TYPE_ID', StringType(), True), \
                                    StructField('PRSN_DRG_RSLT_ID', StringType(), True), \
                                    StructField('DRVR_DRG_CAT_1_ID', StringType(), True), \
                                    StructField('PRSN_DEATH_TIME', TimestampType(), True), \
                                    StructField('INCAP_INJRY_CNT', IntegerType(), True), \
                                    StructField('NONINCAP_INJRY_CNT', IntegerType(), True), \
                                    StructField('POSS_INJRY_CNT', IntegerType(), True), \
                                    StructField('NON_INJRY_CNT', IntegerType(), True), \
                                    StructField('UNKN_INJRY_CNT', IntegerType(), True), \
                                    StructField('TOT_INJRY_CNT', IntegerType(), True), \
                                    StructField('DEATH_CNT', IntegerType(), True), \
                                    StructField('DRVR_LIC_TYPE_ID', StringType(), True), \
                                    StructField('DRVR_LIC_STATE_ID', StringType(), True), \
                                    StructField('DRVR_LIC_CLS_ID', StringType(), True), \
                                    StructField('DRVR_ZIP', StringType(), True)])

### Restrict
restrict_schema = StructType([StructField('CRASH_ID', IntegerType(), True), \
                              StructField('UNIT_NBR', IntegerType(), True), \
                              StructField('DRVR_LIC_RESTRIC_ID', StringType(), True)])

### Unit
units_schema = StructType([StructField('CRASH_ID', IntegerType(), True), \
                           StructField('UNIT_NBR', IntegerType(), True), \
                           StructField('UNIT_DESC_ID', StringType(), True), \
                           StructField('VEH_PARKED_FL', StringType(), True), \
                           StructField('VEH_HNR_FL', StringType(), True), \
                           StructField('VEH_LIC_STATE_ID', StringType(), True), \
                           StructField('VIN', StringType(), True), \
                           StructField('VEH_MOD_YEAR', IntegerType(), True), \
                           StructField('VEH_COLOR_ID', StringType(), True), \
                           StructField('VEH_MAKE_ID', StringType(), True), \
                           StructField('VEH_MOD_ID', StringType(), True), \
                           StructField('VEH_BODY_STYL_ID', StringType(), True), \
                           StructField('EMER_RESPNDR_FL', StringType(), True), \
                           StructField('OWNR_ZIP', StringType(), True), \
                           StructField('FIN_RESP_PROOF_ID', StringType(), True), \
                           StructField('FIN_RESP_TYPE_ID', StringType(), True), \
                           StructField('VEH_DMAG_AREA_1_ID', StringType(), True), \
                           StructField('VEH_DMAG_SCL_1_ID', StringType(), True), \
                           StructField('FORCE_DIR_1_ID', StringType(), True), \
                           StructField('VEH_DMAG_AREA_2_ID', StringType(), True), \
                           StructField('VEH_DMAG_SCL_2_ID', StringType(), True), \
                           StructField('FORCE_DIR_2_ID', StringType(), True), \
                           StructField('VEH_INVENTORIED_FL', StringType(), True), \
                           StructField('VEH_TRANSP_NAME', StringType(), True), \
                           StructField('VEH_TRANSP_DEST', StringType(), True), \
                           StructField('CONTRIB_FACTR_1_ID', StringType(), True), \
                           StructField('CONTRIB_FACTR_2_ID', StringType(), True), \
                           StructField('CONTRIB_FACTR_P1_ID', StringType(), True), \
                           StructField('VEH_TRVL_DIR_ID', StringType(), True), \
                           StructField('FIRST_HARM_EVT_INV_ID', StringType(), True), \
                           StructField('INCAP_INJRY_CNT', IntegerType(), True), \
                           StructField('NONINCAP_INJRY_CNT', IntegerType(), True), \
                           StructField('POSS_INJRY_CNT', IntegerType(), True), \
                           StructField('NON_INJRY_CNT', IntegerType(), True), \
                           StructField('UNKN_INJRY_CNT', IntegerType(), True), \
                           StructField('TOT_INJRY_CNT', IntegerType(), True), \
                           StructField('DEATH_CNT', IntegerType(), True)])

### Charges
charges_schema = StructType([StructField('CRASH_ID', IntegerType(), True), \
                             StructField('UNIT_NBR', IntegerType(), True), \
                             StructField('PRSN_NBR', IntegerType(), True), \
                             StructField('CHARGE', StringType(), True), \
                             StructField('CITATION_NBR', StringType(), True)])

### Damages
damages_schema = StructType([StructField('CRASH_ID', IntegerType(), True), \
                             StructField('DAMAGED_PROPERTY', StringType(), True)])

### Endorsements
endorse_schema = StructType([StructField('CRASH_ID', IntegerType(), True), \
                             StructField('UNIT_NBR', IntegerType(), True), \
                             StructField('DRVR_LIC_ENDORS_ID', StringType(), True)])

## 2. create dataframes by passing schema and filepath
primary_person_df = spark.read.option("header", True) \
                              .option("schema", primary_person_schema) \
                              .csv(primary_person_filepath)

restrict_df = spark.read.option("header", True) \
                              .option("schema", restrict_schema) \
                              .csv(restrict_filepath)
                              
units_df = spark.read.option("header", True) \
                              .option("schema", units_schema) \
                              .csv(units_filepath)
                              
charges_df = spark.read.option("header", True) \
                              .option("schema", charges_schema) \
                              .csv(charges_filepath)
                              
damages_df = spark.read.option("header", True) \
                              .option("schema", damages_schema) \
                              .csv(damages_filepath)
                              
endorse_df = spark.read.option("header", True) \
                              .option("schema", endorse_schema) \
                              .csv(endorse_filepath)
                              

# Create an empty list to append results
result_rows = []

# Analytics
## Analysis 1: Find the number of crashes (accidents) in which number of persons killed are male?
analysis_description_a1 = "Number of crashes where number of persons killed are male"
male_death_crash_counts = primary_person_df.where((col("DEATH_CNT") > 0) & (col("PRSN_GNDR_ID") == "MALE")) \
                                           .select("CRASH_ID") \
                                           .distinct() \
                                           .count()
male_death_crash = (analysis_description_a1, male_death_crash_counts)
result_rows.append(male_death_crash)

## Analysis 2: How many two wheelers are booked for crashes?
analysis_description_a2 = "Number of two wheelers booked for crashes"
two_wheeleres_crash_counts = units_df.join(charges_df, on = ["CRASH_ID"], how = "inner") \
                                     .where(units_df["VEH_BODY_STYL_ID"] == "MOTORCYCLE") \
                                     .select("CRASH_ID") \
                                     .distinct() \
                                     .count()
two_wheeleres_crash = (analysis_description_a2, two_wheeleres_crash_counts)
result_rows.append(two_wheeleres_crash)

## Analysis 3: Which state has highest number of accidents in which females are involved?
analysis_description_a3 = "State having highest accidents where females are involved"
highest_female_acc_state = primary_person_df.where((col("PRSN_GNDR_ID") == "FEMALE")) \
                                            .groupBy(col("DRVR_LIC_STATE_ID")) \
                                            .agg(count(col("CRASH_ID")).alias("HIGH_CRASH_FEMALE")) \
                                            .orderBy(col("HIGH_CRASH_FEMALE").desc()) \
                                            .limit(1) \
                                            .collect()[0][0]
highest_female_acc_state_res = (analysis_description_a3, highest_female_acc_state)
result_rows.append(highest_female_acc_state_res)

## Analysis 4: Which are the Top 5th to 15th VEH_MAKE_IDs that contribute to a largest number of injuries including death
analysis_description_a4 = "Vehicle manufacturer's that contributes to largest no of injuries including death : Top 5th to top 15th"
veh_man_contr_highest_inj_5_to_15 = units_df.where(col("VEH_MAKE_ID") != "NA") \
                                            .groupBy(col("VEH_MAKE_ID")) \
                                            .agg(sum(col("TOT_INJRY_CNT")).alias("TOT_INJ_VEH_ID")) \
                                            .withColumn("RNK", rank().over(Window.orderBy(col("TOT_INJ_VEH_ID").desc()))) \
                                            .where(col("RNK").between(5, 15)) \
                                            .collect()
veh_man_contr_highest_inj_5_to_15 = [{"VEH_MAKE_ID" : row["VEH_MAKE_ID"], "RANK" : row["RNK"]} for row in veh_man_contr_highest_inj_5_to_15]
veh_man_contr_highest_inj_5_to_15_res = (analysis_description_a4, veh_man_contr_highest_inj_5_to_15)
result_rows.append(veh_man_contr_highest_inj_5_to_15_res)

## Analysis 5: For all the body styles involved in crashes, mention the top ethnic user group of each unique body style
analysis_description_a5 = "Top ethnic user group of each body style"
window_spec = Window.partitionBy("VEH_BODY_STYL_ID").orderBy(col("ETHNICITY_CRASH_CNT"))
top_ethnic_grp_body_style = primary_person_df.join(units_df, on = ["CRASH_ID"], how = "inner") \
                                             .where((~ primary_person_df["PRSN_ETHNICITY_ID"].isin(["UNKNOWN", "OTHER", "NA"])) \
                                             & (~ units_df["VEH_BODY_STYL_ID"].isin(["UNKNOWN", "NA", "NOT REPORTED"])) \
                                             & (~ units_df["VEH_BODY_STYL_ID"].like("%OTHER%"))) \
                                             .groupBy(units_df["VEH_BODY_STYL_ID"], primary_person_df["PRSN_ETHNICITY_ID"]) \
                                             .agg(count(primary_person_df["CRASH_ID"]).alias("ETHNICITY_CRASH_CNT")) \
                                             .withColumn("RNK", rank().over(window_spec)) \
                                             .where(col("RNK") == 1) \
                                             .select("VEH_BODY_STYL_ID", "PRSN_ETHNICITY_ID") \
                                             .collect()
top_ethnic_grp_body_style = [{"VEH_BODY_STYL_ID" : row["VEH_BODY_STYL_ID"], "PRSN_ETHNICITY_ID" : row["PRSN_ETHNICITY_ID"]} for row in top_ethnic_grp_body_style]
top_ethnic_grp_body_style_res = (analysis_description_a5, top_ethnic_grp_body_style)
result_rows.append(top_ethnic_grp_body_style_res)

## Analysis 6: Among the crashed cars, what are the Top 5 Zip Codes with highest number crashes with alcohols as the contributing factor to a crash (Use Driver Zip Code)
analysis_description_a6 = "Top 5 zip codes having highest no of car crashes with alcohol as the contributing factor to crash"
top_5_zipcodes_car_crash_contr_alc = primary_person_df.join(units_df, on = ["CRASH_ID"], how = "inner") \
                                                      .where((units_df["VEH_BODY_STYL_ID"].isin("PASSENGER CAR, 4-DOOR", "PASSENGER CAR, 2-DOOR", "POLICE CAR/TRUCK")) \
                                                      & ((lower(units_df["CONTRIB_FACTR_1_ID"]).contains("alcohol")) | (lower(units_df["CONTRIB_FACTR_2_ID"]).contains("alcohol"))) \
                                                      & (primary_person_df["DRVR_ZIP"].isNotNull())) \
                                                      .groupBy(primary_person_df["DRVR_ZIP"]) \
                                                      .agg(count(units_df["CRASH_ID"]).alias("CAR_CRASH_CNT")) \
                                                      .withColumn("RNK", rank().over(Window.orderBy(col("CAR_CRASH_CNT").desc()))) \
                                                      .where(col("RNK").between(1, 5)) \
                                                      .select("DRVR_ZIP", "RNK") \
                                                      .collect()
top_5_zipcodes_car_crash_contr_alc = [{"DRVR_ZIP" : row["DRVR_ZIP"], "RANK" : row["RNK"]} for row in top_5_zipcodes_car_crash_contr_alc]
top_5_zipcodes_car_crash_contr_alc_res = (analysis_description_a6, top_5_zipcodes_car_crash_contr_alc)
result_rows.append(top_5_zipcodes_car_crash_contr_alc_res)

## Analysis 7: Count of Distinct Crash IDs where No Damaged Property was observed and Damage Level (VEH_DMAG_SCL~) is above 4 and car avails Insurance
analysis_description_a7 = "Count of distinct crash Id's where no damage property was observed, damage level was above 4 and car avails Insurance"
crash_cnt_npd_dm_lvl_4_ins = units_df.join(damages_df, on = ["CRASH_ID"], how = "left") \
                                     .where((damages_df["DAMAGED_PROPERTY"].isNull()) & ((units_df["VEH_DMAG_SCL_1_ID"].isin("DAMAGED 5", "DAMAGED 6", "DAMAGED 7 HIGHEST")) | (units_df["VEH_DMAG_SCL_2_ID"].isin("DAMAGED 5", "DAMAGED 6", "DAMAGED 7 HIGHEST"))) & (units_df["FIN_RESP_TYPE_ID"].isin("PROOF OF LIABILITY INSURANCE", "LIABILITY INSURANCE POLICY"))) \
                                     .select("CRASH_ID") \
                                     .distinct() \
                                     .count()
crash_cnt_npd_dm_lvl_4_ins_res = (analysis_description_a7, crash_cnt_npd_dm_lvl_4_ins)
result_rows.append(crash_cnt_npd_dm_lvl_4_ins_res)

## Analysis 8: Determine the Top 5 Vehicle Makes where drivers are charged with speeding related offences, has licensed Drivers, used top 10 used vehicle colours and has car licensed with the Top 25 states with highest number of offences (to be deduced from the data)
analysis_description_a8 = "Top 5 vehicle manufacturer's where drivers are charged with speeding related offences, has licensed Drivers, used top 10 vehicle colours and has car licensed with the Top 25 states with highest number of offences"
top_10_used_veh_col_df = units_df.groupBy(col("VEH_COLOR_ID")) \
                                 .agg(count("*").alias("VEH_COL_CNT")) \
                                 .orderBy(col("VEH_COL_CNT").desc()) \
                                 .limit(10) \
                                 .collect()
top_10_used_veh_color = [row["VEH_COLOR_ID"] for row in top_10_used_veh_col_df]
top_25_stat_high_off_df = units_df.join(charges_df, on = ["CRASH_ID"], how = "inner") \
                                  .groupBy(units_df["VEH_LIC_STATE_ID"]) \
                                  .agg(count("*").alias("VEH_LIC_STATE_CNT")) \
                                  .orderBy(col("VEH_LIC_STATE_CNT").desc()) \
                                  .limit(25) \
                                  .collect()
top_25_state_with_high_off = [row["VEH_LIC_STATE_ID"] for row in top_25_stat_high_off_df]
top_5_veh_manf_speed_off_to_25_stat = units_df.join(charges_df, on = ["CRASH_ID"], how = "inner") \
                                              .join(primary_person_df, on = ["CRASH_ID"], how = "inner") \
                                              .where((lower(charges_df["CHARGE"]).contains("speed")) \
                                              & (primary_person_df["DRVR_LIC_TYPE_ID"].isin("DRIVER LICENSE", "COMMERCIAL DRIVER LIC.")) \
                                              & (units_df["VEH_COLOR_ID"].isin(top_10_used_veh_color)) \
                                              & (units_df["VEH_LIC_STATE_ID"].isin(top_25_state_with_high_off))) \
                                              .groupBy(units_df["VEH_MAKE_ID"]) \
                                              .agg(count("*").alias("OFFENCE_CNT")) \
                                              .orderBy(col("OFFENCE_CNT").desc()) \
                                              .limit(5) \
                                              .collect()
top_5_veh_manf_speed_off_to_25_stat = [{"VEH_MAKE_ID" : row["VEH_MAKE_ID"], "OFFENCE_CNT" : row["OFFENCE_CNT"]} for row in top_5_veh_manf_speed_off_to_25_stat]
top_5_veh_manf_speed_off_to_25_stat_res = (analysis_description_a8, top_5_veh_manf_speed_off_to_25_stat)
result_rows.append(top_5_veh_manf_speed_off_to_25_stat_res)


# Write result records in a dataframe and then write it to a csv file
result_schema = StructType([StructField('Analysis_Description', StringType(), True), \
                            StructField('Result', StringType(), True)])
results_df = spark.createDataFrame(result_rows, result_schema)

## Function to write result data into a single csv file
def writeDFtoCSV(inputDataFrame, fileName, filePath, writeMode):
    inputDataFrame.coalesce(1) \
                  .write.mode(writeMode) \
                  .format("csv") \
                  .option("header", True) \
                  .save(f"{filePath}/{fileName}.csv")

writeDFtoCSV(results_df, "Crashes_Analysis", result_filePath, "overwrite")

# Stop spark session
sc.stop()