from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import *

import logging
import datetime
import argparse
from utils import utils

logger = logging.getLogger('logger')
logger.setLevel(logging.INFO)

log_file = f"logs/{datetime.datetime.now().strftime('%Y-%m-%d_%H%M%S')}.log"
file_handler = logging.FileHandler(log_file)
log_format = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
file_handler.setFormatter(log_format)
logger.addHandler(file_handler)

class case_study_analysis:

    def __init__(self, spark, path_to_config_file):
        """
        Initialize the input dataframes and output folder and format
        param spark: SparkSession object
        param path_to_config_file: config file path
        return: None
        """
        try:
            logger.info(f'path to config file: {path_to_config_file}')

            dataset = utils.read_yaml(path_to_config_file).get("DATASET")
            self.df_charge = utils.load_data_from_csv(spark, dataset.get("Charges"))
            self.df_damage = utils.load_data_from_csv(spark, dataset.get("Damages"))
            self.df_endorse = utils.load_data_from_csv(spark, dataset.get("Endorse"))
            self.df_person = utils.load_data_from_csv(spark, dataset.get("Primary_Person"))
            self.df_units = utils.load_data_from_csv(spark, dataset.get("Units"))
            self.df_restrict = utils.load_data_from_csv(spark, dataset.get("Restrict"))
            logger.info('dataframes initialized from the configured dataset folder')

            output_info = utils.read_yaml(path_to_config_file).get("OUTPUT")
            self.output_folder = output_info.get('FOLDER')
            self.output_format = output_info.get('FORMAT')
            logger.info('output folder and format have been configured')

            # Assuming these set of columns can uniquely identify a vehicle
            self.veh_attribute_cols = ['veh_lic_state_id','vin','veh_mod_year','veh_make_id']
            logger.info('vehicle attribute columns have been set')

        except Exception as e:
            logger.error(f'Error encountered during init.')
            logger.error(e)
                  
    def analysis_q1(self):
        """
        Find the number of crashes (accidents) in which number of males killed are greater than 2?
        return: None
        """
        try:
            df_person_male_killed_more_than_2 = self.df_person\
                .dropDuplicates(['crash_id', 'unit_nbr'])\
                .where((upper(col('prsn_gndr_id'))=='MALE')
                & (upper(col('prsn_injry_sev_id'))=='KILLED'))\
                .groupBy('crash_id').agg(count('*').alias('count_of_males_killed'))\
                .filter(col('count_of_males_killed')>2)
            
            print(f'output for analysis q1:')
            print(df_person_male_killed_more_than_2.count())

            output_file_path = self.output_folder+'analysis_q1'
            utils.write_output(df_person_male_killed_more_than_2, output_file_path, self.output_format)
            logger.info(f'output for Q1 analysis stored at - {output_file_path}')
        except Exception as e:
            logger.error("Error occured during analysis for Q1")
            logger.error(e)

    def analysis_q2(self):
        """
        How many two wheelers are booked for crashes? 
        return: None
        """
        try:
            df_units_2_wheeler = self.df_units\
                .dropDuplicates(['crash_id', 'unit_nbr'])\
                .where(upper(col('veh_body_styl_id')).contains('MOTORCYCLE'))\
                .dropDuplicates(self.veh_attribute_cols)

            print(f'output for analysis q2:')
            print(df_units_2_wheeler.count())
            
            output_file_path = self.output_folder+'analysis_q2'
            utils.write_output(df_units_2_wheeler, output_file_path, self.output_format)
            logger.info(f'output for Q2 analysis stored at - {output_file_path}')

        except Exception as e:
            logger.error("Error occured during analysis for Q2")
            logger.error(e)

    def analysis_q3(self):
        """
        Determine the Top 5 Vehicle Makes of the cars present in the crashes 
        in which driver died and Airbags did not deploy.
        return: None
        """
        try:
            df_units_filtered = self.df_units\
                .dropDuplicates(['crash_id', 'unit_nbr'])\
                .where(upper(col('veh_body_styl_id')).contains('CAR'))

            df_person_filtered = self.df_person\
                .dropDuplicates(['crash_id', 'unit_nbr'])\
                .where((upper(col('prsn_type_id'))=='DRIVER')
                & (upper(col('prsn_airbag_id'))=='NOT DEPLOYED')
                & (upper(col('prsn_injry_sev_id'))=='KILLED'))

            df_person_unit_merged = df_units_filtered\
                .join(df_person_filtered, ['crash_id', 'unit_nbr'], 'inner')\
                .dropDuplicates(self.veh_attribute_cols)\
                .groupBy('veh_make_id').count().orderBy(col('count').desc()).limit(5)

            print(f'output for analysis q3:')
            df_person_unit_merged.show()

            output_file_path = self.output_folder+'analysis_q3'
            utils.write_output(df_person_unit_merged, output_file_path, self.output_format)
            logger.info(f'output for Q3 analysis stored at - {output_file_path}')
            
        except Exception as e:
            logger.error("Error occured during analysis for Q3")
            logger.error(e)

    def analysis_q4(self):
        """
        Determine number of Vehicles with driver having valid licences involved in hit and run? 
        return: None
        """
        try:
            df_units_filtered = self.df_units\
                .dropDuplicates(['crash_id', 'unit_nbr'])\
                .where(upper(col('veh_hnr_fl'))=='Y')\
                .drop('death_cnt','incap_injry_cnt',
                    'non_injry_cnt','nonincap_injry_cnt',
                    'poss_injry_cnt','tot_injry_cnt','unkn_injry_cnt')
                # dropping the common columns in df_person & df_units

            df_person_filtered = self.df_person\
                .dropDuplicates(['crash_id', 'unit_nbr'])\
                .where(upper(col('prsn_type_id')).contains('DRIVER')
                & (upper(col('drvr_lic_cls_id')).contains('CLASS')
                | upper(col('drvr_lic_cls_id')).contains('OTHER')))

            df_person_unit_merged = df_units_filtered\
                .join(df_person_filtered, ['crash_id', 'unit_nbr'], 'inner')\
                .dropDuplicates(self.veh_attribute_cols)

            print(f'output for analysis q4:')
            print(df_person_unit_merged.count())

            output_file_path = self.output_folder+'analysis_q4'
            utils.write_output(df_person_unit_merged, output_file_path, self.output_format)
            logger.info(f'output for Q4 analysis stored at - {output_file_path}')
            
        except Exception as e:
            logger.error("Error occured during analysis for Q4")
            logger.error(e)

    def analysis_q5(self):
        """
        Which state has highest number of accidents in which females are not involved? 
        return: None
        """
        try:
            df_person_filtered = self.df_person\
                .dropDuplicates(['crash_id', 'unit_nbr'])

            df_crash_with_no_female = df_person_filtered\
                .withColumn('count_of_females',when(upper(col('PRSN_GNDR_ID'))=='FEMALE',1).otherwise(0))\
                .groupBy(col('crash_id'))\
                .agg(sum(col('count_of_females')).alias('count_of_females_per_crash_id'))\
                .filter(col('count_of_females_per_crash_id')==0)\

            df_state_most_accidents = df_crash_with_no_female\
                .join(df_person_filtered,'crash_id', 'inner')\
                .groupBy('drvr_lic_state_id')\
                .agg(countDistinct('crash_id').alias('count_of_accidents'))\
                .orderBy(col("count_of_accidents").desc()).limit(1)

            print(f'output for analysis q5:')
            df_state_most_accidents.show()

            output_file_path = self.output_folder+'analysis_q5'
            utils.write_output(df_state_most_accidents, output_file_path, self.output_format)
            logger.info(f'output for Q5 analysis stored at - {output_file_path}')
            
        except Exception as e:
            logger.error("Error occured during analysis for Q5")
            logger.error(e)

    def analysis_q6(self):
        """
        Which are the Top 3rd to 5th VEH_MAKE_IDs that contribute 
        to a largest number of injuries including death 
        return: None
        """
        try:
            df_death_plus_injury_counts = self.df_units\
                .dropDuplicates(['crash_id','unit_nbr'])\
                .withColumn("death_plus_injury_cnt", col("tot_injry_cnt") + col("death_cnt"))\
                .groupBy("veh_make_id")\
                .agg(sum(col('death_plus_injury_cnt')).alias('death_plus_injry_cnt_per_veh_make_id'))

            window_spec = Window.orderBy(col('death_plus_injry_cnt_per_veh_make_id').desc())

            df_top_3_to_5_make_id = df_death_plus_injury_counts\
                .withColumn('rn', row_number().over(window_spec))\
                .orderBy(col('death_plus_injry_cnt_per_veh_make_id').desc())\
                .filter(col("rn").isin(3,4,5))\
                .drop('rn')

            print(f'output for analysis q6:')
            df_top_3_to_5_make_id.show()

            output_file_path = self.output_folder+'analysis_q6'
            utils.write_output(df_top_3_to_5_make_id, output_file_path, self.output_format)
            logger.info(f'output for Q6 analysis stored at - {output_file_path}')
            
        except Exception as e:
            logger.error("Error occured during analysis for Q6")
            logger.error(e)

    def analysis_q7(self):
        """
        For all the body styles involved in crashes, 
        mention the top ethnic user group of each unique body style  
        return: None
        """
        try:
            df_units_filtered = self.df_units\
                .dropDuplicates(['crash_id','unit_nbr'])\
                .where(~upper(col('veh_body_styl_id')).isin('NA', 'UNKNOWN', 'NOT REPORTED'))

            df_person_filtered = self.df_person\
                .dropDuplicates(['crash_id','unit_nbr'])\
                .where(~upper(col('prsn_ethnicity_id')).isin('NA', 'UNKNOWN'))

            df_person_unit_merged = df_units_filtered\
                .join(df_person_filtered, ['crash_id', 'unit_nbr'], 'inner')\
                .groupBy(['veh_body_styl_id', 'prsn_ethnicity_id'])\
                .agg(count('*').alias('count'))

            window_spec = Window.partitionBy('veh_body_styl_id').orderBy(col('count').desc())

            df_top_ethnic_user_group = df_person_unit_merged\
                .withColumn('rn', row_number().over(window_spec))\
                .filter(col('rn')==1).drop('rn')\
                .orderBy(col('count').desc())

            print(f'output for analysis q7:')
            df_top_ethnic_user_group.show()

            output_file_path = self.output_folder+'analysis_q7'
            utils.write_output(df_top_ethnic_user_group, output_file_path, self.output_format)
            logger.info(f'output for Q7 analysis stored at - {output_file_path}')
            
        except Exception as e:
            logger.error("Error occured during analysis for Q7")
            logger.error(e)

    def analysis_q8(self):
        """
        Among the crashed cars, what are the Top 5 Zip Codes with highest number crashes
        with alcohols as the contributing factor to a crash (Use Driver Zip Code)
        return: None
        """
        try:
            df_person_filtered = self.df_person\
                .dropDuplicates(['crash_id','unit_nbr'])\
                .where(col('DRVR_ZIP').isNotNull() & ~upper(col('drvr_zip')).isin('UNKNOWN'))

            df_units_filtered = self.df_units\
                .dropDuplicates(['crash_id','unit_nbr'])

            df_person_unit_merged = df_units_filtered\
                .join(df_person_filtered, ['crash_id', 'unit_nbr'], 'inner')\
                .where(upper(col('contrib_factr_1_id')).contains('ALCOHOL')
                | upper(col('contrib_factr_2_id')).contains('ALCOHOL')
                | upper(col('contrib_factr_p1_id')).contains('ALCOHOL')
                | upper(col('contrib_factr_1_id')).contains('DRINKING')
                | upper(col('contrib_factr_2_id')).contains('DRINKING')
                | upper(col('contrib_factr_p1_id')).contains('DRINKING')
                | upper(col("prsn_alc_rslt_id")).contains('POSITIVE'))\
                .groupBy('drvr_zip')\
                .agg(countDistinct('crash_id').alias('crash_count'))\
                .orderBy(col('crash_count').desc()).limit(5)

            print(f'output for analysis q8:')
            df_person_unit_merged.show()

            output_file_path = self.output_folder+'analysis_q8'
            utils.write_output(df_person_unit_merged, output_file_path, self.output_format)
            logger.info(f'output for Q8 analysis stored at - {output_file_path}')
            
        except Exception as e:
            logger.error("Error occured during analysis for Q8")
            logger.error(e)

    def analysis_q9(self):
        """
        Count of Distinct Crash IDs where No Damaged Property was observed 
        and Damage Level (VEH_DMAG_SCL~) is above 4 and car avails Insurance
        return: None
        """
        try:
            df_units_filtered = self.df_units\
                .dropDuplicates(['crash_id','unit_nbr'])

            df_damage_filtered = self.df_damage\
                .dropDuplicates(['crash_id'])

            df_units_damage_merged = df_units_filtered\
                .join(df_damage_filtered,'crash_id','inner')\
                .where(upper(col('damaged_property')).contains('NONE')
                | upper(col('damaged_property')).contains('NO DAMAGE'))\
                .filter(((upper(col('veh_dmag_scl_1_id')) > "DAMAGED 4") & (~upper(col('veh_dmag_scl_1_id')).isin("NA", "NO DAMAGE", "INVALID VALUE")))
                | ((upper(col('veh_dmag_scl_2_id')) > "DAMAGED 4") & (~upper(col('veh_dmag_scl_2_id')).isin("NA", "NO DAMAGE", "INVALID VALUE"))))\
                .filter(upper(col('fin_resp_type_id')).contains('INSURANCE'))\
                .dropDuplicates(['crash_id'])\
                .orderBy(col("crash_id").desc())

            print(f'output for analysis q9:')
            print(df_units_damage_merged.count())

            output_file_path = self.output_folder+'analysis_q9'
            utils.write_output(df_units_damage_merged, output_file_path, self.output_format)
            logger.info(f'output for Q9 analysis stored at - {output_file_path}')
            
        except Exception as e:
            logger.error("Error occured during analysis for Q9")
            logger.error(e)

    def analysis_q10(self):
        """
        Determine the Top 5 Vehicle Makes where drivers 
        are charged with speeding related offences, has licensed Drivers, 
        used top 10 used vehicle colours and has car licensed with 
        the Top 25 states with highest number of offences (to be deduced from the data)
        return: None
        """
        try:

            df_units_filtered = self.df_units\
                .dropDuplicates(['crash_id', 'unit_nbr'])

            df_charge_filtered = self.df_charge\
                .dropDuplicates(['crash_id', 'unit_nbr', 'prsn_nbr'])

            df_person_filtered = self.df_person\
                .dropDuplicates(['crash_id', 'unit_nbr'])

            df_top_10_vehicle_colors = df_units_filtered\
                .where(~col('veh_color_id').isin('NA')
                & col('veh_color_id').cast("int").isNull())\
                .groupBy('veh_color_id').agg(count('*').alias('color_count')).orderBy(col('color_count').desc()).limit(10)

            print(f'output for analysis q10:')
            top_10_vehicle_colors = [row["veh_color_id"] for row in df_top_10_vehicle_colors.collect()]
            print(f'top_10_vehicle_colors: {top_10_vehicle_colors}')

            df_top_25_states = df_units_filtered\
                .where(~col('veh_lic_state_id').isin('NA')\
                & col('veh_lic_state_id').cast("int").isNull())\
                .groupBy('veh_lic_state_id').agg(count('*').alias('state_count')).orderBy(col('state_count').desc()).limit(25)

            top_25_states = [row["veh_lic_state_id"] for row in df_top_25_states.collect()]
            print(f'top_25_states: {top_25_states}')

            df_charge_person_units_merged = df_charge_filtered\
                .join(df_person_filtered, ['crash_id', 'unit_nbr', 'prsn_nbr'], 'inner')\
                .join(df_units_filtered, ['crash_id', 'unit_nbr'], 'inner')\
                .where(col('veh_color_id').isin(top_10_vehicle_colors)
                & col('veh_lic_state_id').isin(top_25_states)
                & upper(col('prsn_type_id')).contains('DRIVER')
                & (upper(col('drvr_lic_cls_id')).contains('CLASS') | upper(col('drvr_lic_cls_id')).contains('OTHER'))
                & upper(col('charge')).contains('SPEED'))\
                .groupBy('veh_make_id').agg(count('*').alias('offence_count'))\
                .orderBy(col('offence_count').desc()).limit(5)

            df_charge_person_units_merged.show()

            output_file_path = self.output_folder+'analysis_q10'
            utils.write_output(df_charge_person_units_merged, output_file_path, self.output_format)
            logger.info(f'output for Q10 analysis stored at - {output_file_path}')
            
        except Exception as e:
            logger.error("Error occured during analysis for Q10")
            logger.error(e)

if __name__ == '__main__':

    try:    
        
        parser = argparse.ArgumentParser()
        parser.add_argument("--config", required=True, type=str, default="src/config/config.yaml")
        args = parser.parse_args()
        config_file_path = args.config

        spark = SparkSession\
                    .builder \
                    .appName("case_study_solution") \
                    .getOrCreate()
        spark.sparkContext.setLogLevel("ERROR")

        obj = case_study_analysis(spark,config_file_path) 
        
        print('analysis started')

        obj.analysis_q1()
        logger.info('analysis for Q1 finished')

        obj.analysis_q2()
        logger.info('analysis for Q2 finished')

        obj.analysis_q3()
        logger.info('analysis for Q3 finished')

        obj.analysis_q4()
        logger.info('analysis for Q4 finished')

        obj.analysis_q5()
        logger.info('analysis for Q5 finished')

        obj.analysis_q6()
        logger.info('analysis for Q6 finished')

        obj.analysis_q7()
        logger.info('analysis for Q7 finished')

        obj.analysis_q8()
        logger.info('analysis for Q8 finished')

        obj.analysis_q9()
        logger.info('analysis for Q9 finished')

        obj.analysis_q10()
        logger.info('analysis for Q10 finished')

        print('analysis completed')

    except Exception as e:
        logger.error('Error while performing analysis.')
        logger.error(e)

    finally:
        spark.stop()
        logger.info("Successfully completed the execution")
