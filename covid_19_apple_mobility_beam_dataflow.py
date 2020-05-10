import os, datetime, logging
import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText

class SeparateFn(beam.DoFn):
  def process(self, element):
    e = element # element is a dict
    
    # obtain the components of each element instance se
    geo_type = e.get('geo_type') # get method is case sensitive
    region = e.get('region')
    transportation_type = e.get('transportation_type')
    alternative_name = e.get('alternative_name')
    _2020_01_13 = e.get('_2020_01_13')
    _2020_01_14 = e.get('_2020_01_14')
    _2020_01_15 = e.get('_2020_01_15')
    _2020_01_16 = e.get('_2020_01_16')
    _2020_01_17 = e.get('_2020_01_17')
    _2020_01_18 = e.get('_2020_01_18')
    _2020_01_19 = e.get('_2020_01_19')
    _2020_01_20 = e.get('_2020_01_20')
    _2020_01_21 = e.get('_2020_01_21')
    _2020_01_22 = e.get('_2020_01_22')
    _2020_01_23 = e.get('_2020_01_23')
    _2020_01_24 = e.get('_2020_01_24')
    _2020_01_25 = e.get('_2020_01_25')
    _2020_01_26 = e.get('_2020_01_26')
    _2020_01_27 = e.get('_2020_01_27')
    _2020_01_28 = e.get('_2020_01_28')
    _2020_01_29 = e.get('_2020_01_29')
    _2020_01_30 = e.get('_2020_01_30')
    _2020_01_31 = e.get('_2020_01_31')
    _2020_02_01 = e.get('_2020_02_01')
    _2020_02_02 = e.get('_2020_02_02')
    _2020_02_03 = e.get('_2020_02_03')
    _2020_02_04 = e.get('_2020_02_04')
    _2020_02_05 = e.get('_2020_02_05')
    _2020_02_06 = e.get('_2020_02_06')
    _2020_02_07 = e.get('_2020_02_07')
    _2020_02_08 = e.get('_2020_02_08')
    _2020_02_09 = e.get('_2020_02_09')
    _2020_02_10 = e.get('_2020_02_10')
    _2020_02_11 = e.get('_2020_02_11')
    _2020_02_12 = e.get('_2020_02_12')
    _2020_02_13 = e.get('_2020_02_13')
    _2020_02_14 = e.get('_2020_02_14')
    _2020_02_15 = e.get('_2020_02_15')
    _2020_02_16 = e.get('_2020_02_16')
    _2020_02_17 = e.get('_2020_02_17')
    _2020_02_18 = e.get('_2020_02_18')
    _2020_02_19 = e.get('_2020_02_19')
    _2020_02_20 = e.get('_2020_02_20')
    _2020_02_21 = e.get('_2020_02_21')
    _2020_02_22 = e.get('_2020_02_22')
    _2020_02_23 = e.get('_2020_02_23')
    _2020_02_24 = e.get('_2020_02_24')
    _2020_02_25 = e.get('_2020_02_25')
    _2020_02_26 = e.get('_2020_02_26')
    _2020_02_27 = e.get('_2020_02_27')
    _2020_02_28 = e.get('_2020_02_28')
    _2020_02_29 = e.get('_2020_02_29')
    _2020_03_01 = e.get('_2020_03_01')
    _2020_03_02 = e.get('_2020_03_02')
    _2020_03_03 = e.get('_2020_03_03')
    _2020_03_04 = e.get('_2020_03_04')
    _2020_03_05 = e.get('_2020_03_05')
    _2020_03_06 = e.get('_2020_03_06')
    _2020_03_07 = e.get('_2020_03_07')
    _2020_03_08 = e.get('_2020_03_08')
    _2020_03_09 = e.get('_2020_03_09')
    _2020_03_10 = e.get('_2020_03_10')
    _2020_03_11 = e.get('_2020_03_11')
    _2020_03_12 = e.get('_2020_03_12')
    _2020_03_13 = e.get('_2020_03_13')
    _2020_03_14 = e.get('_2020_03_14')
    _2020_03_15 = e.get('_2020_03_15')
    _2020_03_16 = e.get('_2020_03_16')
    _2020_03_17 = e.get('_2020_03_17')
    _2020_03_18 = e.get('_2020_03_18')
    _2020_03_19 = e.get('_2020_03_19')
    _2020_03_20 = e.get('_2020_03_20')
    _2020_03_21 = e.get('_2020_03_21')
    _2020_03_22 = e.get('_2020_03_22')
    _2020_03_23 = e.get('_2020_03_23')
    _2020_03_24 = e.get('_2020_03_24')
    _2020_03_25 = e.get('_2020_03_25')
    _2020_03_26 = e.get('_2020_03_26')
    _2020_03_27 = e.get('_2020_03_27')
    _2020_03_28 = e.get('_2020_03_28')
    _2020_03_29 = e.get('_2020_03_29')
    _2020_03_30 = e.get('_2020_03_30')
    _2020_03_31 = e.get('_2020_03_31')
    _2020_04_01 = e.get('_2020_04_01')
    _2020_04_02 = e.get('_2020_04_02')
    _2020_04_03 = e.get('_2020_04_03')
    _2020_04_04 = e.get('_2020_04_04')
    _2020_04_05 = e.get('_2020_04_05')
    _2020_04_06 = e.get('_2020_04_06')
    _2020_04_07 = e.get('_2020_04_07')
    _2020_04_08 = e.get('_2020_04_08')
    _2020_04_09 = e.get('_2020_04_09')
    _2020_04_10 = e.get('_2020_04_10')
    _2020_04_11 = e.get('_2020_04_11')
    _2020_04_12 = e.get('_2020_04_12')
    _2020_04_13 = e.get('_2020_04_13')
    _2020_04_14 = e.get('_2020_04_14')
    _2020_04_15 = e.get('_2020_04_15')
    _2020_04_16 = e.get('_2020_04_16')
    _2020_04_17 = e.get('_2020_04_17')
    _2020_04_18 = e.get('_2020_04_18')
    _2020_04_19 = e.get('_2020_04_19')
    _2020_04_20 = e.get('_2020_04_20')
    _2020_04_21 = e.get('_2020_04_21')
    _2020_04_22 = e.get('_2020_04_22')
    _2020_04_23 = e.get('_2020_04_23')
    _2020_04_24 = e.get('_2020_04_24')
    _2020_04_25 = e.get('_2020_04_25')
    _2020_04_26 = e.get('_2020_04_26')
    _2020_04_27 = e.get('_2020_04_27')
    _2020_04_28 = e.get('_2020_04_28')
    _2020_04_29 = e.get('_2020_04_29')
    _2020_04_30 = e.get('_2020_04_30')
    _2020_05_01 = e.get('_2020_05_01')
    _2020_05_02 = e.get('_2020_05_02')
    _2020_05_03 = e.get('_2020_05_03')
    _2020_05_04 = e.get('_2020_05_04')
    _2020_05_05 = e.get('_2020_05_05')
    _2020_05_06 = e.get('_2020_05_06')
    _2020_05_07 = e.get('_2020_05_07')
    
    #print(e) # check values of element

    # build new elements (dicts) with new schema: individual date columns to single date and the values as change in mobility
    list_data = [_2020_01_13, _2020_01_14, _2020_01_15, _2020_01_16, _2020_01_17, _2020_01_18, _2020_01_19, _2020_01_20, _2020_01_21, _2020_01_22, _2020_01_23, _2020_01_24, _2020_01_25, _2020_01_26, _2020_01_27, _2020_01_28, _2020_01_29, _2020_01_30, _2020_01_31,
                 _2020_02_01, _2020_02_02, _2020_02_03, _2020_02_04, _2020_02_05, _2020_02_06, _2020_02_07, _2020_02_08, _2020_02_09, _2020_02_10, _2020_02_11, _2020_02_12, _2020_02_13, _2020_02_14, _2020_02_15, _2020_02_16, _2020_02_17, _2020_02_18, _2020_02_19, _2020_02_20, _2020_02_21, _2020_02_22, _2020_02_23, _2020_02_24, _2020_02_25, _2020_02_26, _2020_02_27, _2020_02_28, _2020_02_29,
                 _2020_03_01, _2020_03_02, _2020_03_03, _2020_03_04, _2020_03_05, _2020_03_06, _2020_03_07, _2020_03_08, _2020_03_09, _2020_03_10, _2020_03_11, _2020_03_12, _2020_03_13, _2020_03_14, _2020_03_15, _2020_03_16, _2020_03_17, _2020_03_18, _2020_03_19, _2020_03_20, _2020_03_21, _2020_03_22, _2020_03_23, _2020_03_24, _2020_03_25, _2020_03_26, _2020_03_27, _2020_03_28, _2020_03_29, _2020_03_30, _2020_03_31,
                 _2020_04_01, _2020_04_02, _2020_04_03, _2020_04_04, _2020_04_05, _2020_04_06, _2020_04_07, _2020_04_08, _2020_04_09, _2020_04_10, _2020_04_11, _2020_04_12, _2020_04_13, _2020_04_14, _2020_04_15, _2020_04_16, _2020_04_17, _2020_04_18, _2020_04_19, _2020_04_20, _2020_04_21, _2020_04_22, _2020_04_23, _2020_04_24, _2020_04_25, _2020_04_26, _2020_04_27, _2020_04_28, _2020_04_29, _2020_04_30,
                 _2020_05_01, _2020_05_02, _2020_05_03, _2020_05_04, _2020_05_05, _2020_05_06, _2020_05_07]

    month = 1
    day = 13
    returned_list = []
    
    for i in range(0,len(list_data)): #116 dates
      built_dict = {}
      built_dict['geo_type'] = geo_type
      built_dict['region'] = region
      built_dict['transportation_type'] = transportation_type
      built_dict['alternative_name'] = alternative_name
    
      if day > 9:
        date = '2020-0' + str(month) + '-' + str(day)
      else:
        date = '2020-0' + str(month) + '-0' + str(day)
      built_dict['date'] = date
      if list_data[i] != None:
        change = list_data[i] - 100
      else:
        change = None
      built_dict['change_in_baseline_mobility'] = change
      returned_list += [built_dict]
      
      day += 1
      if day == 30 and month == 2:
        month = 3
        day = 1
      elif day == 31 and month == 4:
        month = 5
        day = 1
      elif day == 32:
        if month == 1:
          month = 2
          day = 1
        elif month == 3:
          month = 4
          day = 1
    
    # return list of new elements (dicts)
    #print(returned_list)
    return returned_list
           
def run():
     PROJECT_ID = 'nimble-cortex-266516'
     BUCKET = 'gs://covid_19_cs327_extracredit-dataflow'
     DIR_PATH = BUCKET + '/output/' + datetime.datetime.now().strftime('%Y_%m_%d_%H_%M_%S') + '/'

     # run pipeline on Dataflow 
     options = {
          'runner': 'DataflowRunner',
          'job_name': 'separatemobility',
          'project': PROJECT_ID,
          'temp_location': BUCKET + '/temp',
          'staging_location': BUCKET + '/staging',
          'machine_type': 'n1-standard-4', # https://cloud.google.com/compute/docs/machine-types
          'num_workers': 1
     }
     opts = beam.pipeline.PipelineOptions(flags=[], **options)

     p = beam.Pipeline('DataflowRunner', options=opts)

     sql = 'SELECT * FROM covid_19_apple_mobility_staging.mobility_report'
     bq_source = beam.io.BigQuerySource(query=sql, use_standard_sql=True)

     # format of pipeline transforms: '|' = apply, comment (what the transform does), '>>' = using, function
     query_results = p | 'Read from BigQuery' >> beam.io.Read(bq_source)
        
     # write input PCollection from sql query to log file (this was done by commenting out the ParDo, output, and writing into BQ transforms since the input.txt written was the same as output.txt)
     query_results | 'Write input' >> WriteToText('input.txt')

     # apply ParDo to format the Exit_Only transformation  
     separated_pcoll = query_results | 'Separate Element' >> beam.ParDo(SeparateFn())
        
     # write separated PCollection to log file
     separated_pcoll | 'Write separated log' >> WriteToText('output.txt')
        
     dataset_id = 'covid_19_apple_mobility_modeled'
     table_id = 'mobility_report_beam_DF'
     schema_id = 'geo_type:STRING,region:STRING,transportation_type:STRING,alternative_name:STRING,date:DATE,change_in_baseline_mobility:FLOAT'

     #write PCollection to new BQ table ! Problem resides here
     separated_pcoll | 'Write BQ table' >> beam.io.WriteToBigQuery(dataset=dataset_id, 
                                                  table=table_id, 
                                                  schema=schema_id,
                                                  project=PROJECT_ID,
                                                  create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                                  write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
                                                  batch_size=int(100))
     
     result = p.run()
     result.wait_until_finish()      


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()
