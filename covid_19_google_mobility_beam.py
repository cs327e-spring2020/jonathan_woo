import logging
import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText

class FormatCounty(beam.DoFn):
  def process(self, element):
    e = element # element is a dict
    
    # obtain the components of each element instance p
    country_region_code = e.get('country_region_code')
    country_region = e.get('country_region')
    sub_region_1 = e.get('sub_region_1')
    sub_region_2 = e.get('sub_region_2')
    date = e.get('date')
    retail_and_recreation_percent_change_from_baseline = e.get('retail_and_recreation_percent_change_from_baseline')
    grocery_and_pharmacy_percent_change_from_baseline = e.get('grocery_and_pharmacy_percent_change_from_baseline')
    parks_percent_change_from_baseline = e.get('parks_percent_change_from_baseline')
    transit_stations_percent_change_from_baseline = e.get('transit_stations_percent_change_from_baseline')
    workplaces_percent_change_from_baseline = e.get('workplaces_percent_change_from_baseline')
    residential_percent_change_from_baseline = e.get('residential_percent_change_from_baseline')
    #print(se) # check values of p

    # split sub_region2
    if sub_region_2 != None:
      if sub_region_2 != '':
        split = sub_region_2.split(' ')
        returnedString = ''
        foundCounty = False
    
        if split[-1].upper() == 'COUNTY':
          split = split[0:-1] #exclude county string
          foundCounty = True
          if foundCounty:
            for i in range(0,len(split)):
              if i == len(split) - 1:
                returnedString += split[i]
              else:
                returnedString += split[i] + ' '
          else:
            returnedString = sub_region_2
      else:
        returnedString = None
    else:
      returnedString = None
    # replace the county value with the modified county string (if the string had county in it)
    e['sub_region_2'] = returnedString
    
    # return new element
    return [e] 
           
def run():
    PROJECT_ID = 'nimble-cortex-266516'

     # Project ID is required when using the BQ source
    options = {
     'project': PROJECT_ID
     }
    opts = beam.pipeline.PipelineOptions(flags=[], **options)

     # Create beam pipeline using local runner
    p = beam.Pipeline('DirectRunner', options=opts)

    sql = 'select * from covid_19_google_mobility_staging.mobility_report where country_region = "United States" and sub_region_2 is not null limit 50'
    bq_source = beam.io.BigQuerySource(query=sql, use_standard_sql=True)

     # format of pipeline transforms: '|' = apply, comment (what the transform does), '>>' = using, function
    query_results = p | 'Read from BigQuery' >> beam.io.Read(bq_source)
        
     # write input PCollection from sql query to log file (this was done by commenting out the ParDo, output, and writing into BQ transforms since the input.txt written was the same as output.txt)
    #query_results | 'Write input' >> WriteToText('google_input.txt')

     # apply ParDo to format the Coordinate transformation  
    formatted_county_pcoll = query_results | 'Format County' >> beam.ParDo(FormatCounty())
        
     # write formatted District PCollection to log file
    #formatted_county_pcoll | 'Write formatted log' >> WriteToText('google_output.txt')

    dataset_id = 'covid_19_google_mobility_modeled'
    table_id = 'mobility_report_Beam'
    schema_id = 'country_region_code:STRING,country_region:STRING,sub_region_1:STRING,sub_region_2:STRING,date:DATE,retail_and_recreation_percent_change_from_baseline:INTEGER,grocery_and_pharmacy_percent_change_from_baseline:INTEGER,parks_percent_change_from_baseline:INTEGER,transit_stations_percent_change_from_baseline:INTEGER,workplaces_percent_change_from_baseline:INTEGER,residential_percent_change_from_baseline:INTEGER'

     #write PCollection to new BQ table ! Problem resides here
    formatted_county_pcoll | 'Write BQ table' >> beam.io.WriteToBigQuery(dataset=dataset_id, 
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
