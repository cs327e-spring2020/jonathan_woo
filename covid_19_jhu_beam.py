import logging
import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText

class SplitProvinceState(beam.DoFn):
  def process(self, element):
    e = element # element is a dict
    
    # obtain the components of each element instance p
    province_state = e.get('province_state')
    country_region = e.get('country_region')
    date = e.get('date')
    latitude = e.get('latitude')
    longitude = e.get('longitude')
    location_geom = e.get('location_geom')
    confirmed = e.get('confirmed')
    deaths = e.get('deaths')
    recovered = e.get('recovered')
    active = e.get('active')
    fips = e.get('fips')
    admin2 = e.get('admin2')
    combined_key = e.get('combined_key')
    #print(e) # check values of e

    # split province_state
    if province_state != None:
      if province_state != '':
        split = province_state.split(',')
        returnedProvince = ''
        foundState = False
        state = split[-1].strip()
    
        if len(split) > 1 and len(state) == 2: # state abbrivations are two characters long
          provinceName = split[0:-1] #exclude state string
          foundState = True
          
        if foundState:
          for i in range(0,len(provinceName)):
            if i == len(split) - 1:
              returnedProvince += split[i]
            else:
              returnedProvince += split[i] + ' '
          if state == 'AL':
            state = 'Alabama'
          elif state == 'AK':
            state = 'Alaska'
          elif state == 'AZ':
            state = 'Arizona'
          elif state == 'AR':
            state = 'Arkansas'
          elif state == 'CA':
            state = 'California'
          elif state == 'CO':
            state = 'Colorado'
          elif state == 'CT':
            state = 'Connecticut'
          elif state == 'DE':
            state = 'Delaware'
          elif state == 'FL':
            state = 'Florida'
          elif state == 'GA':
            state = 'Georgia'
          elif state == 'HI':
            state = 'Hawaii'
          elif state == 'ID':
            state = 'Idaho'
          elif state == 'IL':
            state = 'Illinois'
          elif state == 'IN':
            state = 'Indiana'
          elif state == 'IA':
            state = 'Iowa'
          elif state == 'KS':
            state = 'Kansas'
          elif state == 'KY':
            state = 'Kentucky'
          elif state == 'LA':
            state = 'Louisiana'
          elif state == 'ME':
            state = 'Maine'
          elif state == 'MD':
            state = 'Maryland'
          elif state == 'MA':
            state = 'Massachusetts'
          elif state == 'MI':
            state = 'Michigan'
          elif state == 'MN':
            state = 'Minnesota'
          elif state == 'MS':
            state = 'Mississippi'
          elif state == 'MO':
            state = 'Missouri'
          elif state == 'MT':
            state = 'Montana'
          elif state == 'NE':
            state = 'Nebraska'
          elif state == 'NV':
            state = 'Nevada'
          elif state == 'NH':
            state = 'New Hampshire'
          elif state == 'NJ':
            state = 'New Jersey'
          elif state == 'NM':
            state = 'New Mexico'
          elif state == 'NY':
            state = 'New York'
          elif state == 'NC':
            state = 'North Carolina'
          elif state == 'ND':
            state = 'North Dakota'
          elif state == 'OH':
            state = 'Ohio'
          elif state == 'OK':
            state = 'Oklahoma'
          elif state == 'OR':
            state = 'Oregon'
          elif state == 'PA':
            state = 'Pennsylvania'
          elif state == 'RI':
            state = 'Rhode Island'
          elif state == 'SC':
            state = 'South Dakota'
          elif state == 'TN':
            state = 'Tennessee'
          elif state == 'TX':
            state = 'Texas'
          elif state == 'UT':
            state = 'Utah'
          elif state == 'VT':
            state = 'Vermont'
          elif state == 'VA':
            state = 'Virginia'
          elif state == 'WA':
            state = 'Washington'
          elif state == 'WV':
            state = 'West Virginia'
          elif state == 'WI':
            state = 'Wisconsin'
          elif state == 'WY':
            state = 'Wyoming'
        else:
          returnedProvince = province_state
          state = None
      else:
        returnedProvince = None
        state = None
    else:
      returnedProvince = None
      state = None
    # build new element with the province and state fields
    eNew = {}

    eNew['province'] = returnedProvince
    eNew['state'] = state

    if country_region == 'US':
      eNew['country_region'] = 'United States'
    else:
      eNew['country_region'] = country_region
    eNew['date'] = date
    #eNew['latitude'] = latitude #not needed therefore NOT included in schema
    #eNew['longitude'] = longitude #not needed
    #eNew['location_geom'] = location_geom #not needed
    eNew['confirmed'] = confirmed
    eNew['deaths'] = deaths
    eNew['recovered'] = recovered
    if confirmed != None and deaths != None and recovered != None:
      eNew['active'] = confirmed - deaths - recovered #make active cases value since original data has nulls
    else:
      eNew['active'] = active #use given value if can't perform operation
    #eNew['fips'] = fips #not needed
    eNew['admin2'] = admin2
    eNew['combined_key'] = combined_key
    
    # return new element
    return [eNew] 
           
def run():
    PROJECT_ID = 'nimble-cortex-266516'

     # Project ID is required when using the BQ source
    options = {
     'project': PROJECT_ID
     }
    opts = beam.pipeline.PipelineOptions(flags=[], **options)

     # Create beam pipeline using local runner
    p = beam.Pipeline('DirectRunner', options=opts)

    sql = 'select * from covid_19_jhu_staging.summary where country_region = "US" limit 50'
    bq_source = beam.io.BigQuerySource(query=sql, use_standard_sql=True)

     # format of pipeline transforms: '|' = apply, comment (what the transform does), '>>' = using, function
    query_results = p | 'Read from BigQuery' >> beam.io.Read(bq_source)
        
     # write input PCollection from sql query to log file (this was done by commenting out the ParDo, output, and writing into BQ transforms since the input.txt written was the same as output.txt)
    #query_results | 'Write input' >> WriteToText('jhu_input.txt')

     # apply ParDo to format the Coordinate transformation  
    split_pcoll = query_results | 'split provinceState' >> beam.ParDo(SplitProvinceState())
        
     # write formatted District PCollection to log file
    #split_pcoll | 'Write formatted log' >> WriteToText('jhu_output.txt')

    dataset_id = 'covid_19_jhu_modeled'
    table_id = 'summary_Beam'
    schema_id = 'province:STRING,state:STRING,country_region:STRING,date:DATE,confirmed:INTEGER,deaths:INTEGER,recovered:INTEGER,active:INTEGER,admin2:STRING,combined_key:STRING'

     #write PCollection to new BQ table ! Problem resides here
    split_pcoll | 'Write BQ table' >> beam.io.WriteToBigQuery(dataset=dataset_id, 
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
