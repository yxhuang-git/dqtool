copy into ${schema}.${table}
from  @dqtool_s3_stage/${filename}
FILE_FORMAT = (TYPE = 'CSV' date_format ='YYYY/MM/DD' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1)
FORCE = TRUE; 					
