from flowfile_core.schemas.input_schema import RawData
import flowfile as ff

df_1 = ff.from_raw_data(RawData(
    **{'columns': [{'name': 'id', 'data_type': 'Integer'}, {'name': 'tags', 'data_type': 'String'}],
       'data': [[1, 2, 3], ['python,data,analysis', 'machine,learning', 'etl,pipeline,flowfile']]}))
df_2 = df_1.collect().with_columns(ff.col("tags").str.split(",").alias("tag")).explode('tag')
