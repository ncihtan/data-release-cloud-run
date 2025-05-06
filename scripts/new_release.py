import pandas as pd 
from google.cloud import bigquery
import synapseclient
import re
import argparse

def load_bq(client, project, dataset, table, data, schema):
    '''
    Load table and schema to BigQuery
    '''

    print( 'Loading %s.%s.%s to BigQuery' 
        % (project, dataset, table))

    table_bq = '%s.%s.%s' % (project, dataset, table)
    job_config = bigquery.LoadJobConfig(
        schema=schema, 
        write_disposition="WRITE_TRUNCATE",
        autodetect=False,
        allow_jagged_rows=True,
        allow_quoted_newlines=True,
        source_format=bigquery.SourceFormat.CSV
    )

    job = client.load_table_from_dataframe(
        data, table_bq, job_config=job_config
    )

def get_description(attribute, schema):
    
    SHEET_ID = '1RpwQqY7xi-arWJMOMpF0EOhbXPCcQudv8RZ_fp0o_es'
    SHEET_NAME = 'Sheet1'
    url = f'https://docs.google.com/spreadsheets/d/{SHEET_ID}/gviz/tq?tqx=out:csv&sheet={SHEET_NAME}'
    
    add_descriptions = pd.read_csv(url)
    
    try:
        dsc = schema[
            schema['Attribute'] == attribute.replace('_',' ')]['Description'].values[0]
        description = (dsc[:1024]) if len(dsc) > 1024 else dsc

    except:
        try:
            dsc = add_descriptions[
                add_descriptions['Attribute'] == attribute
            ]['Description'].values[0]
            
            description = (dsc[:1024]) if len(dsc) > 1024 else dsc
        except:
            description = 'Description unavailable. Contact DCC for more information'
            print(
                '{} attribute not found in HTAN schema'.format(
                    attribute)
            )

    return description

def main(args):

    htan_release = args.releaseVersion.replace('release','Release ')

    # instantiate BigQuery client
    client = bigquery.Client()

    #instantiate synapse client
    syn = synapseclient.Synapse()
    syn.login()

    schema = client.query("""
        SELECT * FROM `htan-dcc.metadata.data-model`
    """).result().to_dataframe()

    # Query HTAN Fileview excluding test center projects
    fileview = syn.tableQuery("SELECT id, currentVersion\
        FROM syn20446927 WHERE type = 'file' \
        AND projectId NOT IN \
        ('syn21989705','syn20977135','syn20687304','syn32596076','syn52929270')"
        ).asDataFrame()


    # Update released.entities table
    parameters = [
        bigquery.ScalarQueryParameter('htan_release', 'STRING', htan_release)
    ]
    query = """
    SELECT entityId, @htan_release AS Data_Release, Id, type, CDS_Release,
    IDC_Release, Component, channel_metadata_version, channel_metadata_synapseId 
    FROM `htan-dcc.data_release.shortlist`
    UNION ALL
    SELECT * FROM `htan-dcc.released.entities`
    """
    job_config = bigquery.QueryJobConfig()
    job_config.query_parameters = parameters

    all_entities = client.query(query, job_config=job_config).result().to_dataframe()
    
    # Remove BAI files from released entity listings
    bai_files = syn.tableQuery("SELECT id, currentVersion\
        FROM syn20446927 WHERE type = 'file' \
        AND name LIKE '%.bai' \
        AND projectId NOT IN \
        ('syn21989705','syn20977135','syn20687304','syn32596076','syn52929270')"
        ).asDataFrame()
        
    all_entities = all_entities[~all_entities['entityId'].isin(bai_files['id'])].drop_duplicates()

    # create BigQuery table schema
    ent_schema = []
    default_type='STRING'

    for column_name, dtype in all_entities.dtypes.items():
        ent_schema.append(
            {
                'name': re.sub('[^0-9a-zA-Z]+', '_', column_name),
                'type': default_type if column_name not in 
                    ['Manifest_Version'] else 'integer',
                'description': get_description(column_name, schema)
            }
        )

    # load to BQ
    versioned_entities = 'entities_v' + re.sub(
        r'[^a-zA-Z0-9_]', '_', htan_release.replace('Release ', '')).lower()

    load_bq(client, 'htan-dcc', 'released', versioned_entities, 
        all_entities, ent_schema
    )

    load_bq(client, 'htan-dcc', 'released', 'entities', 
        all_entities, ent_schema
    )


    # Update released.metadata table
    # get Synapse IDs of all old and new manifests
    all_metadata = client.query("""
        WITH old AS (
        SELECT Manifest_Id 
        FROM `htan-dcc.released.metadata`
        ),
        new_metadata AS (
        SELECT Manifest_Id 
        FROM `htan-dcc.data_release.manifests`
        WHERE Manifest_Version IS NOT NULL
        )
        SELECT DISTINCT *
        FROM (
        SELECT * FROM old
        UNION ALL
        SELECT * FROM new_metadata
        ) AS combined_data;
    """).result().to_dataframe()

    # merge to fileview to get latest version
    all_metadata = all_metadata.merge(
        fileview, how='left', left_on='Manifest_Id',
        right_on='id').drop(columns=['id'])

    all_metadata.rename(columns={"currentVersion": "Manifest_Version"},inplace=True)

    # create BigQuery table schema
    met_schema = []
    default_type='STRING'

    for column_name, dtype in all_metadata.dtypes.items():
        met_schema.append(
            {
                'name': re.sub('[^0-9a-zA-Z]+', '_', column_name),
                'type': default_type if column_name not in 
                    ['Manifest_Version'] else 'integer',
                'description': get_description(column_name, schema)
            }
        )

    versioned_metadata = 'metadata_v' + re.sub(
        r'[^a-zA-Z0-9_]', '_', htan_release.replace('Release ', '')).lower()

    # load to BQ
    load_bq(client, 
        'htan-dcc', 'released', versioned_metadata,
        all_metadata.drop_duplicates(), met_schema
    )

    load_bq(client, 
        'htan-dcc', 'released', 'metadata',
        all_metadata.drop_duplicates(), met_schema
    )



if __name__ == "__main__":

   parser = argparse.ArgumentParser()

   parser.add_argument('-r', '--releaseVersion', 
      required=True, 
      help = 'Version number of major HTAN data release e.g. "release4.0"')

   args = parser.parse_args()

   main(args)
