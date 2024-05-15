import json 
import re
import pandas as pd
import synapseclient
from google.cloud import bigquery



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
        dsc = schema[schema['Attribute'] == attribute]['Description'].values[0]
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

    

def bq_release_lists(client, syn, fileview, center_map, entities, 
    meta_map, id_prov, clinical, biospecimen):

    schema = client.query("""
        SELECT * FROM `htan-dcc.metadata.data-model`
    """).result().to_dataframe()

    entities_bq = client.query("""
        SELECT * EXCEPT(channel_metadata_version,
        channel_metadata_synapseId) 
        FROM `htan-dcc.released.entities`
    """).result().to_dataframe()

    img_bq = client.query("""
        SELECT entityId, Channel_Metadata_Filename, HTAN_Center
        FROM `htan-dcc.combined_assays.ImagingLevel2`
        WHERE Channel_Metadata_Filename NOT LIKE 'Not Applicable'
    """).result().to_dataframe()

    entities['type'] = ['folder' if x == 'AccessoryManifest' else 'file' for x in list(entities['Component'])]
    entities['CDS_Release'] = None
    entities['IDC_Release'] = None

    ## Add channel metadata IDs for imaging files
    img = entities[entities['Component']=='ImagingLevel2']
    img = img.merge(img_bq,how='left',on='entityId')

    channel_df = img[[
        'Channel_Metadata_Filename','HTAN_Center']].drop_duplicates()
    channel_df.dropna(subset=['Channel_Metadata_Filename'],inplace=True)

    channel_version = []
    channel_id = []

    # walk down provided Synapse path to 
    # get entityId of channel metadata file
    for i,r in channel_df.iterrows():
        channel = r['Channel_Metadata_Filename']
        id = center_map[r['HTAN_Center']]['synapse_id']      
        
        # Use Synapse ID directly if provided
        if bool(re.search("^(syn)[0-9]{8}$", channel)):
            cv = fileview[fileview['id'] == channel]['currentVersion'].values[0]
            channel_version.append(cv)
            channel_id.append(channel)
        
        else:
            folders = channel.split('/')
            for f in folders:
                children = list(syn.getChildren(id, 
                    includeTypes=['folder','file']))
                record = [j for j in children if j['name'] == f]
                
                if len(record) == 0:
                    print('Channel metadata file %s not found' % channel)
                    channel_version.append('Channel file not found')
                    channel_id.append('Channel file not found')
                    continue
                
                else:
                    id=record[0]['id']
            
            cv = fileview[fileview['id'] == id]['currentVersion'].values[0]
            channel_version.append(cv)
            channel_id.append(id)

    channel_df['channel_metadata_synapseId'] = channel_id
    channel_df['channel_metadata_version'] = channel_version

    channel_full = img.merge(channel_df, how='left', 
        on=['Channel_Metadata_Filename','HTAN_Center'])[[
            'channel_metadata_version',
            'channel_metadata_synapseId','entityId'
        ]]

    entities = entities.merge(channel_full[[
        'channel_metadata_version',
        'channel_metadata_synapseId','entityId']],
        how='left',on='entityId')

    ent_schema = []
    default_type='STRING'

    for column_name, dtype in entities.dtypes.items():
        ent_schema.append(
            {
                'name': re.sub('[^0-9a-zA-Z]+', '_', column_name),
                'type': default_type if column_name not in 
                    ['Manifest_Version'] else 'integer',
                'description': get_description(column_name, schema)
            }
        )

    entities.columns = entities.columns.str.replace(
        '[^0-9a-zA-Z]+','_', regex=True
    )

    load_bq(client, 'htan-dcc', 'data_release', 'shortlist', 
        entities.drop_duplicates(), ent_schema
    )


    ## ----------------------------------------------------------------------
    ## Update the data_release.metadata table
    ## We need to pull in manifest IDs of the assay manifests as well as 
    ## clinical and biospecimen manifests for patients from which data is derived

    metadata_bq = client.query("""
        SELECT DISTINCT Manifest_Id
        FROM `htan-dcc.released.metadata`
    """).result().to_dataframe()

    metadata = entities[['Manifest_Id']].drop_duplicates()

    for c in clinical:
        manifests = entities[['HTAN_Data_File_ID']].merge(
            id_prov[['HTAN_Participant_ID','HTAN_Assayed_Biospecimen_ID',
            'HTAN_Data_File_ID']], how='left',
            on = 'HTAN_Data_File_ID'
        )
        cm = meta_map[c][[
            'HTAN Participant ID','Manifest_Id']
            ].drop_duplicates() 
        manifests = manifests.merge(cm,how='left',
            left_on='HTAN_Participant_ID',
            right_on='HTAN Participant ID'
        )
        metadata = pd.concat(
            [metadata,manifests[['Manifest_Id']]]
        )
    
    for b in biospecimen:
        manifests = entities[['HTAN_Data_File_ID']].merge(
            id_prov[['HTAN_Participant_ID','HTAN_Assayed_Biospecimen_ID',
            'HTAN_Data_File_ID']], how='left',
            on='HTAN_Data_File_ID'
        )
        bm = meta_map[b][[
            'HTAN Biospecimen ID','Manifest_Id','Manifest_Version']
            ].drop_duplicates()
        manifests = manifests.merge(bm,how='left',
            left_on='HTAN_Assayed_Biospecimen_ID',
            right_on='HTAN Biospecimen ID'
        )
        metadata = pd.concat(
            [metadata,manifests[['Manifest_Id']]]
        )

    metadata.dropna(subset=['Manifest_Id'],inplace=True)

    metadata = metadata.merge(fileview,how='left',
        left_on='Manifest_Id',right_on='id')

    metadata = metadata[['Manifest_Id','currentVersion']].rename(
        columns={'currentVersion': 'Manifest_Version'}
    )

    met_schema = []
    default_type='STRING'

    for column_name, dtype in metadata.dtypes.items():
        met_schema.append(
            {
                'name': re.sub('[^0-9a-zA-Z]+', '_', column_name),
                'type': default_type if column_name not in 
                    ['Manifest_Version'] else 'integer',
                'description': get_description(column_name, schema)
            }
        )

    # load to BQ
    load_bq(client, 
        'htan-dcc', 'data_release', 'manifests',
        metadata.drop_duplicates(), met_schema
    )
