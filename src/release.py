import json
import yaml
import sys
import os
import synapseclient
import pandas as pd
import argparse
import glob
import re
from google.cloud import bigquery

from validation.get_manifests import GetManifests
from validation.list_files import FullFileList, GetParentIds

from validation.file_validation import entity_exists, htan_id_regex, basename_regex, file_name_unique
from validation.file_validation import htan_id_unique, adjacent_bios, unique_bios, unique_demographics
from validation.file_validation import parents_exist, get_channel_files
from validation.generate_release_lists import bq_release_lists

def load_bq(client, project, dataset, table, data):
    '''
    Load dataframe to BigQuery
    '''
    print('Loading: '+dataset+'.'+table)
    
    table_bq = '%s.%s.%s' % (project, dataset, table)

    # make column names bq friendly
    data.columns = data.columns.str.replace(
       '[^0-9a-zA-Z]+','_', regex=True
    )
    schema = [
       bigquery.SchemaField(name, 'STRING') for name in data.columns
    ]

    job_config = bigquery.LoadJobConfig( 
        write_disposition="WRITE_TRUNCATE",      
        autodetect=False,
        schema=schema,
        source_format=bigquery.SourceFormat.CSV,
        allow_jagged_rows=True,
        allow_quoted_newlines=True
    )
    
    job = client.load_table_from_dataframe(
        data, table_bq, job_config=job_config
    )


def main():

   bq_project = 'htan-dcc'
   bq_dataset = 'data_release'

   SYN_PAT = os.environ.get('SYNAPSE_AUTH_TOKEN')

   #instantiate synapse client
   syn = synapseclient.Synapse()

   try:
      syn.login(authToken=SYN_PAT)
   except synapseclient.core.exceptions.SynapseNoCredentialsError:
      print("Please fill in 'username' and 'password'/'api_key' values in .synapseConfig.")
   except synapseclient.core.exceptions.SynapseAuthenticationError:
      print("Please make sure the credentials in the .synapseConfig file are correct.")

   # instantiate BigQuery client
   client = bigquery.Client()

   with open('./config.yaml', 'r') as file:
      config = yaml.safe_load(file)
   
   center_map = config['centers']
   clinical = config['clinical_attributes']
   biospecimen = config['biospecimen_attributes']
   assay_files = config['files']

   id_prov = client.query("""
      SELECT * FROM `htan-dcc.id_provenance.upstream_ids`
   """).result().to_dataframe()

   # Get manifest and file exclusion list
   SHEET_ID = '1tUOd0kiQfW-cjnTbX24Tso5Gnq42k7sKQZLCLFLxBCA'
   SHEET_NAME = 'current'
   url = f'https://docs.google.com/spreadsheets/d/{SHEET_ID}/gviz/tq?tqx=out:csv&sheet={SHEET_NAME}'

   exclude = pd.read_csv(url)
   exclude_files = list(exclude['file id'])
   exclude_manifests = list(exclude['manifest id'])

   # Get Synapse IDs of all released files
   released_entities = client.query("""
      SELECT * FROM `htan-dcc.released.entities`
   """).result().to_dataframe()

   exclude_files = exclude_files + list(released_entities['entityId'])

   # Query HTAN Fileview excluding test center projects
   fileview = syn.tableQuery("SELECT id, currentVersion\
      FROM syn20446927 WHERE type = 'file' \
      AND projectId NOT IN \
      ('syn21989705','syn20977135','syn20687304','syn32596076','syn52929270')"
      ).asDataFrame()

   # Pull tables from Synapse
   meta_map, extra_cols = GetManifests(syn, center_map)
   parent_ids = GetParentIds(meta_map)
   file_list = FullFileList(meta_map, assay_files)

   # Filter out released files and those in datasets we will exclude from release
   sub = 'entityId not in @exclude_files and Manifest_Id not in @exclude_manifests'
   releasable = file_list.query(sub)

   # Validate files
   print( '' )
   print( ' Running validation checks ' )
   print( '' )

   e_id_unique = htan_id_unique(file_list, releasable)
   e_id_regex = htan_id_regex(releasable)
   e_basename_regex = basename_regex(releasable)
   e_exist_syn = entity_exists(fileview, releasable)
   e_adj_bios = adjacent_bios(meta_map, id_prov)
   e_unique_bios = unique_bios(meta_map, id_prov)
   e_unique_demo = unique_demographics(meta_map, id_prov)
   e_parents = parents_exist(releasable,parent_ids)

   img_new = releasable[releasable['Component'] == 'ImagingLevel2']
   channel_aux_files, e_missing_channel = get_channel_files(
      syn, img_new, meta_map['ImagingLevel2'], center_map
   )

   # alias and unique basename checks only for awareness
   # not blockers for release
   # e_alias = check_alias(syn, entities_to_release) 
   # e_name_unique = file_name_unique(file_list, releasable)

   # hash check currently not implemented- too computationally intensive
   #e_hash = check_hash(syn, releasable) 

   errors = [e_id_unique, e_id_regex, e_basename_regex, 
      e_exist_syn, e_adj_bios, e_parents, e_missing_channel]

   merged = {}
   for d in errors:
      for k, v in d.items():
         if k not in merged:
            merged[k] = []
         merged[k].append(v)

   errors_all = pd.DataFrame(list(merged.items()),
      columns=['entityId','Errors']
   )

   releasable = releasable.merge(errors_all, on='entityId', how='left')

   print( '' )
   print( ' Generating validation output lists ' )
   print( '' )

   # generate output of non-data-model columns in current release manifests
   records = []
   for key, value in extra_cols.items():
      for item in value:
         records.append({'Manifest_Id': key, 'column_name': item})

   load_bq(client, bq_project, bq_dataset, 'extra_cols', pd.DataFrame(records))

   # Subset releasable files with no errors
   new_release = releasable[releasable['Errors'].isnull()].sort_values(
      by=['HTAN Center','Component']).drop(columns=['Errors'], 
      axis=1)
   
   bq_release_lists(client, syn, fileview, center_map, new_release, 
      meta_map, id_prov, clinical, biospecimen)

   # Create list of files with errors
   error_list = releasable[~(releasable['Errors'].isnull())].sort_values(
      by=['HTAN Center','Component'])
   
   load_bq(client, bq_project, bq_dataset, 'errors', error_list)


   # Generate list of errors containing duplicate bios/case IDs
   dup_bios = pd.DataFrame(list(e_unique_bios.items()),
      columns=['entityId','Errors'])
   dup_demo = pd.DataFrame(list(e_unique_demo.items()),
      columns=['entityId','Errors'])

   df_cb_errors = pd.concat([dup_bios,dup_demo])

   subset_cb_errors = releasable[['entityId','HTAN Center']].merge(
      df_cb_errors, on='entityId', how='inner')[['HTAN Center','Errors']]

   subset_cb_errors = subset_cb_errors.loc[
      subset_cb_errors.astype(str).drop_duplicates().index]

   load_bq(client, bq_project, bq_dataset, 'clin_bio_errors', subset_cb_errors)

   print( '' )
   print( ' Done ' )
   print( '' )

if __name__ == "__main__":

   main()
