import glob
import pandas as pd
import os
import json

from datetime import datetime
from validation.manifest_validation import check_attributes, extra_columns

def GetManifests(syn, center_map):

    url = 'https://raw.githubusercontent.com/ncihtan/data-models/main/HTAN.model.csv'
    schema = pd.read_csv(url)

    # Exclude test center A, B, C and HTAN HCA immune cells census and test center syn32596076
    manifests = syn.tableQuery("SELECT * FROM syn20446927 \
        WHERE name LIKE 'synapse_storage_manifest%'  \
        AND projectId NOT IN \
        ('syn21989705','syn20977135','syn20687304','syn32596076','syn52929270')"
        ).asDataFrame()

    manifests['modifiedDate'] = pd.to_datetime(manifests['modifiedOn'], unit = 'ms').dt.date
    manifests['createdDate'] = pd.to_datetime(manifests['createdOn'], unit = 'ms').dt.date

    # set manifest submission cutoff date
    '''
    manifest_latest = manifests.loc[
        (manifests['createdDate'] < datetime(2024, 1, 29).date()) 
    ]
    '''
    manifest_latest = manifests

    manifest_latest = manifest_latest.sort_values(
        'modifiedOn', ascending=False
    ).drop_duplicates(['parentId'])

    metadata_manifests = manifest_latest.groupby(['projectId'])

    # --------------------------------------------------------------------------
    meta_map = {}
    extra_cols = {}

    for project_id, dataset_group in metadata_manifests:

        center = syn.get(project_id[0], downloadFile = False).name

        if not center in center_map:
            #do not include project outside official HTAN centers (e.g. test ones)
            continue

        center_id = center_map[center]['center_id']

        datasets = dataset_group.to_dict("records")

        for dataset in datasets:
            manifest_location = './cache/' + center_id + "/" + dataset["id"] + "/"
            manifest_path = manifest_location + "synapse_storage_manifest.csv"

            manifest_id = dataset["id"]

            try:
                manifest = syn.get(manifest_id, downloadLocation=manifest_location, ifcollision='overwrite.local')
                os.rename(glob.glob(manifest_location + "*.csv")[0], manifest_path)
            except:
                continue

            manifest_data = pd.read_csv(manifest_path)

            # Exclude bai files from release
            if 'File Format' in manifest_data.columns:
                manifest_data = manifest_data[~manifest_data['File Format'].isin(['bai','BAI'])].reset_index()

            try:
                component = manifest_data['Component'][0]
            except:
                print("Component not found for manifest %s" % manifest_id)
                continue

            # check that manifest contains mandatory DependsOn attributes
            check_attributes(manifest_data, component, manifest_id)

            # check whether manifests contain non-data-model columns
            extra_cols = extra_columns(list(manifest_data.columns), 
                component.lower(), schema, extra_cols, manifest_id)

            # add in manifest id and center name columns
            manifest_data['Manifest_Id'] = manifest_id
            manifest_data['HTAN Center'] = center
            manifest_data['Manifest_Version'] = manifest.versionNumber

            # create metadata map by merging manifests by component
            if component in meta_map:
                meta_map[component] = pd.concat(
                    [meta_map[component],manifest_data]).reset_index(drop=True)
            else:
                meta_map[component] = manifest_data

    return meta_map, extra_cols
