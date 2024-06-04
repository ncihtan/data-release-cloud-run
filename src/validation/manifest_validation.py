import pandas as pd
import sys

def check_attributes(manifest, component, manifest_id):

    cols = list(manifest.columns)

    l1_required = ['Component', 'Filename', 'File Format', 'HTAN Parent Biospecimen ID', 'HTAN Data File ID']
    l234_required = ['Component', 'Filename', 'File Format', 'HTAN Parent Data File ID', 'HTAN Data File ID']

    if any(x in component for x in ['Level1','Auxiliary']):
        
        for req in l1_required:
            if req in cols:
                pass
            else:
                e = f'{req} is missing from DependsOn for manifest {manifest_id}'
                sys.exit(e)

    elif any(x in component for x in ['Level2','Level3','Level4']):
        
        for req in l234_required:
            if req in cols:
                pass
            elif (component in ['ImagingLevel2','SRRSImagingLevel2']) and (req == 'HTAN Parent Data File ID'):
                pass
            else:
                e = f'{req} is missing from DependsOn for manifest {manifest_id}'
                print(e)
                continue

    else:
        pass


def extra_columns(columns, component, data_model, out, manifest_id):

    attr = ['entityId','Uuid','Id','eTag','index'] + list(data_model['Attribute'])

    diff = list(set(columns)-set(attr))

    if len(diff) > 0:
        out[manifest_id] = diff

    return out
