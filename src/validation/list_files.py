import pandas as pd


def GetParentIds(meta_map):
    """
    Create table containing primary and parent IDs from all manifests
    """
    
    primary_cols = [
        'HTAN Data File ID',
        'HTAN Biospecimen ID'
    ]

    parent_cols = [
        'HTAN Parent Data File ID',
        'HTAN Parent Biospecimen ID',
        'HTAN Parent ID'
    ]

    all_cols = primary_cols + parent_cols + ['entityId','Component']
    id_list = pd.DataFrame(columns=all_cols)
        
    for component in meta_map:
        data = meta_map[component]
        id_list = pd.concat(
            [id_list,data],axis=0
        ).reset_index(drop=True)[all_cols]
    
    id_list['primaryId'] = [[e for e in row if e==e] for row 
        in id_list[primary_cols].values.tolist()]
    id_list['parentId'] = [[e for e in row if e==e] for row 
        in id_list[parent_cols].values.tolist()]
        
    id_list = id_list[[
        'primaryId','parentId','entityId','Component']].explode(
        'primaryId').explode('parentId')
        
    id_list = id_list.assign(parentId = \
        id_list.parentId.str.split("[,;]")).explode('parentId')

    id_list = id_list[id_list['parentId'].str.contains('Not') == False]
    id_list = id_list.applymap(
        lambda x: x.strip() if isinstance(x, str) else x
    ).drop_duplicates()
        
    return id_list




def FullFileList(meta_map, files):
    """
    Get table listing of all HTAN files
    """
    
    file_list = pd.DataFrame()
    
    for comp in meta_map:
        if any(s in comp for s in files):
            df = meta_map[comp].drop_duplicates()
            
            if comp == 'AccessoryManifest':
                df['entityId'] = df['Accessory Synapse ID']
                df['HTAN Data File ID'] = df['Filename'] = None

            cols = [
                'HTAN Data File ID',
                'Filename',
                'entityId',
                'Manifest_Id',
                'Manifest_Version',
                'HTAN Center',
                'Component'
            ]
            
            if 'Id' in df.columns:
                cols = cols + ['Id']
            if 'Uuid' in df.columns:
                cols = cols + ['Uuid']
            
            file_list = pd.concat([file_list,df[cols]], 
                axis=0, ignore_index=True
            )
    
    file_list['Id'] = file_list['Id'].fillna(file_list['Uuid'])
    file_list.drop(columns=['Uuid'],inplace=True)
    
    return file_list
