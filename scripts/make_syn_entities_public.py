import synapseclient
import json

def make_public(synid, syn):
    print(synid)
    registered = syn.getPermissions(synid, '273948') == ['DOWNLOAD','READ']
    if registered:
        pass
    else:
        syn.setPermissions(synid,'273948',['DOWNLOAD','READ'])
    public = syn.getPermissions(synid, 'PUBLIC') == ['READ']
    if public:
        pass
    else:
        syn.setPermissions(synid, principalId = 'PUBLIC', accessType = ['READ'])

def main():

    syn = synapseclient.Synapse()
    syn.login()

    f = json.load(open('release4/synapse_public_entities.json'))

    for j in f:
        make_public(j,syn)

        
if __name__ == "__main__":

	main()

