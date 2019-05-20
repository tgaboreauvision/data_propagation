import csv
from inspect import getmembers
from pprint import pprint

from crm_class import Odata

"""creates class to access CRM"""
dynamics = Odata(sandbox=False)
dynamics.get_access_token()

"""Open data from csv"""
with open('deeplink_data_20190325.csv', 'r') as csvfile:
    reader = csv.DictReader(csvfile)
    data = [row for row in reader]

print(len(data))

# def get_contacts_from_email(email):
#     """Write functionm to return contacts using email address"""
#     contacts = dynamics.get_req('contacts', fltr=f"emailaddress1 eq '{email}'")
#     return contacts


def get_account_by_account_number(account_number):
    accounts = dynamics.get_req('accounts', fltr=f"name eq '{account_number}'")
    if not accounts:
        print(f'no account found with account number: {account_number}')
    elif len(accounts) > 1:
        print(f'more than one account found with account number: {account_number}')
    else:
        return accounts[0]['accountid']


def patch_deeplink(guid, deeplink):
    data = {'websiteurl': deeplink}
    request = dynamics.patch_req('accounts', guid, data)
    if request.status_code == 200:
        return True
    else:
        pprint(getmembers(request))


for i, row in enumerate(data[12900:]):
    account_number = row['name']
    deeplink = row['deeplink']
    if deeplink:
        guid = row['accountid']
        if guid:
            patched = patch_deeplink(guid, deeplink)
            if patched:
                print(i, f'patched {account_number} ({guid}) with {deeplink}')
    else:
        print(i, f'no deeplink in file for {account_number}')



