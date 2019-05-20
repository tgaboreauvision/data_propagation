import csv
from inspect import getmembers
from pprint import pprint

from crm_class import Odata


"""creates class to access CRM"""
dynamics = Odata(sandbox=False)
dynamics.get_access_token()


"""Open data from csv"""
with open('gocardlaess_data_20190328.csv', 'r') as csvfile:
    reader = csv.DictReader(csvfile)
    data = [row for row in reader]


def get_account_by_account_number(account_number):
    accounts = dynamics.get_req('accounts', fltr=f"name eq '{account_number}'")
    if not accounts:
        print(f'no account found with account number: {account_number}')
    elif len(accounts) > 1:
        print(f'more than one account found with account number: {account_number}')
    else:
        return accounts[0]['accountid']


def patch_gocardless(guid, gocardless, attempt = 1):
    if attempt >= 5:
        return None
    data = {'d4e_gocardlessurl': gocardless}
    request = dynamics.patch_req('accounts', guid, data)
    if request.status_code == 200:
        return True
    else:
        if request.reason == 'Unauthorized':
            attempt += 1
            print(f'oData failure - retrying (attempt {attempt})')
            dynamics.get_access_token()
            patch_gocardless(guid, gocardless, attempt)


for i, row in enumerate(data[1650:]):
    account_number = row['name']
    gocardless = row['gocardlessurl']
    if gocardless:
        # account_guid = get_account_by_account_number(row['account_number'])
        # energy_user_guid = get_energy_user_by_account(account_guid)
        guid = row['accountid']
        if guid:
            patched = patch_gocardless(guid, gocardless)
            if patched:
                print(i, f'patched {account_number} ({guid}) with {gocardless}')
    else:
        print(i, f'no gocardless in file for {account_number}')



