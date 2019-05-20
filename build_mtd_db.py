from to_postgres import db_builder
import settings

# create instance of builder class
new_db = db_builder(settings.local_postgres_db_name, settings.local_postgres_db_password)

# create target db
new_db.create_target_db(drop_existing=False)

account_fields = ['accountid', 'name']


new_db.add_table('accounts_small', source='odata', entity='accounts', select=account_fields)
new_db.add_table('supply_points', source='odata', entity='d4e_energy_supply_points')
new_db.add_table('meters', source='odata', entity='d4e_meters')
new_db.add_table('registers', source='odata', entity='d4e_registers')



