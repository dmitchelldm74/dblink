from dblink import Database, Table, autoid
import os

db = Database("storage")

users = Table("users")
users.add_field('id', int)
users.add_field('first_name', str)
users.add_field('last_name', str, default="Mitchell")
users.add_field('cost', float)
users.add_field('attr', dict)

db.add(users)

users.dump([
    [users.autoid, "Anna", "Mitchell", 4.25, {"allergic": "no"}],
])

#print(users.pop(first_name="Anna"))

for user in users.load():
    print(user, "user")

print(users.count())