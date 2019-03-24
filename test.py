import dblink
import os

db = Database("storage.db")

users = dblink.Table("users")
users.add_field('id', int)
users.add_field('first_name', str)

db.add(users)


users.dump([
    [0, "Daniel"],
    [1, "Anna"]
])

for user in users.load():
    print(user)
for user in users.get(id=10):
    print(user)
print(users.count())