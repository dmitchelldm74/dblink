import dblink

users = dblink.Table("users")
users.add_field('id', int)
users.add_field('first_name', str)
users.add_field('last_name', str)


users.dump([
    [0, "Daniel", "Mitchell"],
    [1, "Anna", "Mitchell"]
])

for user in users.load():
    print(user)
for user in users.get(id=10):
    print(user)
print(users.count())