from db import DB

db = DB("test.db")

print "Current database keys: ", db.keys()

db['foo'] = 'bar'
print "Foo value: ", db['foo']

db.set('foo2', 'bar2')
print "Foo2 value :", db.get('foo2')

db.close()
del db
