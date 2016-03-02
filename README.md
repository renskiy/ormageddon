# ORMageddon

ORMageddon is asynchronous ORM framework based on [peewee ORM](https://github.com/coleifer/peewee). For now it supports only PostgreSQL using [aiopg](https://github.com/aio-libs/aiopg).

Status
------
ORMageddon is in active development stage and is not ready for production yet.

Some examples
-------------
You can test ORMageddon using following little example:

    import ormageddon
    import peewee
    
    db = ormageddon.PostgresqlDatabase(database='ormageddon')
    
    class User(ormageddon.Model):
    
        class Meta:
            database = db
    
        id = peewee.PrimaryKeyField()
        
    async def print_user(user_id):
        user = await User.get(User.id == user_id)
        print(user)
        
    async def print_users(start, stop):
        async for user in User.select()[start:stop]:
            print(user)

Transactions
------------

    async def manual_transaction():
        await db.begin()
        try:
            # do whatever you need
        except:
            await db.rollback()
            raise
        else:
            await db.commit()
        
    async def transaction_context():
        async with db.transaction() as transaction:
            # do whatever you need
