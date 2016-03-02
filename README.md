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
    
    db = ormageddon.PostgresqlDatabase(database='ormageddon', user='postgres', host='127.0.0.1')
    
    class User(ormageddon.Model):
    
        class Meta:
            database = db
    
        id = peewee.PrimaryKeyField()
        
    async def print_user(user_id):
        user = await User.get(User.id == user_id)
        print(user)
        
    async def print_users(start=None, stop=None):
        async for user in User.select()[start:stop]:
            print(user)
            
    if __name__ == '__main__':
        loop = asyncio.get_event_loop()
        future = asyncio.gather(print_users(), loop=loop)
        loop.run_until_complete(future)

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
