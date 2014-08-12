this module lets the user manipulate the data on sql servers(MySQL, MSSQL, ..) the same way it manipulate data from Wakanda data store.

the module reads an sql source from the directory "sqlsources" under solution directory .

after this we can retrieve the sqlds (for an SQL Data Store object same as the ds object) object from with we can do something like:

var sqlds = require('waf-sqlds').sqlds;

var dataclass = sqlds.mysqlsource.benchdb.people;

var entity = dataclass.find('id = 13');

One point to be mentioned is that this module offers a mechanism to do an optimistic locking for entities by now like an "ORM" that will store a version for every entity to check against it for every save done on an entity. This presume that we are the only one which manipulates the data on the database servers.

Two other solutions are planned which are:
HashVersion mechanism:
which will stores two values of hash for every entity (for example sha1 and md5) and before doing a save on an entity check if these two values are always the same. We use two hash functions to minimize collision probability as this will be the product of the collision probability for the hash functions used. this doesn't need to have to be the only user of the database server but there is a small collision risk even if this must be very low.

TriggerVersion mechanism:
which will use a table to store entities versions that will be updated after every update using a trigger. the version manager will read entities version from the trigger table and check against it for every save done. this need the privileges to create the trigger and is somewhat slow.
