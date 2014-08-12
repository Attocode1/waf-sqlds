this module lets the user manipulate the data on sql servers(MySQL, MSSQL, ..) the same way it manipulate data from Wakanda data store.

the module reads an sql source from the directory "sqlsources" under solution directory .

after this we can retrieve the sqlds (for an SQL Data Store object same as the ds object) object from with we can do something like:

var sqlds = require('waf-sqlds').sqlds;

var dataclass = sqlds.mysqlsource.benchdb.people;

var entity = dataclass.find('id = 13');
