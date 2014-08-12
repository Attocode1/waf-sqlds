/* Copyright (c) 4D, 2011
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */

var sql = require('waf-sql');

var sessionManager = null;

var getSessionManager = function () {
	if(sessionManager == null) {
		sessionManager = new SQLSessionManager();
	}

	return sessionManager;
};

var SQLSessionManager = function() {
	var sessions = {};
	var params = {};

	/*
	** addSession
	*/
	this.addSession = function(sourceName, inParams) {
		params[sourceName] = inParams;
	};

	/*
	** getSession
	*/
	this.getSession = function(sourceName) {
		if(sessions[sourceName] === undefined) {
			//if this._sessions[sourceName].isConnected
			var sourceParams = params[sourceName];
			sessions[sourceName] = sql.connect(sourceParams);
		}

		var session = sessions[sourceName];
		return session;
	};

	/*
	** getPrimaryKey
	*/
	this.getPrimaryKey = function(dataclass, filter) {
		var session = this.getSession(dataclass.sourceName);
		var key = dataclass.getPrimaryKey();
		var attributes = Object.getOwnPropertyNames(key).join(",");
		var primaryKey = session.find(attributes, dataclass.name, filter);

		return primaryKey;
	};

	/*
	** getPrimaryKeys
	*/
	this.getPrimaryKeys = function(dataclass, filter) {
		var session = this.getSession(dataclass.sourceName);
		var key = dataclass.getPrimaryKey();
		var attributes = Object.getOwnPropertyNames(key).join(",");
		var res = session.select(attributes, dataclass.name, filter);
		var primaryKeys = res.getAllRows();

		return primaryKeys;
	};

	/*
	** getFirst
	*/
	this.getFirst = function(dataclass, attributes, filter) {
		var session = this.getSession(dataclass.sourceName);
		var row = session.find(attributes.join(","), dataclass.name, filter);

		return row;
	};
	
	/*
	** getFirstWithChecksum
	*/	
	this.getFirstWithChecksum = function(dataclass, attributes, filter) {
		var query = "select ";
		query += attributes.join(',');
		
		query += "sha1(concat(";
		var allAttributes = dataclass.getAttributesNames();
		query += allAttributes.join(',');
		query += ")) as checksum";
		query += " from " + dataclass.name + " where ";
		Object.getOwnPropertyNames(filter).forEach(function(a) {
			query += "`" + a + "` = ";
			var value = filter[a];
			if(typeof value == "number") {
				query += value + " ";
			} else if(value instanceof Date) {
				query += "'" + value.getUTCFullYear() + "-" + (value.getUTCMonth() + 1) + "-" + value.getUTCDate();
				query += " " + value.getUTCHours() + ":" + value.getUTCMinutes() + ":" + value.getUTCSeconds() + ".";
				query += value.getUTCMilliseconds();
			} else {
				query += "'" + value + "' ";
			}
		});
		
		var session = this.getSession(dataclass.sourceName);
		var res = session.execute(query);
		var row = res.getNextRow();
		
		return row;
	};	

	/*
	** getAll
	*/
	this.getAll = function(dataclass, attributes, filter) {
		var session = this.getSession(dataclass.sourceName);
		var res = session.select(attributes.join(","), dataclass.name, filter);
		var rows = res.getAllRows();

		return rows;
	};


	/*
	** save
	*/
	this.save = function(dataclass, values, filter) {
		var session = this.getSession(dataclass.sourceName);
		session.update(dataclass.name, values, filter);
	};


	/*
	** saveNew
	*/
	this.saveNew = function(dataclass, values) {
		var session = this.getSession(dataclass.sourceName);
		session.insert(dataclass.name, [values]);
	};
	
	this.saveWithChecksumVerification = function(dataclass, values, filter, checksum) {
		var query = "update `" + dataclass.name + "` set ";
		var first = true;
		Object.getOwnPropertyNames(values).forEach(function(a) {
			if(first)
				first = false;
			else
				query += ",";
			
			query += "`" + a + "` = ";
			var value = values[a];
			if(typeof value == "number") {
				query += value + " ";
			} else if(value instanceof Date) {
				query += "'" + value.getUTCFullYear() + "-" + (value.getUTCMonth() + 1) + "-" + value.getUTCDate();
				query += " " + value.getUTCHours() + ":" + value.getUTCMinutes() + ":" + value.getUTCSeconds() + ".";
				query += value.getUTCMilliseconds() + "'";
			} else {
				query += "'" + value + "' ";
			}
		});
		
		query += " where ";
		
		var strFilterQuery = "";
		first = true;
		Object.getOwnPropertyNames(filter).forEach(function(a) {
			if(first)
				first = false;
			else
				strFilterQuery += ",";
			
			strFilterQuery += "`" + a + "` = ";
			var value = filter[a];
			if(typeof value == "number") {
				strFilterQuery += value + " ";
			} else if(value instanceof Date) {
				strFilterQuery += "'" + value.getUTCFullYear() + "-" + (value.getUTCMonth() + 1) + "-" + value.getUTCDate();
				strFilterQuery += " " + value.getUTCHours() + ":" + value.getUTCMinutes() + ":" + value.getUTCSeconds() + ".";
				strFilterQuery += value.getUTCMilliseconds() + "'";
			} else {
				strFilterQuery += "'" + value + "' ";
			}
		});
		
		query += strFilterQuery;
		
		query += " and ";
		query += "sha1(concat(" + dataclass.getAttributesNames().join(',') + ")) = '" + checksum + "';";
		query += "select sha1(concat(" + dataclass.getAttributesNames().join(',') + ")) as newVersion ";
		query += "from " + dataclass.name + " ";
		query += "where ";
		query += strFilterQuery;

		var session = this.getSession(dataclass.sourceName);
		var res = session.execute(query);
		var affectedRowCount = res.getAffectedRowCount();
		res = session.getNextResult();
		var row = res.getNextRow();
		return {
			affectedRowCount : affectedRowCount,
			newVersion : row.newVersion
		};
	};


	/*
	** remove
	*/
	this.remove = function(dataclass, filter) {
		var session = this.getSession(dataclass.sourceName);
		session.delete(dataclass.name, filter);
	};

	/*
	** getCount
	*/

	this.getCount = function(dataclass) {
		var session = this.getSession(dataclass.sourceName);
		var count = session.getCount(dataclass.name);
		return count;
	};
};

var ORMVersionManager = function() {
	var entityVersion = {};

	this.getVersion = function(entity) {
		var key = entity.getKey();
		var version = 0;
		if(entityVersion.hasOwnProperty(key)) {
			version = entityVersion[key];
		}

		return version;
	};

	this.updateVersion = function(entity) {
		var key = entity.getKey();
		var version = 0;
		if(entityVersion.hasOwnProperty(key)) {
			version = entityVersion[key];
		}

		entityVersion[key] = version + 1;
	};
};

var HashVersionManager = function() {
	this.getVersion = function(entity) {
		var session = getSessionManager().getSession(entity.sourceName);
		var fields = "sha1(concat(" + entity.getAttributesNames().join(',') + ")) as version";
		var row = session.find(fields, entity.dataclassName, entity.getPrimaryKey());
		var hash = row.version;
		
		return hash;		
	};
};

var TriggerVersionManager = function() {
	this.getVersion = function(entity) {
		/*
		** TODO
		*/
	};

	this.updateVersion = function(entity) {
		/*
		** TODO
		*/
	};
};

var SQLEntityManager = function (inVersionType) {
	/*
	** getVersionManager
	*/
	var versionManager = null;
	var versionType = inVersionType || "orm";

	if(versionManager == null) {
		if(versionType == "orm") {
			versionManager = new ORMVersionManager();
		} else if(versionType == "trigger") {
			versionManager = new TriggerVersionManager();
		} else if(versionType == "hash") {
			versionManager = new HashVersionManager();
		} else {
			throw new Error("Unknown version manager type!");
		}
	}

	this.getVersionType = function() {
		return versionType;
	};

	/*
	** createEntity
	*/
	this.createEntity = function(dataclass, primaryKey) {
		var entity = new SQLEntity(dataclass, primaryKey);
		var version = versionManager.getVersion(entity);
		entity.setVersion(version);

		return entity;
	};

	/*
	** saveEntity
	*/
	this.saveEntity = function(entity) {
		if(versionType == "orm") {
			//if this a retrieved entity
			//check if the version is the same as the one saved in the entity manager
			var kind = entity.getKind();
			if(kind == "retrieved") {
				if(entity.getVersion() != versionManager.getVersion(entity)) {
					throw new Error("This entity has been changed in the server!");
				}
			}

			if(kind == "retrieved") {
				getSessionManager().save(dataclass, entity.getChangedValues(), entity.getPrimaryKey());
			} else {
				getSessionManager().saveNew(dataclass, entity.getValues());
			}

			versionManager.updateVersion(entity);
			var version = versionManager.getVersion(entity);
			entity.setVersion(version);
		} else if(versionType == "hash") {
			//save the entity only if the same version as the server
			var kind = entity.getKind();
			var version = entity.getVersion();
			if(kind == "retrieved") {
				var data = getSessionManager().saveWithChecksumVerification(dataclass, entity.getValues(), entity.getPrimaryKey(), version);
				var affectedRowCount = data.affectedRowCount;
				var newVersion = data.newVersion;
				if(affectedRowCount != 1) {
					throw new Error("This entity has been changed in the server!");
				}
				entity.setVersion(newVersion);
			} else {
				getSessionManager().saveNew(dataclass, entity.getValues());
			}


		} else if(versionType == "trigger") {
			/*
			** TODO
			*/
		} else {
			throw new Error("Unknown version type!");
		}
	};
};

var SQLDataStore = function (name, model, versionType) {
	this.name = name;
	var entityManager = null;
	
	this.getEntityManager = function() {
		if(entityManager == null) {
			entityManager = new SQLEntityManager(versionType);
		}
		
		return entityManager;
	};
	
	var sqlschemas = {};
	var that = this;
	var schemaNames = Object.getOwnPropertyNames(model);
	schemaNames.forEach(function (schemaName) {
		sqlschemas[schemaName] = new SQLSchema(schemaName, model[schemaName], that);
		Object.defineProperty(that, schemaName, {
			enumerable: true,
			configurable: true,
			set: function (value) {
				//no setter
			},
			get: function () {
				return sqlschemas[schemaName];
			}
		});
	});
};

var SQLSchema = function (name, model, datastore) {
	this.sourceName = datastore.name;
	this.name = name;
	var entityManager = datastore.getEntityManager();
	
	this.getEntityManager = function() {
		return entityManager;
	};	

	var dataClasses = {};
	var tableNames = Object.getOwnPropertyNames(model);

	var that = this;

	tableNames.forEach(function (tableName) {
		dataClasses[tableName] = new SQLDataClass(that, tableName, model[tableName]);

		Object.defineProperty(that, tableName, {
			enumerable: true,
			configurable: true,
			set: function (value) {
				//no setter
			},
			get: function () {
				return dataClasses[tableName];
			}
		});
	});
};
var SQLDataClass = function (schema, name, model) {
	this.sourceName = schema.sourceName;
	this.schemaName = schema.name;
	this.name = name;
	this.count = getSessionManager().getCount(this);

	/*
	** getEntityManager
	*/
	var entityManager = schema.getEntityManager();
	
	this.getEntityManager = function() {
		return entityManager;
	}
	
	/*
	** getPrimaryKey
	*/
	this.getPrimaryKey = function () {
		var keys = {};
		Object.getOwnPropertyNames(model).forEach(function (a) {
			if (model[a].isPrimaryKey) {
				keys[a] = null;
			}
		});

		return keys;
	};
	/*
	** getLightAttributesNames
	*/
	this.getLightAttributesNames = function () {
		var attrs = [];
		Object.getOwnPropertyNames(model).forEach(function (a) {
			if (model[a].type !== "blob") {
				attrs.push(a);
			}
		});

		return attrs;
	};
	/*
	** getAttributesNames
	*/
	this.getAttributesNames = function () {
		var attrs = [];
		Object.getOwnPropertyNames(model).forEach(function (a) {
			attrs.push(a);
		});

		return attrs;
	};
	/*
	** find
	*/
	this.find = function(filter) {
		var primaryKey = getSessionManager().getPrimaryKey(this, filter);
		var entity = entityManager.createEntity(this, primaryKey);
		return entity;
	};
	/*
	** query
	*/
	this.query = function(filter) {
		var collection = new SQLEntityCollection(this, filter);

		return collection;
	};
	/*
	** createEntity
	*/
	this.createEntity = function(values) {
		var entity = new SQLEntity(this);

		if (values) {
			Object.getOwnPropertyNames(values).forEach(function(p) {
				entity[p] = values[p];
			});
		}

		return entity;
	};
	/*
	** all function
	*/
	this.all = function() {
		var collection = new SQLEntityCollection(this, "");
		return collection;
	};
	
};
var SQLEntityCollection = function (dataclass, queryPath) {
	//if we want pagination
	//this.count = this._entitieskeys.length;

	this.queryPath = queryPath || '';
	this.queryPlan = '';

	var entitiesKeys = getSessionManager().getPrimaryKeys(dataclass, queryPath);
	this.length = entitiesKeys.length;

	//this.count = getSessionManager().getCount(dataclass);

	var currentEntity = 0;

	var that = this;

	dataclass.getAttributesNames().forEach(function(name) {
		Object.defineProperty(that, name, {
			enumerable: true,
			configurable: true,
			set: function (value) {
			},
			get: function () {
				var res = [];
				var rows = getSessionManager().getAll(dataclass, [name], queryPath);
				rows.forEach(function(row) {
					res.push(row[name]);
				});
				return res;
			}
		});
	});

	/*
	 * count 
	 */
	this.count = function() {
		return this.length;
	};

	/*
	** query
	*/
	this.query = function (filter) {};

	/*
	** first
	*/
	this.first = function() {
		var e = null;

		if(this.length > 0) {
			var key = entitiesKeys[0];
			e = new SQLEntity(dataclass, key);
		}

		return e;
	};
};

var SQLEntity = function (dataclass, primaryKey) {
	var values = {};
	var changedValues = {};
	var gotten = false;
	var kind = "created";
	var saved = false;
	var removed = false;
	var that = this;

	this.sourceName = dataclass.sourceName;
	this.schemaName = dataclass.schemaName;
	this.dataclassName = dataclass.name;
	
	/*
	** getEntityManager
	*/
	var entityManager = dataclass.getEntityManager();
	
	this.getEntityManager = function() {
		return entityManager;
	}
	
	var version;

	if(primaryKey) {
		kind = "retrieved";
	} else {
		primaryKey = dataclass.getPrimaryKey();
		kind = "created";
	}

	dataclass.getAttributesNames().forEach(function(a) {
		Object.defineProperty(that, a, {
			enumerable : true,
			configurable : true,
			set : function(value) {
				changedValues[a] = value;
				values[a] = value;
				saved = false;
			},
			get : function() {
				if(kind == "retrieved") {
					if(!gotten) {
						var that = this;
						var attrs = dataclass.getLightAttributesNames();
						var o = getSessionManager().getFirst(dataclass, attrs, primaryKey);
						
						Object.getOwnPropertyNames(o).forEach(function (a) {
							if(values[a] === undefined) {
								values[a] = o[a];
							}
						});
						
						gotten = true;
					}
					
					if(values[a] === undefined) {
						var o = getSessionManager().getFirst(dataclass, a, pkey);
						values[a] = o[a];
					}
				}
				
				return values[a];
			}
		});
	});
	
	this.getKind = function() {
		return kind;
	};
	
	/*
	** setVersion
	*/
	this.setVersion = function(newVersion) {
		version = newVersion;
	};

	/*
	** setVersion
	*/
	this.getVersion = function() {
		return version;
	};

	/*
	** getKey
	** the key is a uniq reference to be used by the entity manager
	*/
	this.getKey = function() {
		var key = dataclass.schemaName + dataclass.name + JSON.stringify(primaryKey);
		return key;
	};

	/*
	** save
	*/
	this.save = function() {
		if(!saved) {
			entityManager.saveEntity(this);
			changedValues = {};
			saved = true;
		}
	};
	
	/*
	** getValues
	*/
	this.getValues = function() {
		return values;
	};
	
	/*
	** getChangedValues
	*/
	this.getChangedValues = function() {
		return changedValues;
	};
	
	this.getPrimaryKey = function() {
		return primaryKey;
	};

	/*
	** remove
	*/
	this.remove = function() {
		if(!removed) {
			if(kind == "retrieved") {
				getSessionManager().remove(dataclass, primaryKey);
				removed = true;
			}
		}
	};
	
	/*
	** getAttributesNames
	*/
	this.getAttributesNames = function() {
		return dataclass.getAttributesNames();
	};
};

exports.sqlds = (function() {
	var sqlds = {};

	//for all sources in sqlsources folder
	//	params = JSON.parse(strfile);
	//	session = sql.connect(params);
	//	model = sql.createModel(params)
	//	append source name to sqlds
	//
	//

	folder = Folder(solution.getFolder().path + 'sqlsources');

	if (folder.exists) {
		folder.forEachFile(function (file) {
			var params, contents, sourceName;

			params = JSON.parse(file.toString());
			model = sql.createModel(params);
			sourceName = file.nameNoExt;
			getSessionManager().addSession(sourceName, params);
			sqlds[sourceName] = new SQLDataStore(sourceName, model, params.versionType);
		});
	}

	return sqlds;
})();

