// better-sqlite3 Client
// -------
const Client_SQLite3 = require('../sqlite3');

class Client_LibSQL extends Client_SQLite3 {
  _driver() {
    let driver = require('@libsql/client')
    return driver.createClient
  }

  // Get a raw connection from the database, returning a promise with the connection object.
  async acquireRawConnection() {
    const options = this.connectionSettings.options || {};

    return new this.driver(this.connectionSettings.filename, {
      file: this.connectionSettings.filename,
      url : this.connectionSettings.url,
      authToken: this.connectionSettings.authToken,
      nativeBinding: options.nativeBinding,
      readonly: !!options.readonly,
    });
  }

  // Used to explicitly close a connection, called internally by the pool when
  // a connection times out or the pool is shutdown.
  async destroyRawConnection(connection) {
    return connection.close();
  }

  // Runs the query on the specified connection, providing the bindings and any
  // other necessary prep work.
  async _query(connection, obj) {
    if (!obj.sql) throw new Error('The query is empty');

    if (!connection) {
      throw new Error('No connection provided');
    }

    //const statement = connection.prepare(obj.sql);
    //const bindings = this._formatBindings(obj.bindings);

    if (obj.sql.toLowerCase().indexOf("select")) {
      const response = await connection.execute(
        {
          sql : obj.sql,
          args : obj.bindings
        }
      );
      obj.response = response;
      return obj;
    }

    const response = await connection.execute(
      {
        sql : obj.sql,
        args : obj.bindings
      }
    );
    obj.response = response;
    obj.context = {
      lastID: response.lastInsertRowid,
      changes: response.changes,
    };

    return obj;
  }

  _formatBindings(bindings) {
    if (!bindings) {
      return [];
    }
    return bindings.map((binding) => {
      if (binding instanceof Date) {
        return binding.valueOf();
      }

      if (typeof binding === 'boolean') {
        return Number(binding);
      }

      return binding;
    });
  }
}

Object.assign(Client_LibSQL.prototype, {
  // The "dialect", for reference .
  driverName: '@libsql/client',
});

module.exports = Client_LibSQL;
