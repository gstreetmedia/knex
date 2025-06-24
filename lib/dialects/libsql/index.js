// better-sqlite3 Client
// -------
const Client_SQLite3 = require('../sqlite3');
const Transaction = require('./execution/libsql-transaction');

class Client_LibSQL extends Client_SQLite3 {
  _driver() {
    let driver = require('@libsql/client');
    return driver.createClient;
  }

  // Get a raw connection from the database, returning a promise with the connection object.
  async acquireRawConnection() {
    const options = this.connectionSettings.options || {};

    return new this.driver({
      file: this.connectionSettings.filename,
      syncUrl: options.url || options.syncUrl,
      authToken: options.authToken,
      encryptionKey: options.encryptionKey,
      syncInterval: options.syncInterval,
      concurrency: options.concurrency || 20,
      readonly: !!options.readonly,
    });
  }

  // Used to explicitly close a connection, called internally by the pool when
  // a connection times out or the pool is shutdown.
  async destroyRawConnection(connection) {
    return connection.close();
  }

  /**
   * Runs the query on the specified connection, providing the bindings and any
   * other necessary prep work.
   * @param connection
   * @param obj
   * @returns {Promise<{sql}|*>}
   * @private
   */
  async _query(connection, obj) {
    if (!obj.sql) throw new Error('The query is empty');

    if (!connection) {
      throw new Error('No connection provided');
    }

    const { method } = obj;
    let callMethod;
    switch (method) {
      case 'insert':
      case 'update':
        callMethod = obj.returning ? 'execute' : 'execute';
        break;
      case 'counter':
      case 'del':
        callMethod = 'execute';
        break;
      default:
        callMethod = 'execute';
    }

    /**
     * @type {ResultSet}
     */
    const response = await connection[callMethod](
      {
        sql: obj.sql,
        args: obj.bindings,
      },
    );

    obj.response = response;

    obj.context = {
      lastID: response.lastInsertRowid,
      changes: response.rowsAffected,
    };

    return obj;
  }

  transaction() {
    return new Transaction(this, ...arguments);
  }

}

Object.assign(Client_LibSQL.prototype, {
  // The "dialect", for reference .
  driverName: '@libsql/client',
});

module.exports = Client_LibSQL;
