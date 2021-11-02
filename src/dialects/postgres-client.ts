import { Pool, PoolConfig } from 'pg';
import parseDbUrl from 'parse-database-url';

const DROP_SCHEMA_QUERY = 'drop schema public cascade; create schema public;';

export default class PostgresClient {
  private pool: Pool | undefined;
  private connUrl: string;
  private connOptions: PoolConfig;

  constructor(conUrl: string) {
    this.connUrl = conUrl;
    this.connOptions = parseDbUrl(this.connUrl);
  }

  buildPool() {
    return new Pool(this.connOptions);
  }

  dropTables() {
    return this.query(DROP_SCHEMA_QUERY);
  }

  async connect() {
    if (!this.pool) {
      this.pool = this.buildPool();
    }
    const pgClient = await this.pool.connect();

    return pgClient;
  }

  async query(sql, params = []) {
    const pgClient = await this.connect();
    const resultSet = await pgClient.query(sql, params);
    const shouldDestroy = true;
    pgClient.release(shouldDestroy);
    return resultSet;
  }

  find(sql, params) {
    return this.query(sql, params).then(result => result.rows);
  }
  findOne(sql, params) {
    return this.query(sql, params).then(result => result.rows[0]);
  }
}
