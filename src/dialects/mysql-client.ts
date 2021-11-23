import { createConnection, Connection, RowDataPacket } from 'mysql2/promise';
import parseDbUrl from 'parse-database-url';

export default class MysqlClient {
  private connection?: Connection;
  public database: string;
  private dbUrl: string;

  constructor(dbUrl: string) {
    const options = parseDbUrl(dbUrl);
    this.dbUrl = dbUrl;
    this.database = options.database;
  }

  async getConnection() {
    return !this.connection
      ? await createConnection(this.dbUrl)
      : this.connection;
  }

  async dropTables() {
    const connection = await this.getConnection();

    const results = await this.find(
      `
      SELECT concat('DROP TABLE IF EXISTS ', table_name, ';') AS fullSQL
      FROM information_schema.tables
      WHERE table_schema = ?;
    `,
      [this.database]
    );
    if (!results) throw new Error('Table list not found');
    const sqlResults = results.map(result => result.fullSQL).join(' ');
    return sqlResults && connection.query(sqlResults);
  }

  async find(sql: string, params: string[] = []) {
    const connection = await this.getConnection();

    const query = await connection.query(sql, params);

    if (!query[0]) throw new Error('Query failed');

    return query[0] as RowDataPacket[];
  }

  async findOne(sql, params = []) {
    const connection = await this.getConnection();
    const query = await connection.query(sql, params);

    return query[0][0];
  }
}
