import { createConnection, Connection } from 'mysql2/promise';
const connections = {};
import parseDbUrl from 'parse-database-url';

type MysqlConnOptions = {
  driver: 'mysql';
  user: string;
  password: string;
  host: string;
  database: string;
};
export class MysqlClient {
  private connection: Connection;
  private options: MysqlConnOptions;
  private database: string;

  constructor(dbUrl: string) {
    const options = parseDbUrl(dbUrl);
    this.options = options;
    this.database = options.database;

    const key = `${options.username}:${options.password}@${options.host}:${options.port}/${options.database}`;
    let conn = connections[key];
    if (!conn) {
      conn = connections[key] = createConnection(dbUrl);
    }
    this.connection = conn;
  }

  dropTables() {
    return this.find(
      `
      SELECT concat('DROP TABLE IF EXISTS ', table_name, ';') AS fullSQL
      FROM information_schema.tables
      WHERE table_schema = ?;
    `,
      [this.options.database]
    ).then((results: any) => {
      const sql = results.map(result => result.fullSQL).join(' ');
      return sql && this.connection.query(sql);
    });
  }

  async find(sql, params: string[] = []) {
    const query = await this.connection.query(sql, params);

    return query[0];
  }

  async findOne(sql, params = []) {
    const query = await this.connection.query(sql, params);

    return query[0][0];
  }
}
