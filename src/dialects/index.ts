import url from 'url';
import PostgresDialect from './postgres';
import MysqlDialect from './mysql';

export async function describeDatabase(options) {
  let dialect = options.dialect;
  if (!dialect) {
    if (typeof options === 'string') {
      const info = new url.URL(options);
      dialect = info.protocol;
      if (dialect && dialect.length > 1) {
        dialect = info.protocol.substring(0, info.protocol.length - 1);
      }
    }
    if (!dialect) {
      throw new Error(`Dialect not found for options ${options}`);
    }
  }

  switch (dialect) {
    case 'mysql':
      return new MysqlDialect().describeDatabase(options);
    case 'postgres':
      return new PostgresDialect().describeDatabase(options);
    default:
      throw new Error(`No implementation found for dialect ${dialect}`);
  }
}
