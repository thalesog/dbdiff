/* eslint-disable */
// @ts-nocheck

import url from 'url';
import PostgresDialect from './postgres';
import MysqlDialect from './mysql';

const dialects = {
  postgres: PostgresDialect,
  mysql: MysqlDialect,
};

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
  const clazz = dialects[dialect];
  if (!clazz) {
    throw new Error(`No implementation found for dialect ${dialect}`);
  }
  const obj = new (Function.prototype.bind.apply(clazz, [options]))();
  return obj.describeDatabase(options);
}
