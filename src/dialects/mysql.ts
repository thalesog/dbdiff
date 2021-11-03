import { MysqlClient } from './mysql-client';

export class MySQLDialect {
  _quote(str: any) {
    return `\`${str}\``;
  }

  async describeDatabase(options) {
    const schema = { dialect: 'mysql', sequences: [], tables: [] };
    const client = new MysqlClient(options);
    const [rows, fields] = await client.connection.query('SHOW TABLES');
    const field = fields[0].name;
    const tables = (rows as any).map(row => row[field]);
    const tables_1 = tables.map(table => {
      const t = {
        name: table,
        constraints: [],
        indexes: [],
        columns: [],
      };
      return client
        .find(`DESCRIBE ${this._quote(table)}`)
        .then((columns: any) => {
          t.columns = columns.map(column => ({
            name: column.Field,
            nullable: column.Null === 'YES',
            default_value: column.Default,
            type: column.Type,
            extra: column.Extra,
          }));
          return t;
        });
    });
    schema.tables = tables_1;
    const constraints: any = await client.find(
      'SELECT * FROM information_schema.KEY_COLUMN_USAGE WHERE TABLE_SCHEMA=?',
      [client.database]
    );
    constraints.forEach(constraint => {
      const name = constraint['CONSTRAINT_NAME'];
      const table_1: any = schema.tables.find(
        (table_2: any) => table_2.name === constraint['TABLE_NAME']
      );
      let info = table_1.constraints.find(constr => constr.name === name);
      const foreign = !!constraint['REFERENCED_TABLE_NAME'];
      if (!info) {
        info = {
          name,
          type: foreign ? 'foreign' : name === 'PRIMARY' ? 'primary' : 'unique',
          columns: [],
        };
        if (foreign) info.referenced_columns = [];
        table_1.constraints.push(info);
      }
      if (foreign) {
        info.referenced_table = constraint['REFERENCED_TABLE_NAME'];
        info.referenced_columns.push(constraint['REFERENCED_COLUMN_NAME']);
      }
      info.columns.push(constraint['COLUMN_NAME']);
    });
    schema.tables.map((table_3: any) =>
      client
        .find(`SHOW INDEXES IN ${this._quote(table_3.name)}`)
        .then((indexes: any) => {
          indexes
            .filter(
              index =>
                !table_3.constraints.find(
                  constraint_1 => constraint_1.name === index.Key_name
                )
            )
            .forEach(index_1 => {
              let info_1 = table_3.indexes.find(
                indx => index_1.Key_name === indx.name
              );
              if (!info_1) {
                info_1 = {
                  name: index_1.Key_name,
                  type: index_1.Index_type,
                  columns: [],
                };
                table_3.indexes.push(info_1);
              }
              info_1.columns.push(index_1.Column_name);
            });
        })
    );
    return schema;
  }
}
