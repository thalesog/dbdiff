const pync = require('pync');
const MysqlClient = require('./mysql-client');

export class MySQLDialect {
  _quote(str) {
    return `\`${str}\``;
  }

  describeDatabase(options) {
    const schema = { dialect: 'mysql', sequences: [], tables: [] };
    const client = new MysqlClient(options);
    return client
      .query('SHOW TABLES')
      .then(result => {
        const field = result.fields[0].name;
        const rows = result.rows;
        const tables = rows.map(row => row[field]);

        return pync.map(tables, table => {
          const t = {
            name: table,
            constraints: [],
            indexes: [],
            columns: [],
          };
          return client.find(`DESCRIBE ${this._quote(table)}`).then(columns => {
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
      })
      .then(tables => {
        schema.tables = tables;
        return client.find(
          'SELECT * FROM information_schema.KEY_COLUMN_USAGE WHERE TABLE_SCHEMA=?',
          [client.database]
        );
      })
      .then(constraints => {
        constraints.forEach(constraint => {
          const name = constraint['CONSTRAINT_NAME'];
          const table: any = schema.tables.find(
            (table: any) => table.name === constraint['TABLE_NAME']
          );
          let info = table.constraints.find(constr => constr.name === name);
          const foreign = !!constraint['REFERENCED_TABLE_NAME'];
          if (!info) {
            info = {
              name,
              type: foreign
                ? 'foreign'
                : name === 'PRIMARY'
                ? 'primary'
                : 'unique',
              columns: [],
            };
            if (foreign) info.referenced_columns = [];
            table.constraints.push(info);
          }
          if (foreign) {
            info.referenced_table = constraint['REFERENCED_TABLE_NAME'];
            info.referenced_columns.push(constraint['REFERENCED_COLUMN_NAME']);
          }
          info.columns.push(constraint['COLUMN_NAME']);
        });
        return pync.series(schema.tables, table =>
          client
            .find(`SHOW INDEXES IN ${this._quote(table.name)}`)
            .then(indexes => {
              indexes
                .filter(
                  index =>
                    !table.constraints.find(
                      constraint => constraint.name === index.Key_name
                    )
                )
                .forEach(index => {
                  let info = table.indexes.find(
                    indx => index.Key_name === indx.name
                  );
                  if (!info) {
                    info = {
                      name: index.Key_name,
                      type: index.Index_type,
                      columns: [],
                    };
                    table.indexes.push(info);
                  }
                  info.columns.push(index.Column_name);
                });
            })
        );
      })
      .then(() => schema);
  }
}
