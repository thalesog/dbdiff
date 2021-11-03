import PostgresClient from './postgres-client';

export default class PostgresDialect {
  _unquote(str: string): string {
    if (str.substring(0, 1) === '"' && str.substring(str.length - 1) === '"') {
      return str.substring(1, str.length - 1);
    }
    return str;
  }

  async describeDatabase(
    options: string
  ): Promise<{ dialect: string; sequences: any[]; tables: any[] }> {
    const schema = {
      dialect: 'postgres',
      sequences: [] as any[],
      tables: [] as any[],
    };
    const client = new PostgresClient(options);
    const tables = await client.find(
      'SELECT * FROM pg_tables WHERE schemaname NOT IN ($1, $2, $3)',
      ['temp', 'pg_catalog', 'information_schema']
    );
    schema.tables = await Promise.all(
      tables.map(async (table: any) => {
        const columns = await client.find(
          `
          SELECT
            table_name,
            table_schema,
            column_name,
            data_type,
            udt_name,
            character_maximum_length,
            is_nullable,
            column_default
          FROM
            INFORMATION_SCHEMA.COLUMNS
          WHERE
            table_name=$1 AND table_schema=$2;`,
          [table.tablename, table.schemaname]
        );
        return {
          name: table.tablename,
          schema: table.schemaname,
          indexes: [],
          constraints: [],
          columns: columns.map((column: any) => ({
            name: column.column_name,
            nullable: column.is_nullable === 'YES',
            default_value: column.column_default,
            type: dataType(column),
          })),
        };
      })
    );
    const indexes = await client.find(
      `
          SELECT
            i.relname as indname,
            i.relowner as indowner,
            idx.indrelid::regclass,
            idx.indisprimary,
            idx.indisunique,
            am.amname as indam,
            idx.indkey,
            ARRAY(
              SELECT pg_get_indexdef(idx.indexrelid, k + 1, true)
              FROM generate_subscripts(idx.indkey, 1) as k
              ORDER BY k
            ) AS indkey_names,
            idx.indexprs IS NOT NULL as indexprs,
            idx.indpred IS NOT NULL as indpred,
            ns.nspname
          FROM
            pg_index as idx
          JOIN pg_class as i
            ON i.oid = idx.indexrelid
          JOIN pg_am as am
            ON i.relam = am.oid
          JOIN pg_namespace as ns
            ON ns.oid = i.relnamespace
            AND ns.nspname NOT IN ('pg_catalog', 'pg_toast')
          WHERE (NOT idx.indisprimary) AND (NOT idx.indisunique);
        `,
      []
    );
    indexes.forEach((index: any) => {
      const tableName = index.indrelid.split('.').pop();
      const tables = schema.tables.find(
        table => table.name === tableName && table.schema === index.nspname
      );
      tables.indexes.push({
        name: index.indname,
        schema: tables.schema,
        type: index.indam,
        columns: index.indkey_names,
      });
    });
    const constraints = await client.find(
      `
          SELECT conrelid::regclass AS table_from, n.nspname, contype, conname, pg_get_constraintdef(c.oid) AS description
          FROM   pg_constraint c
          JOIN   pg_namespace n ON n.oid = c.connamespace
          WHERE  contype IN ('f', 'p', 'u')
          ORDER  BY conrelid::regclass::text, contype DESC;
        `,
      []
    );
    const types = {
      u: 'unique',
      f: 'foreign',
      p: 'primary',
    };
    constraints.forEach((constraint: any) => {
      const tableFrom = this._unquote(constraint.table_from).split('.').pop();
      const tables = schema.tables.find(
        table => table.name === tableFrom && table.schema === constraint.nspname
      );
      if (!tables) return;
      const { description } = constraint as { description: string };
      let i = description.indexOf('(');
      let n = description.indexOf(')');
      const m = description.indexOf('REFERENCES');
      const info = {
        name: constraint.conname,
        schema: tables.schema,
        type: types[constraint.contype],
        columns: description
          .substring(i + 1, n)
          .split(',')
          .map(s => this._unquote(s.trim())),
        referenced_table: '',
        referenced_columns: [] as any[],
      };
      tables.constraints.push(info);
      if (m > 0) {
        const substr = description.substring(m + 'REFERENCES'.length);
        i = substr.indexOf('(');
        n = substr.indexOf(')');
        info.referenced_table = substr.substring(0, i).trim();
        info.referenced_columns = substr
          .substring(i + 1, n)
          .split(',')
          .map(s_1 => this._unquote(s_1.trim()));
      }
    });
    const sequences: any = await client.find(
      'SELECT * FROM information_schema.sequences',
      []
    );
    schema.sequences = sequences.map(sequence => {
      sequence.schema = sequence.sequence_schema;
      sequence.name = sequence.sequence_name;
      sequence.cycle = sequence.cycle_option === 'YES';
      delete sequence.sequence_name;
      delete sequence.sequence_catalog;
      delete sequence.sequence_schema;
      delete sequence.cycle_option;
      return sequence;
    });
    return schema;
  }
}

function dataType(info: {
  data_type: string;
  udt_name: any;
  character_maximum_length: any;
}): string {
  let type: string;
  if (info.data_type === 'ARRAY') {
    type = info.udt_name;
    if (type.substring(0, 1) === '_') {
      type = type.substring(1);
    }
    type += '[]';
  } else if (info.data_type === 'USER-DEFINED') {
    type = info.udt_name; // hstore for example
  } else {
    type = info.data_type;
  }

  if (info.character_maximum_length) {
    type = `${type} (${info.character_maximum_length})`;
  }
  return type;
}
