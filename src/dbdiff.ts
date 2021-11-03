import _ from 'underscore';
import * as util from 'util';
import { describeDatabase } from './dialects';

type SqlType = {
  sql: string;
  level: LogLevel;
};

export enum LogLevel {
  COMMENT,
  SAFE,
  WARN,
  DROP,
}

export default class DbDiff {
  private sql: SqlType[] = [];
  private selectedDialect: 'postgres' | 'mysql' = 'postgres';
  private currentQuotation: '`' | '"' = '"';
  private quotationOptions = {
    mysql: '`',
    postgres: '"',
  };

  _log(sql: string, level: LogLevel) {
    this.sql.push({ sql, level });
  }

  _drop(sql): void {
    this._log(sql, LogLevel.DROP);
  }

  _warn(sql) {
    this._log(sql, LogLevel.WARN);
  }

  _safe(sql) {
    this._log(sql, LogLevel.SAFE);
  }

  _comment(sql) {
    this._log(sql, LogLevel.COMMENT);
  }

  _quote(name) {
    return `${this.currentQuotation}${name}${this.currentQuotation}`;
  }

  _compareTables(table1, table2) {
    const tableName = this._fullName(table1);

    const columNames1 = this._columnNames(table1);
    const columNames2 = this._columnNames(table2);

    const diff1 = _.difference(columNames1, columNames2);
    const diff2 = _.difference(columNames2, columNames1);

    diff1.forEach(columnName => {
      this._drop(
        `ALTER TABLE ${tableName} DROP COLUMN ${this._quote(columnName)};`
      );
    });

    diff2.forEach(columnName => {
      const col = table2.columns.find(column => column.name === columnName);
      this._safe(
        `ALTER TABLE ${tableName} ADD COLUMN ${this._quote(
          columnName
        )} ${this._columnDescription(col)};`
      );
    });

    const common = _.intersection(columNames1, columNames2);
    common.forEach(columnName => {
      const col1 = table1.columns.find(column => column.name === columnName);
      const col2 = table2.columns.find(column => column.name === columnName);

      if (this.selectedDialect === 'mysql' && !_.isEqual(col1, col2)) {
        const extra = col2.extra ? ` ${col2.extra}` : '';
        const comment =
          col1.type !== col2.type
            ? `-- Previous data type was ${col1.type}\n`
            : '';
        if (
          col1.type !== col2.type ||
          (col1.nullable !== col2.nullable && !col2.nullable)
        ) {
          this._warn(
            `${comment}ALTER TABLE ${tableName} MODIFY ${this._quote(
              columnName
            )} ${this._columnDescription(col2)}${extra};`
          );
        } else {
          this._safe(
            `${comment}ALTER TABLE ${tableName} MODIFY ${this._quote(
              columnName
            )} ${this._columnDescription(col2)}${extra};`
          );
        }
        return;
      }
      if (col1.type !== col2.type) {
        this._warn(`-- Previous data type was ${col1.type}`);
        this._warn(
          `ALTER TABLE ${tableName} ALTER COLUMN ${this._quote(
            columnName
          )} SET DATA TYPE ${col2.type};`
        );
      }
      if (col1.nullable !== col2.nullable) {
        if (col2.nullable) {
          this._safe(
            `ALTER TABLE ${tableName} ALTER COLUMN ${this._quote(
              columnName
            )} DROP NOT NULL;`
          );
        } else {
          this._warn(
            `ALTER TABLE ${tableName} ALTER COLUMN ${this._quote(
              columnName
            )} SET NOT NULL;`
          );
        }
      }
    });
  }

  _createIndex(table, index) {
    const tableName = this._fullName(table);
    const keys = index.columns.map(key => `${this._quote(key)}`).join(',');
    if (this.selectedDialect === 'postgres') {
      this._safe(
        `CREATE INDEX ${this._quote(index.name)} ON ${tableName} USING ${
          index.type
        } (${keys});`
      );
    } else {
      // mysql
      this._safe(
        `CREATE INDEX ${this._quote(index.name)} USING ${
          index.type
        } ON ${tableName} (${keys});`
      );
    }
  }

  _dropIndex(table, index) {
    if (this.selectedDialect === 'postgres') {
      this._safe(`DROP INDEX ${this._fullName(index)};`);
    } else {
      this._safe(
        `DROP INDEX ${this._fullName(index)} ON ${this._fullName(table)};`
      );
    }
  }

  _compareIndexes(table1, table2) {
    const indexNames1 = this._indexNames(table1);
    const indexNames2 = this._indexNames(table2);

    const diff1 = _.difference(indexNames1, indexNames2);
    const diff2 = _.difference(indexNames2, indexNames1);

    if (diff1.length > 0) {
      diff1.forEach(indexName => {
        const index = table1.indexes.find(index => index.name === indexName);
        this._dropIndex(table1, index);
      });
    }
    if (diff2.length > 0) {
      diff2.forEach(indexName => {
        const index = table2.indexes.find(index => index.name === indexName);
        this._createIndex(table2, index);
      });
    }

    const inter = _.intersection(indexNames1, indexNames2);
    inter.forEach(indexName => {
      const index1 = table1.indexes.find(index => index.name === indexName);
      const index2 = table2.indexes.find(index => index.name === indexName);

      if (
        _.difference(index1.columns, index2.columns).length > 0 ||
        index1.primary !== index2.primary ||
        index1.unique !== index2.unique
      ) {
        const index = index2;
        this._comment(`-- Index ${this._fullName(index)} needs to be changed`);
        this._dropIndex(table1, index);
        this._createIndex(table1, index);
      }
    });
  }

  _compareSequences(db1, db2) {
    const sequenceNames1 = this._sequenceNames(db1);
    const sequenceNames2 = this._sequenceNames(db2);

    const diff1 = _.difference(sequenceNames1, sequenceNames2);
    const diff2 = _.difference(sequenceNames2, sequenceNames1);

    diff1.forEach(sequenceName => {
      this._safe(`DROP SEQUENCE ${sequenceName};`);
    });

    diff2.forEach(sequenceName => {
      const sequence = db2.sequences.find(
        sequence => sequenceName === this._fullName(sequence)
      );
      this._safe(this._sequenceDescription(sequence));
    });

    const inter = _.intersection(sequenceNames1, sequenceNames2);
    inter.forEach(sequenceName => {
      const sequence1 = db1.sequences.find(
        sequence => sequenceName === this._fullName(sequence)
      );
      const sequence2 = db2.sequences.find(
        sequence => sequenceName === this._fullName(sequence)
      );

      const desc1 = this._sequenceDescription(sequence1);
      const desc2 = this._sequenceDescription(sequence2);

      if (desc2 !== desc1) {
        this._safe(`DROP SEQUENCE ${sequenceName};`);
        this._safe(desc2);
      }
    });
  }

  _compareConstraints(table1, table2) {
    const tableName = this._fullName(table2);
    table2.constraints.forEach(constraint2 => {
      let constraint1 =
        table1 &&
        table1.constraints.find(cons => constraint2.name === cons.name);
      if (constraint1) {
        if (_.isEqual(constraint1, constraint2)) return;
        if (this.selectedDialect === 'postgres') {
          this._safe(
            `ALTER TABLE ${tableName} DROP CONSTRAINT ${this._quote(
              constraint2.name
            )};`
          );
        } else {
          this._safe(
            `ALTER TABLE ${tableName} DROP INDEX ${this._quote(
              constraint2.name
            )};`
          );
        }
        constraint1 = null;
      }
      if (!constraint1) {
        const keys = constraint2.columns
          .map(s => `${this._quote(s)}`)
          .join(', ');
        const logLevel = table1 ? LogLevel.WARN : LogLevel.SAFE;
        let fullName = this._quote(constraint2.name);
        if (constraint2.type === 'primary') {
          if (this.selectedDialect === 'mysql') fullName = 'foo';
          this._log(
            `ALTER TABLE ${tableName} ADD CONSTRAINT ${fullName} PRIMARY KEY (${keys});`,
            logLevel
          );
        } else if (constraint2.type === 'unique') {
          this._log(
            `ALTER TABLE ${tableName} ADD CONSTRAINT ${fullName} UNIQUE (${keys});`,
            logLevel
          );
        } else if (constraint2.type === 'foreign') {
          const foreignKeys = constraint2.referenced_columns
            .map(s => `${this._quote(s)}`)
            .join(', ');
          this._log(
            `ALTER TABLE ${tableName} ADD CONSTRAINT ${fullName} FOREIGN KEY (${keys}) REFERENCES ${this._quote(
              constraint2.referenced_table
            )} (${foreignKeys});`,
            logLevel
          );
        }
      }
    });
  }

  compareSchemas(db1, db2) {
    this.sql = [];
    this.selectedDialect = db1.dialect;
    this.currentQuotation = this.quotationOptions[db1.dialect];

    this._compareSequences(db1, db2);

    db1.tables.forEach(table => {
      const t = this._findTable(db2, table);
      if (!t) {
        this._drop(`DROP TABLE ${this._fullName(table)};`);
      }
    });

    db2.tables.forEach(table => {
      const t = this._findTable(db1, table);
      const tableName = this._fullName(table);
      if (!t) {
        const columns = table.columns.map(col => {
          let extra = '';
          if (col.extra === 'auto_increment') {
            extra = ' PRIMARY KEY AUTO_INCREMENT';
            const constraint = table.constraints.find(
              constraints => constraints.type === 'primary'
            );
            table.constraints.splice(table.constraints.indexOf(constraint), 1);
          }
          return `\n  ${this._quote(col.name)} ${this._columnDescription(
            col
          )}${extra}`;
        });
        this._safe(`CREATE TABLE ${tableName} (${columns.join(',')}\n);`);

        const indexNames2 = this._indexNames(table);
        indexNames2.forEach(indexName => {
          const index = table.indexes.find(index => index.name === indexName);
          this._createIndex(table, index);
        });
      } else {
        this._compareTables(t, table);
        this._compareIndexes(t, table);
      }
    });

    db2.tables.forEach(table => {
      const t = this._findTable(db1, table);
      this._compareConstraints(t, table);
    });
  }

  compare(conn1, conn2) {
    return Promise.all([describeDatabase(conn1), describeDatabase(conn2)]).then(
      results => {
        const db1 = results[0];
        const db2 = results[1];
        this.compareSchemas(db1, db2);
      }
    );
  }

  _commentOut(sql) {
    return sql
      .split('\n')
      .map(line => (line.substring(0, 2) === '--' ? line : `-- ${line}`))
      .join('\n');
  }

  _columnNames(table) {
    return table.columns.map(col => col.name).sort();
  }

  _columnDescription(col) {
    let desc = col.type;
    if (col.default_value != null) {
      desc += ` DEFAULT ${col.default_value}`;
    }
    desc += col.nullable ? ' NULL' : ' NOT NULL';
    return desc;
  }

  _indexNames(table) {
    return table.indexes.map(index => index.name).sort();
  }

  _isNumber(n) {
    return +n == n; // eslint-disable-line
  }

  _sequenceDescription(sequence: any) {
    return util.format(
      'CREATE SEQUENCE %s INCREMENT %s %s %s %s %s CYCLE;',
      this._fullName(sequence),
      sequence.increment,
      this._isNumber(sequence.minimum_value)
        ? `MINVALUE ${sequence.minimum_value}`
        : 'NO MINVALUE',
      this._isNumber(sequence.maximum_value)
        ? `MAXVALUE ${sequence.maximum_value}`
        : 'NO MAXVALUE',
      this._isNumber(sequence.start_value)
        ? `START ${sequence.start_value}`
        : '',
      sequence.cycle ? '' : 'NO'
    );
  }

  _sequenceNames(db) {
    return db.sequences.map(sequence => this._fullName(sequence));
  }

  _fullName(obj) {
    if (obj.schema)
      return `${this._quote(obj.schema)}.${this._quote(obj.name)}`;
    return this._quote(obj.name);
  }

  _findTable(db, table) {
    return db.tables.find(
      t => t.name === table.name && t.schema === table.schema
    );
  }

  commands(type) {
    let level = 1;
    if (type === 'drop') level = 3;
    else if (type === 'warn') level = 2;
    return this.sql
      .map((sql: any) => {
        return sql.level > level ? this._commentOut(sql.sql) : sql.sql;
      })
      .join('\n\n');
  }
}
