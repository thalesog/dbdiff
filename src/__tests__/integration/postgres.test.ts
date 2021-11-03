import { runCLI } from '../helpers';
import PostgresClient from '../../dialects/postgres-client';

const conString1 = 'postgres://postgres:dolby1212@localhost/dbdiff-test-db1';
const conString2 = 'postgres://postgres:dolby1212@localhost/dbdiff-test-db2';

describe('Postgresql', () => {
  const levelArray = ['safe', 'warn', 'drop'];
  const db1Client = new PostgresClient(conString1);
  const db2Client = new PostgresClient(conString2);

  async function runCommands(commands1, commands2) {
    for await (const command of commands1) {
      await db1Client.query(command, []);
    }

    for await (const command of commands2) {
      await db2Client.query(command, []);
    }
  }

  beforeAll(() => {
    // void db1Client.query('CREATE DATABASE "dbdiff-test-db1";');
    // void db2Client.query('CREATE DATABASE "dbdiff-test-db2";');
  });

  beforeEach(async () => {
    await db1Client.dropTables();
    await db2Client.dropTables();
  });

  it('should create a table', async () => {
    const commands1 = [];
    const commands2 = [
      'CREATE TABLE users (email VARCHAR(255), tags varchar(255)[])',
    ];
    await runCommands(commands1, commands2);
    for await (const level of levelArray) {
      const { stdout } = runCLI(process.cwd(), [
        '-s',
        conString1,
        '-d',
        conString2,
        '-l',
        String(level),
      ]);
      expect(stdout).toContain('CREATE TABLE "public"."users" (');
      expect(stdout).toContain('  "email" character varying (255) NULL,');
      expect(stdout).toContain('  "tags" varchar[] NULL');
      expect(stdout).toContain(');');
    }
  });

  it('should drop a table', async () => {
    const commands1 = ['CREATE TABLE users (email VARCHAR(255))'];
    const commands2 = [];
    await runCommands(commands1, commands2);
    for await (const level of levelArray) {
      const { stdout } = runCLI(process.cwd(), [
        '-s',
        conString1,
        '-d',
        conString2,
        '-l',
        String(level),
      ]);
      if (level === 'drop') {
        expect(stdout).toContain('DROP TABLE "public"."users";');
      } else {
        expect(stdout).toContain('-- DROP TABLE "public"."users";');
      }
    }
  });

  it('should create a table wih a serial sequence', async () => {
    const commands1 = [];
    const commands2 = ['CREATE TABLE users (id serial)'];
    await runCommands(commands1, commands2);
    for await (const level of levelArray) {
      const { stdout } = runCLI(process.cwd(), [
        '-s',
        conString1,
        '-d',
        conString2,
        '-l',
        String(level),
      ]);
      expect(stdout).toContain(
        'CREATE SEQUENCE "public"."users_id_seq" INCREMENT 1 MINVALUE 1 MAXVALUE 2147483647 START 1 NO CYCLE;'
      );
      expect(stdout).toContain('CREATE TABLE "public"."users" (');
      expect(stdout).toContain(
        '  "id" integer DEFAULT nextval(\'users_id_seq\'::regclass) NOT NULL'
      );
      expect(stdout).toContain(');');
    }
  });

  it('should add a column to a table', async () => {
    const commands1 = ['CREATE TABLE users (email VARCHAR(255))'];
    const commands2 = [
      'CREATE TABLE users (email VARCHAR(255))',
      'ALTER TABLE users ADD COLUMN first_name VARCHAR(255)',
    ];
    await runCommands(commands1, commands2);
    for await (const level of levelArray) {
      const { stdout } = runCLI(process.cwd(), [
        '-s',
        conString1,
        '-d',
        conString2,
        '-l',
        String(level),
      ]);

      expect(stdout).toContain(
        'ALTER TABLE "public"."users" ADD COLUMN "first_name" character varying (255) NULL;'
      );
    }
  });

  it('should drop a column from a table', async () => {
    const commands1 = [
      'CREATE TABLE users (email VARCHAR(255))',
      'ALTER TABLE users ADD COLUMN first_name VARCHAR(255)',
    ];
    const commands2 = ['CREATE TABLE users (email VARCHAR(255))'];

    await runCommands(commands1, commands2);
    for await (const level of levelArray) {
      const { stdout } = runCLI(process.cwd(), [
        '-s',
        conString1,
        '-d',
        conString2,
        '-l',
        String(level),
      ]);
      if (level === 'drop') {
        expect(stdout).toContain(
          'ALTER TABLE "public"."users" DROP COLUMN "first_name";'
        );
      } else {
        expect(stdout).toContain(
          '-- ALTER TABLE "public"."users" DROP COLUMN "first_name";'
        );
      }
    }
  });

  it('should change the type of a column', async () => {
    const commands1 = [
      'CREATE TABLE users (email VARCHAR(255))',
      'ALTER TABLE users ADD COLUMN first_name VARCHAR(200)',
    ];
    const commands2 = [
      'CREATE TABLE users (email VARCHAR(255))',
      'ALTER TABLE users ADD COLUMN first_name VARCHAR(255)',
    ];
    await runCommands(commands1, commands2);
    for await (const level of levelArray) {
      const { stdout } = runCLI(process.cwd(), [
        '-s',
        conString1,
        '-d',
        conString2,
        '-l',
        String(level),
      ]);
      expect(stdout).toContain(
        '-- Previous data type was character varying (200)'
      );
      if (['drop', 'warn'].includes(level)) {
        expect(stdout).toContain(
          'ALTER TABLE "public"."users" ALTER COLUMN "first_name" SET DATA TYPE character varying (255);'
        );
      } else {
        expect(stdout).toContain(
          '-- ALTER TABLE "public"."users" ALTER COLUMN "first_name" SET DATA TYPE character varying (255);'
        );
      }
    }
  });

  it('should change a column to not nullable', async () => {
    const commands1 = [
      'CREATE TABLE users (email VARCHAR(255))',
      'ALTER TABLE users ADD COLUMN first_name VARCHAR(255)',
    ];
    const commands2 = [
      'CREATE TABLE users (email VARCHAR(255))',
      'ALTER TABLE users ADD COLUMN first_name VARCHAR(255) NOT NULL',
    ];
    await runCommands(commands1, commands2);
    for await (const level of levelArray) {
      const { stdout } = runCLI(process.cwd(), [
        '-s',
        conString1,
        '-d',
        conString2,
        '-l',
        String(level),
      ]);
      if (['drop', 'warn'].includes(level)) {
        expect(stdout).toContain(
          'ALTER TABLE "public"."users" ALTER COLUMN "first_name" SET NOT NULL;'
        );
      } else {
        expect(stdout).toContain(
          '-- ALTER TABLE "public"."users" ALTER COLUMN "first_name" SET NOT NULL;'
        );
      }
    }
  });

  it('should change a column to nullable', async () => {
    const commands1 = [
      'CREATE TABLE users (email VARCHAR(255))',
      'ALTER TABLE users ADD COLUMN first_name VARCHAR(255) NOT NULL',
    ];
    const commands2 = [
      'CREATE TABLE users (email VARCHAR(255))',
      'ALTER TABLE users ADD COLUMN first_name VARCHAR(255)',
    ];
    const expected =
      'ALTER TABLE "public"."users" ALTER COLUMN "first_name" DROP NOT NULL;';
    await runCommands(commands1, commands2);
    for await (const level of levelArray) {
      const { stdout } = runCLI(process.cwd(), [
        '-s',
        conString1,
        '-d',
        conString2,
        '-l',
        String(level),
      ]);

      expect(stdout).toContain(expected);
    }
  });

  it('should create a sequence', async () => {
    const commands1 = [];
    const commands2 = ['CREATE SEQUENCE seq_name'];
    const expected =
      'CREATE SEQUENCE "public"."seq_name" INCREMENT 1 MINVALUE 1 MAXVALUE 9223372036854775807 START 1 NO CYCLE;';
    await runCommands(commands1, commands2);
    for await (const level of levelArray) {
      const { stdout } = runCLI(process.cwd(), [
        '-s',
        conString1,
        '-d',
        conString2,
        '-l',
        String(level),
      ]);

      expect(stdout).toContain(expected);
    }
  });

  it('should drop a sequence', async () => {
    const commands1 = ['CREATE SEQUENCE seq_name'];
    const commands2 = [];
    const expected = 'DROP SEQUENCE "public"."seq_name";';
    await runCommands(commands1, commands2);
    for await (const level of levelArray) {
      const { stdout } = runCLI(process.cwd(), [
        '-s',
        conString1,
        '-d',
        conString2,
        '-l',
        String(level),
      ]);

      expect(stdout).toContain(expected);
    }
  });

  // TODO: update a sequence

  it('should create an index', async () => {
    const commands1 = [
      'CREATE TABLE users (email VARCHAR(255))',
      'ALTER TABLE users ADD COLUMN first_name VARCHAR(255)',
    ];
    const commands2 = [
      'CREATE TABLE users (email VARCHAR(255))',
      'ALTER TABLE users ADD COLUMN first_name VARCHAR(255)',
      'CREATE INDEX users_email ON "users" (email)',
    ];
    const expected =
      'CREATE INDEX "users_email" ON "public"."users" USING btree ("email");';
    await runCommands(commands1, commands2);
    for await (const level of levelArray) {
      const { stdout } = runCLI(process.cwd(), [
        '-s',
        conString1,
        '-d',
        conString2,
        '-l',
        String(level),
      ]);

      expect(stdout).toContain(expected);
    }
  });

  it('should drop an index', async () => {
    const commands1 = [
      'CREATE TABLE users (email VARCHAR(255))',
      'ALTER TABLE users ADD COLUMN first_name VARCHAR(255)',
      'CREATE INDEX users_email ON users (email)',
    ];
    const commands2 = [
      'CREATE TABLE users (email VARCHAR(255))',
      'ALTER TABLE users ADD COLUMN first_name VARCHAR(255)',
    ];
    const expected = 'DROP INDEX "public"."users_email";';
    await runCommands(commands1, commands2);
    for await (const level of levelArray) {
      const { stdout } = runCLI(process.cwd(), [
        '-s',
        conString1,
        '-d',
        conString2,
        '-l',
        String(level),
      ]);

      expect(stdout).toContain(expected);
    }
  });

  it('should recreate an index', async () => {
    const commands1 = [
      'CREATE TABLE users (email VARCHAR(255))',
      'ALTER TABLE users ADD COLUMN first_name VARCHAR(255)',
      'ALTER TABLE users ADD COLUMN last_name VARCHAR(255)',
      'CREATE INDEX some_index ON "users" (first_name)',
    ];
    const commands2 = [
      'CREATE TABLE users (email VARCHAR(255))',
      'ALTER TABLE users ADD COLUMN first_name VARCHAR(255)',
      'ALTER TABLE users ADD COLUMN last_name VARCHAR(255)',
      'CREATE INDEX some_index ON "users" (last_name)',
    ];

    await runCommands(commands1, commands2);
    for await (const level of levelArray) {
      const { stdout } = runCLI(process.cwd(), [
        '-s',
        conString1,
        '-d',
        conString2,
        '-l',
        String(level),
      ]);

      expect(stdout).toContain(
        '-- Index "public"."some_index" needs to be changed'
      );
      expect(stdout).toContain('DROP INDEX "public"."some_index";');
      expect(stdout).toContain(
        'CREATE INDEX "some_index" ON "public"."users" USING btree ("last_name");'
      );
    }
  });

  it('should create a table with an index', async () => {
    const commands1 = [];
    const commands2 = [
      'CREATE TABLE users (email VARCHAR(255))',
      'CREATE INDEX users_email ON users (email)',
    ];

    await runCommands(commands1, commands2);
    for await (const level of levelArray) {
      const { stdout } = runCLI(process.cwd(), [
        '-s',
        conString1,
        '-d',
        conString2,
        '-l',
        String(level),
      ]);

      expect(stdout).toContain('CREATE TABLE "public"."users" (');
      expect(stdout).toContain('  "email" character varying (255) NULL');
      expect(stdout).toContain(');');
      expect(stdout).toContain(
        'CREATE INDEX "users_email" ON "public"."users" USING btree ("email");'
      );
    }
  });

  it('should support all constraint types', async () => {
    const commands1 = [];
    const commands2 = [
      'CREATE TABLE users (id serial, email VARCHAR(255));',
      'CREATE TABLE items (id serial, name VARCHAR(255), user_id bigint);',
      'ALTER TABLE users ADD CONSTRAINT users_pk PRIMARY KEY (id);',
      'ALTER TABLE users ADD CONSTRAINT email_unique UNIQUE (email);',
      'ALTER TABLE items ADD CONSTRAINT items_fk FOREIGN KEY (user_id) REFERENCES users (id);',
    ];

    await runCommands(commands1, commands2);
    for await (const level of levelArray) {
      const { stdout } = runCLI(process.cwd(), [
        '-s',
        conString1,
        '-d',
        conString2,
        '-l',
        String(level),
      ]);

      expect(stdout).toContain(
        'CREATE SEQUENCE "public"."users_id_seq" INCREMENT 1 MINVALUE 1 MAXVALUE 2147483647 START 1 NO CYCLE;'
      );

      expect(stdout).toContain(
        'CREATE SEQUENCE "public"."items_id_seq" INCREMENT 1 MINVALUE 1 MAXVALUE 2147483647 START 1 NO CYCLE;'
      );

      expect(stdout).toContain('CREATE TABLE "public"."items" (');
      expect(stdout).toContain(
        '  "id" integer DEFAULT nextval(\'items_id_seq\'::regclass) NOT NULL,'
      );
      expect(stdout).toContain('  "name" character varying (255) NULL,');
      expect(stdout).toContain('  "user_id" bigint NULL');
      expect(stdout).toContain(');');
      expect(stdout).toContain('CREATE TABLE "public"."users" (');
      expect(stdout).toContain(
        '  "id" integer DEFAULT nextval(\'users_id_seq\'::regclass) NOT NULL,'
      );
      expect(stdout).toContain('  "email" character varying (255) NULL');
      expect(stdout).toContain(');');
      expect(stdout).toContain(
        'ALTER TABLE "public"."items" ADD CONSTRAINT "items_fk" FOREIGN KEY ("user_id") REFERENCES "users" ("id");'
      );
      expect(stdout).toContain(
        'ALTER TABLE "public"."users" ADD CONSTRAINT "email_unique" UNIQUE ("email");'
      );
      expect(stdout).toContain(
        'ALTER TABLE "public"."users" ADD CONSTRAINT "users_pk" PRIMARY KEY ("id");'
      );
    }
  });

  it('should support existing constriants with the same name', async () => {
    const commands1 = [
      'CREATE TABLE users (email VARCHAR(255), api_key VARCHAR(255));',
      'ALTER TABLE users ADD CONSTRAINT a_unique_constraint UNIQUE (email);',
    ];
    const commands2 = [
      'CREATE TABLE users (email VARCHAR(255), api_key VARCHAR(255));',
      'ALTER TABLE users ADD CONSTRAINT a_unique_constraint UNIQUE (api_key);',
    ];

    await runCommands(commands1, commands2);
    for await (const level of levelArray) {
      const { stdout } = runCLI(process.cwd(), [
        '-s',
        conString1,
        '-d',
        conString2,
        '-l',
        String(level),
      ]);

      if (['warn', 'drop'].includes(level)) {
        expect(stdout).toContain(
          'ALTER TABLE "public"."users" DROP CONSTRAINT "a_unique_constraint";'
        );
        expect(stdout).toContain(
          'ALTER TABLE "public"."users" ADD CONSTRAINT "a_unique_constraint" UNIQUE ("api_key");'
        );
      } else {
        expect(stdout).toContain(
          'ALTER TABLE "public"."users" DROP CONSTRAINT "a_unique_constraint";'
        );
        expect(stdout).toContain(
          '-- ALTER TABLE "public"."users" ADD CONSTRAINT "a_unique_constraint" UNIQUE ("api_key");'
        );
      }
    }
  });
});
