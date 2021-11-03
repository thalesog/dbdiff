import { Command, Option } from 'commander';
import parseDbUrl from 'parse-database-url';
import DbDiff from './dbdiff';
const packageJson = require('../package.json');
const version: string = packageJson.version;

const program = new Command();

program
  .version(version)
  .name('dbdiff')
  .requiredOption('-s, --source [dbUrl]', 'source database url')
  .requiredOption('-d, --destination [dbUrl]', 'destination database url')
  .addOption(
    new Option('-l, --level <level>', 'chooses the safety of the sql')
      .choices(['safe', 'warn', 'drop'])
      .default('safe')
  )
  .parse(process.argv);

const { source, destination, level } = program.opts();

const dbdiff = new DbDiff();

void (async () => {
  console.log(`
✅ Starting database comparison 🚀
----------------------------------
➡️ Level => ${level}
----------------------------------
➡️ Source
${JSON.stringify(parseDbUrl(source), null, 2)}
➡️ Destination
${JSON.stringify(parseDbUrl(destination), null, 2)}
    `);
  process.exitCode = 0;
  try {
    await dbdiff.compare(source, destination);
    console.log('\n\n\n------------ RESULT ------------\n\n\n');
    console.log(dbdiff.commands(level));
    console.log('\n\n\n------------ RESULT ------------\n\n\n');
  } catch (error) {
    console.error(error);
    throw error;
  }
})();
