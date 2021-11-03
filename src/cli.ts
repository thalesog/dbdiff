import { Command, Option } from 'commander';
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

console.log('Starting comparison');
console.log(`Source => ${source}`);
console.log(`Destination => ${destination}`);
console.log(`Level => ${level}`);

async function main() {
  try {
    await dbdiff.compare(source, destination);
    console.log(dbdiff.commands(level));
  } catch (error) {
    console.error(error);
  }
}

void main();