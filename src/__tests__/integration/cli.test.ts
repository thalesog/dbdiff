import { runCLI } from '../helpers';
describe('dbdiff', () => {
  it('should display the help contents', () => {
    const { stdout } = runCLI(process.cwd(), ['--help']);

    expect(stdout).toContain('Usage: dbdiff [options]');
    expect(stdout).toContain(
      '-V, --version              output the version number'
    );
    expect(stdout).toContain('-s, --source [dbUrl]       source database url');
    expect(stdout).toContain(
      '-d, --destination [dbUrl]  destination database url'
    );
    expect(stdout).toContain(
      '-l, --level <level>        chooses the safety of the sql (choices: "safe",'
    );
    expect(stdout).toContain(
      '-h, --help                 display help for command'
    );
  });

  it('should display version', () => {
    const { stdout } = runCLI(process.cwd(), ['--version']);
    expect(stdout).toContain('1.0.0');
  });
});
