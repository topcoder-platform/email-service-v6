import { execFile } from 'node:child_process';
import { promisify } from 'node:util';

const execFileAsync = promisify(execFile);

describe('application startup script', () => {
  it('reports malformed database URLs without exposing credentials or a stack', async () => {
    const username = 'startup-user-secret';
    const password = 'startup-password-secret';
    const malformedUrl = `postgresql://${username}:${password}@[invalid-host`;

    const result = await execFileAsync('bash', ['appStartUp.sh'], {
      cwd: process.cwd(),
      env: {
        PATH: process.env.PATH ?? '',
        DATABASE_URL: malformedUrl,
        POSTGRES_SCHEMA: 'email',
      },
    }).then(
      ({ stdout, stderr }) => ({ code: 0, stdout, stderr }),
      (error: { code?: number; stdout?: string; stderr?: string }) => ({
        code: error.code ?? 1,
        stdout: error.stdout ?? '',
        stderr: error.stderr ?? '',
      }),
    );
    const output = `${result.stdout}${result.stderr}`;

    expect(result.code).not.toBe(0);
    expect(output.trim()).toBe('database_url_normalization_failed');
    expect(output).not.toContain(username);
    expect(output).not.toContain(password);
    expect(output).not.toContain(malformedUrl);
    expect(output).not.toMatch(/ERR_INVALID_URL|TypeError|\n\s*at\s/);
  });
});
