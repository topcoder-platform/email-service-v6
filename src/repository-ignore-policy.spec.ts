import { readFileSync } from 'node:fs';
import { resolve } from 'node:path';

const projectRoot = resolve(__dirname, '..');

/**
 * Reads an ignore file into the normalized rules used by policy assertions.
 *
 * @param fileName - Ignore-file name relative to the project root.
 * @returns Trimmed, slash-normalized rules with blank lines and comments removed.
 * @throws If the ignore file cannot be read from the project root.
 */
function readIgnoreRules(fileName: string): string[] {
  return readFileSync(resolve(projectRoot, fileName), 'utf8')
    .split(/\r?\n/u)
    .map((rule) => rule.trim().replaceAll('\\', '/').replace(/^\.\//u, ''))
    .filter((rule) => rule.length > 0 && !rule.startsWith('#'));
}

const sharedRequiredRules = {
  environment: ['.env', '.env.*'],
  dependencies: ['node_modules/'],
  'generated output': ['dist/', 'coverage/'],
  logs: [
    'logs/',
    '*.log',
    'npm-debug.log*',
    'yarn-debug.log*',
    'yarn-error.log*',
    'pnpm-debug.log*',
  ],
  'temporary files and caches': [
    'tmp/',
    'temp/',
    '.cache/',
    '.eslintcache',
    '*.tmp',
    '*.temp',
    '*.swp',
    '*.swo',
    '*~',
    '*.bak',
  ],
  'editors and operating-system metadata': [
    '.vscode/',
    '.idea/',
    '*.iml',
    '.DS_Store',
    '._*',
    'Thumbs.db',
    'Desktop.ini',
  ],
  'private keys and certificates': [
    'id_rsa',
    'id_dsa',
    'id_ecdsa',
    'id_ed25519',
    '*.ppk',
    '*.pem',
    '*.key',
    '*.crt',
    '*.cer',
    '*.cert',
    '*.der',
    '*.csr',
    '*.p12',
    '*.pfx',
    '*.pkcs12',
    '*.jks',
    '*.keystore',
    '*.truststore',
    '*.ca-bundle',
    '*.crt-bundle',
    '*.p7b',
    '*.p7c',
  ],
} as const;

describe.each(['.gitignore', '.dockerignore'])(
  '%s repository safeguards',
  (fileName) => {
    const rules = readIgnoreRules(fileName);

    it.each(Object.entries(sharedRequiredRules))(
      'covers %s',
      (_category, requiredRules) => {
        expect(rules).toEqual(expect.arrayContaining(requiredRules));
      },
    );

    it('retains the environment sample through last-match semantics', () => {
      const environmentWildcardIndex = rules.indexOf('.env.*');
      const sampleExceptionIndex = rules.indexOf('!.env.sample');

      expect(environmentWildcardIndex).toBeGreaterThanOrEqual(0);
      expect(sampleExceptionIndex).toBeGreaterThan(environmentWildcardIndex);
    });
  },
);

describe('Docker build-context policy', () => {
  const dockerIgnoreRules = readIgnoreRules('.dockerignore');

  it('recursively excludes protected context categories and Git metadata', () => {
    expect(dockerIgnoreRules).toEqual(
      expect.arrayContaining([
        '**/.env',
        '**/.env.*',
        '**/.git/',
        '**/node_modules/',
        '**/dist/',
        '**/coverage/',
        '**/logs/',
        '**/*.log',
        '**/tmp/',
        '**/temp/',
        '**/.cache/',
        '**/.vscode/',
        '**/.idea/',
        '**/.DS_Store',
        '**/Thumbs.db',
        ...sharedRequiredRules['private keys and certificates'].map(
          (rule) => `**/${rule}`,
        ),
      ]),
    );
  });

  it.each([
    'src/**',
    'prisma/**',
    'prisma.config.ts',
    'package.json',
    'pnpm-lock.yaml',
    'nest-cli.json',
    'tsconfig.json',
    'tsconfig.build.json',
    'eslint.config.mjs',
    '.prettierrc',
    'appStartUp.sh',
    'README.md',
  ])('keeps required build input %s eligible for COPY', (requiredPath) => {
    expect(dockerIgnoreRules).not.toContain(requiredPath);
    expect(dockerIgnoreRules).not.toContain(`!${requiredPath}`);
  });
});

describe('Dockerfile build and startup contract', () => {
  const dockerfile = readFileSync(resolve(projectRoot, 'Dockerfile'), 'utf8');

  it('copies source and performs the frozen install, lint, and build', () => {
    expect(dockerfile).toMatch(/^COPY\s+\.\s+\.\s*$/mu);
    expect(dockerfile).toMatch(
      /^RUN\s+pnpm install --frozen-lockfile(?:\s|$)/mu,
    );
    expect(dockerfile).toMatch(/^RUN\s+pnpm (?:run )?lint\s*$/mu);
    expect(dockerfile).toMatch(/^RUN\s+pnpm (?:run )?build\s*$/mu);
  });

  it('prepares and starts the application startup script', () => {
    expect(dockerfile).toMatch(/^RUN\s+chmod \+x appStartUp\.sh\s*$/mu);
    expect(dockerfile).toMatch(
      /^CMD\s+\[\s*["']\.\/appStartUp\.sh["']\s*\]\s*$/mu,
    );
  });
});
