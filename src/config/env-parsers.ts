/**
 * Parses a boolean environment value using strict `true` and `false` strings.
 *
 * @param value - Raw environment value, or `undefined` when it was omitted.
 * @param name - Environment variable name used in validation errors.
 * @param defaultValue - Value returned when the environment variable is omitted.
 * @returns The parsed boolean value.
 * @throws {Error} When a supplied value is not `true` or `false`.
 */
export function parseBoolean(
  value: unknown,
  name: string,
  defaultValue?: boolean,
): boolean {
  if (value === undefined || value === '') {
    if (defaultValue !== undefined) {
      return defaultValue;
    }
    throw new Error(`${name} is required`);
  }

  if (typeof value === 'boolean') {
    return value;
  }

  if (typeof value !== 'string') {
    throw new Error(`${name} must be true or false`);
  }

  const normalizedValue = value.trim().toLowerCase();
  if (normalizedValue === 'true') {
    return true;
  }
  if (normalizedValue === 'false') {
    return false;
  }

  throw new Error(`${name} must be true or false`);
}

/**
 * Parses an environment value as an integer greater than zero.
 *
 * @param value - Raw environment value.
 * @param name - Environment variable name used in validation errors.
 * @param defaultValue - Value returned when the environment variable is omitted.
 * @returns The parsed positive integer.
 * @throws {Error} When the value is missing without a default or is not positive.
 */
export function parsePositiveInteger(
  value: unknown,
  name: string,
  defaultValue?: number,
): number {
  if (value === undefined || value === '') {
    if (defaultValue !== undefined) {
      return defaultValue;
    }
    throw new Error(`${name} is required`);
  }

  if (typeof value !== 'number' && typeof value !== 'string') {
    throw new Error(`${name} must be a positive integer`);
  }

  const parsedValue = typeof value === 'number' ? value : Number(value.trim());
  if (!Number.isInteger(parsedValue) || parsedValue <= 0) {
    throw new Error(`${name} must be a positive integer`);
  }

  return parsedValue;
}

/**
 * Parses a comma-separated environment value into trimmed, non-empty entries.
 *
 * @param value - Raw comma-separated environment value.
 * @param name - Environment variable name used in validation errors.
 * @param allowEmpty - Whether an omitted value may produce an empty list.
 * @returns The parsed list.
 * @throws {Error} When no entries remain and empty lists are not allowed.
 */
export function parseCommaSeparatedList(
  value: unknown,
  name: string,
  allowEmpty = false,
): string[] {
  const entries =
    typeof value === 'string'
      ? value
          .split(',')
          .map((entry) => entry.trim())
          .filter(Boolean)
      : [];

  if (entries.length === 0 && !allowEmpty) {
    throw new Error(`${name} must contain at least one value`);
  }

  return entries;
}

/**
 * Parses a JSON environment value and ensures its top-level value is an object.
 *
 * @param value - Raw JSON string.
 * @param name - Environment variable name used in validation errors.
 * @returns The parsed JSON object.
 * @throws {Error} When the value is missing, malformed, an array, or not an object.
 */
export function parseJsonObject(
  value: unknown,
  name: string,
): Record<string, unknown> {
  if (typeof value !== 'string' || value.trim() === '') {
    throw new Error(`${name} is required and must be a JSON object`);
  }

  let parsedValue: unknown;
  try {
    parsedValue = JSON.parse(value) as unknown;
  } catch {
    throw new Error(`${name} must be valid JSON`);
  }

  if (
    parsedValue === null ||
    typeof parsedValue !== 'object' ||
    Array.isArray(parsedValue)
  ) {
    throw new Error(`${name} must be a JSON object`);
  }

  return parsedValue as Record<string, unknown>;
}

/**
 * Replaces escaped newline sequences in a PEM environment value with newlines.
 *
 * @param value - Raw certificate, key, or CA value.
 * @returns The normalized PEM value, preserving `undefined` when omitted.
 */
export function normalizePemNewlines(value: unknown): string | undefined {
  if (value === undefined || value === '') {
    return undefined;
  }

  if (typeof value !== 'string') {
    throw new Error('PEM values must be strings');
  }

  return value.replace(/\\n/g, '\n');
}
