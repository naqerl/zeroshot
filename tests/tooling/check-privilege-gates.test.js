'use strict';

const assert = require('node:assert/strict');
const { spawnSync } = require('node:child_process');
const path = require('node:path');
const { it } = require('node:test');

const projectRoot = path.resolve(__dirname, '..', '..');

it('tests the privileged-test gate checker', () => {
  const result = spawnSync(
    'python3',
    ['-m', 'unittest', 'discover', '-s', '.github/scripts/tests', '-v'],
    { cwd: projectRoot, encoding: 'utf8' }
  );
  assert.equal(result.status, 0, result.error?.message ?? result.stdout + result.stderr);
});
