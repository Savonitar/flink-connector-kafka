/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const { test } = require('node:test');

const workflow = fs.readFileSync(path.join(__dirname, '../workflows/weekly.yml'), 'utf8');
const AsyncFunction = Object.getPrototypeOf(async function() {}).constructor;

function step(id) {
    const matches = workflow.split(/^      - /m)
        .filter(value => value.includes(`\n        id: ${id}\n`));
    assert.equal(matches.length, 1, `Expected exactly one workflow step with id ${id}`);
    return matches[0];
}

function block(step, header) {
    const lines = step.split('\n');
    assert.equal(lines.filter(line => line === header).length, 1,
        `Expected exactly one YAML block ${header.trim()}`);
    const body = [];
    for (const line of lines.slice(lines.indexOf(header) + 1)) {
        if (line.trim() && !line.startsWith('            ')) break;
        body.push(line.slice(12));
    }
    assert.ok(body.join('\n').trim(), `Empty YAML block ${header.trim()}`);
    return body.join('\n');
}

function script(id) {
    return new AsyncFunction('github', 'context', 'core', 'require', 'process',
        block(step(id), '          script: |'));
}

const previousScript = script('previous');
const matrixScript = script('matrix');
const matrix = JSON.parse(block(step('matrix'), '          FLINK_BRANCHES: >-'));
const statePath = 'state/weekly-matrix.json';

async function execute(script, f, filesystem = {}) {
    const outputs = {};
    await script(f.github, f.context, {
        ...f.core,
        setOutput(name, value) { outputs[name] = value; },
    }, name => {
        assert.equal(name, 'fs');
        return filesystem;
    }, { env: { FLINK_BRANCHES: JSON.stringify(f.matrix) } });
    return outputs;
}

async function findPreviousRun(f) {
    const outputs = await execute(previousScript, f);
    assert.equal(typeof outputs.run_id, 'string');
    return outputs.run_id;
}

async function selectMatrix(f) {
    let state;
    const outputs = await execute(matrixScript, f, {
        readFileSync(file, encoding) {
            assert.equal(file, statePath);
            assert.equal(encoding, 'utf8');
            if (f.readError) throw f.readError;
            if (f.previousText !== undefined) return f.previousText;
            if (f.previous === undefined) throw new Error('Artifact missing');
            return JSON.stringify(f.previous);
        },
        mkdirSync(directory, options) {
            assert.equal(directory, 'state');
            assert.deepEqual(options, { recursive: true });
        },
        writeFileSync(file, contents) {
            assert.equal(file, statePath);
            state = JSON.parse(contents);
        },
    });
    assert.notEqual(state, undefined, 'The matrix step must persist its baseline');
    return { matrix: JSON.parse(outputs.matrix), state };
}

function fixture() {
    const branches = { main: 'main-sha', 'v5.0': 'v5-sha', 'v4.0': 'v4-sha', 'v3.4': 'v3-sha' };
    const runs = [
        { id: 20, run_number: 20, conclusion: null, head_sha: 'workflow-sha' },
        { id: 19, run_number: 19, conclusion: 'success', head_sha: 'workflow-sha' },
    ];
    const lookups = [];
    const warnings = [];
    return {
        matrix,
        branches,
        runs,
        lookups,
        warnings,
        previous: matrix.map(entry => ({ ...entry, sha: branches[entry.branch] })),
        context: {
            eventName: 'schedule',
            repo: { owner: 'apache', repo: 'flink-connector-kafka' },
            ref: 'refs/heads/main',
            runNumber: 20,
            sha: 'workflow-sha',
        },
        core: { info() {}, warning(message) { warnings.push(message); } },
        github: { rest: {
            repos: { async getBranch({ branch }) {
                lookups.push(branch);
                if (!branches[branch]) throw new Error('Branch lookup failed');
                return { data: { commit: { sha: branches[branch] } } };
            } },
            actions: { async listWorkflowRuns(params) {
                assert.equal(params.workflow_id, 'weekly.yml');
                assert.equal(params.branch, 'main');
                assert.equal(params.status, undefined);
                return { data: { workflow_runs: runs } };
            } },
        } },
    };
}

async function completeRun(f, artifacts, conclusion = 'success') {
    const run = {
        id: f.context.runNumber,
        run_number: f.context.runNumber,
        conclusion: null,
        head_sha: f.context.sha,
    };
    f.runs.unshift(run);
    const previousRun = await findPreviousRun(f);
    const result = await selectMatrix({ ...f, previous: artifacts.get(previousRun) });
    artifacts.set(String(run.id), result.state);
    run.conclusion = conclusion;
    f.context.runNumber++;
    return result;
}

test('unchanged green baseline skips released versions but keeps snapshots', async () => {
    const f = fixture();
    assert.equal(await findPreviousRun(f), '19');
    const result = await selectMatrix(f);
    assert.deepEqual(result.matrix, [{ ...matrix[0], sha: 'main-sha' }]);
    assert.deepEqual(result.state, f.previous);
    assert.deepEqual(f.lookups, ['main', 'v5.0', 'v4.0', 'v3.4']);
});

test('successive snapshot-only weeks preserve a successful full-run baseline', async () => {
    const f = fixture();
    f.runs.length = 0;
    const artifacts = new Map();
    const full = await completeRun(f, artifacts);
    assert.equal(full.matrix.length, matrix.length);

    for (let week = 0; week < 3; week++) {
        const snapshots = await completeRun(f, artifacts);
        assert.deepEqual(snapshots.matrix, [full.matrix[0]]);
        assert.deepEqual(snapshots.state, full.state);
    }
});

test('a failed full run must pass before later weeks can resume snapshot-only tests', async () => {
    const f = fixture();
    f.runs.length = 0;
    const artifacts = new Map();
    await completeRun(f, artifacts);
    f.branches['v5.0'] = 'new-v5-sha';

    const failed = await completeRun(f, artifacts, 'failure');
    assert.equal(failed.matrix.length, matrix.length);
    const retry = await completeRun(f, artifacts);
    assert.deepEqual(retry.matrix, failed.matrix);
    const snapshots = await completeRun(f, artifacts);
    assert.deepEqual(snapshots.matrix, [retry.matrix[0]]);
    assert.deepEqual(snapshots.state, retry.state);
});

test('a failed manual full run invalidates an unchanged snapshot-only baseline', async () => {
    const f = fixture();
    f.runs.length = 0;
    const artifacts = new Map();
    const full = await completeRun(f, artifacts);
    assert.equal((await completeRun(f, artifacts)).matrix.length, 1);

    f.context.eventName = 'workflow_dispatch';
    const manual = await completeRun(f, artifacts, 'failure');
    assert.deepEqual(manual.matrix, full.matrix);
    f.context.eventName = 'schedule';
    const retry = await completeRun(f, artifacts);
    assert.deepEqual(retry.matrix, full.matrix);
});

for (const branch of ['main', 'v5.0', 'v4.0', 'v3.4']) {
    test(`new commits on ${branch} run the full matrix at the resolved SHAs`, async () => {
        const f = fixture();
        f.branches[branch] = 'new-sha';
        const result = await selectMatrix(f);
        assert.equal(result.matrix.length, matrix.length);
        assert.ok(result.matrix.filter(entry => entry.branch === branch)
            .every(entry => entry.sha === 'new-sha'));
        assert.deepEqual(result.state, result.matrix);
    });
}

test('manual dispatch always runs the full matrix', async () => {
    const f = fixture();
    f.context.eventName = 'workflow_dispatch';
    f.github.rest.actions.listWorkflowRuns = () => assert.fail('Manual run queried history');
    assert.equal(await findPreviousRun(f), '');
    assert.equal((await selectMatrix(f)).matrix.length, matrix.length);
});

for (const conclusion of ['failure', 'cancelled', 'timed_out', 'skipped', null]) {
    test(`a previous ${conclusion} run cannot reuse an older green baseline`, async () => {
        const f = fixture();
        f.runs[1].conclusion = conclusion;
        f.runs.push({ id: 18, run_number: 18, conclusion: 'success', head_sha: 'workflow-sha' });
        assert.equal(await findPreviousRun(f), '');
    });
}

test('current and newer runs are excluded when selecting the previous run', async () => {
    const f = fixture();
    f.runs.unshift({ id: 21, run_number: 21, conclusion: 'success', head_sha: 'workflow-sha' });
    assert.equal(await findPreviousRun(f), '19');
});

test('out-of-order history still selects the latest earlier run', async () => {
    const f = fixture();
    f.runs.push({ id: 18, run_number: 18, conclusion: 'failure', head_sha: 'workflow-sha' });
    f.runs.reverse();
    assert.equal(await findPreviousRun(f), '19');
});

test('changed workflow revision invalidates the baseline', async () => {
    const f = fixture();
    f.runs[1].head_sha = 'old-workflow-sha';
    assert.equal(await findPreviousRun(f), '');
});

test('first run and inaccessible history require a full build', async () => {
    const f = fixture();
    f.runs.length = 0;
    assert.equal(await findPreviousRun(f), '');
    f.github.rest.actions.listWorkflowRuns = async () => { throw new Error('API unavailable'); };
    assert.equal(await findPreviousRun(f), '');
    assert.equal(f.warnings.length, 1);
});

for (const previous of [undefined, null, [], {}, [{ branch: 'main' }]]) {
    test(`missing or invalid baseline ${JSON.stringify(previous)} runs all versions`, async () => {
        const f = fixture();
        f.previous = previous;
        assert.equal((await selectMatrix(f)).matrix.length, matrix.length);
    });
}

test('a malformed artifact runs the full matrix', async () => {
    const f = fixture();
    f.previousText = '{invalid JSON';
    assert.equal((await selectMatrix(f)).matrix.length, matrix.length);
});

test('an unreadable artifact runs the full matrix', async () => {
    const f = fixture();
    f.readError = new Error('Artifact cannot be read');
    assert.equal((await selectMatrix(f)).matrix.length, matrix.length);
});

test('matrix changes invalidate the baseline even with unchanged branch SHAs', async () => {
    const f = fixture();
    f.matrix = matrix.map(entry => ({ ...entry, jdk: '21' }));
    assert.equal((await selectMatrix(f)).matrix.length, matrix.length);
});

test('branch lookup failure falls back to the branch and cannot record a green baseline', async () => {
    const f = fixture();
    delete f.branches['v5.0'];
    const result = await selectMatrix(f);
    assert.equal(result.matrix.length, matrix.length);
    assert.ok(result.matrix.filter(entry => entry.branch === 'v5.0')
        .every(entry => (entry.sha || entry.branch) === 'v5.0'));
    assert.deepEqual(result.state, []);
    assert.equal(f.warnings.length, 1);
    f.branches['v5.0'] = 'v5-sha';
    f.previous = result.state;
    assert.equal((await selectMatrix(f)).matrix.length, matrix.length);
});
