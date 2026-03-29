const fs = require('fs');
const path = require('path');
const { DatabaseSync } = require('node:sqlite');
const config = require('../../config');

const SELECT_SQL_RE = /^\s*(SELECT|PRAGMA|EXPLAIN|VALUES)\b/i;

const SCHEMA_TABLE_CHECK_SQL = `
	SELECT 1
	FROM sqlite_master
	WHERE type = 'table' AND name = 'gallery'
	LIMIT 1
`;

function normalizeValue(value) {
	if (value === undefined) {
		return null;
	}
	if (typeof value === 'boolean') {
		return value ? 1 : 0;
	}
	return value;
}

function escapeLiteral(value) {
	const normalized = normalizeValue(value);
	if (normalized === null) {
		return 'NULL';
	}
	if (typeof normalized === 'number') {
		if (!Number.isFinite(normalized)) {
			return 'NULL';
		}
		return `${normalized}`;
	}
	if (typeof normalized === 'bigint') {
		return normalized.toString();
	}
	if (normalized instanceof Date) {
		return `'${normalized.toISOString().replace('T', ' ').replace('Z', '')}'`;
	}
	return `'${String(normalized).replace(/'/g, '\'\'')}'`;
}

function formatToken(value) {
	return escapeLiteral(value);
}

class ConnectDB {
	constructor() {
		this.dbPath = path.isAbsolute(config.sqlitePath)
			? config.sqlitePath
			: path.resolve(__dirname, '../../', config.sqlitePath);
		this.db = null;
		this.threadId = 'sqlite';
		this.connection = {
			format: this.format.bind(this)
		};
		this.connect = this.connect.bind(this);
		this.query = this.query.bind(this);
		this.destroy = this.destroy.bind(this);
	}

	connect(callback) {
		const runConnect = async () => {
			if (this.db) {
				return this;
			}
			const dir = path.dirname(this.dbPath);
			if (!fs.existsSync(dir)) {
				fs.mkdirSync(dir, { recursive: true });
			}
			this.db = new DatabaseSync(this.dbPath);

			await this.exec('PRAGMA busy_timeout = 30000');
			await this.exec('PRAGMA synchronous = NORMAL');
			await this.ensureSchema();
			return this;
		};
		if (typeof callback === 'function') {
			runConnect().then(() => callback(null)).catch(err => callback(err));
			return;
		}
		return runConnect();
	}

	exec(sql, values = []) {
		const statement = this.db.prepare(sql);
		const result = statement.run(...values);
		return {
			affectedRows: result.changes,
			insertId: Number(result.lastInsertRowid || 0),
			changes: result.changes
		};
	}

	all(sql, values = []) {
		const statement = this.db.prepare(sql);
		return statement.all(...values);
	}

	get(sql, values = []) {
		const statement = this.db.prepare(sql);
		return statement.get(...values);
	}

	async ensureSchema() {
		const row = await this.get(SCHEMA_TABLE_CHECK_SQL);
		if (row) {
			return;
		}
		const structPath = path.resolve(__dirname, '../../struct.sql');
		const schemaSql = fs.readFileSync(structPath, 'utf8');
		this.db.exec(schemaSql);
	}

	format(sql, values = []) {
		const params = Array.isArray(values) ? values : [values];
		let index = 0;
		return String(sql).replace(/\?/g, () => {
			if (index >= params.length) {
				return '?';
			}
			const token = formatToken(params[index]);
			index++;
			return token;
		});
	}

	async queryInternal(sql, values) {
		await this.connect();
		const params = Array.isArray(values) ? values : (values === undefined ? [] : [values]);
		if (SELECT_SQL_RE.test(sql)) {
			return this.all(sql, params.map(normalizeValue));
		}
		return this.exec(sql, params.map(normalizeValue));
	}

	query(sql, values, callback) {
		let params = values;
		let cb = callback;
		if (typeof values === 'function') {
			cb = values;
			params = undefined;
		}
		if (typeof cb === 'function') {
			this.queryInternal(sql, params).then(
				results => cb(null, results),
				err => cb(err)
			);
			return;
		}
		return this.queryInternal(sql, params);
	}

	destroy() {
		if (!this.db) {
			return;
		}
		this.db.close();
		this.db = null;
	}
}

module.exports = ConnectDB;
