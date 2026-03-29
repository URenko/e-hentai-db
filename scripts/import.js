const fs = require('fs');
const ConnectDB = require('../app/util/connectDB');

class Import {
	constructor() {
		this.connection = new ConnectDB();

		this.tagMap = {};
		this.query = this.query.bind(this);
		this.readFile = this.readFile.bind(this);
		this.run = this.run.bind(this);
		this.filePath = process.argv[2] || './gdata.json';
		this.force = process.argv.indexOf('-f') >= 0;
	}

	query(...args) {
		return new Promise((resolve, reject) => {
			try {
				this.connection.query(...args, (error, results) => {
					if (error) {
						reject(error);
						return;
					}
					resolve(results);
				});
			} catch(err) {
				reject(err);
			}
		});
	}

	readFile() {
		return new Promise((resolve, reject) => {
			try {
				// work around with kMaxLength
				let data = {};
				let lastChunk = '';
				const rstream = fs.createReadStream(this.filePath, 'utf8');

				const parseChunk = (chunk) => {
					const result = chunk.match(/^{?"(\d+)":\s*?({[\s\S]+?})}?$/);
					const [, key, value] = result;
					data[key] = JSON.parse(value);
				};

				rstream.on('error', reject);
				rstream.on('data', (chunk) => {
					const parts = (lastChunk + chunk).split(/,\s?(?="\d+":)/);
					// last part may incomplete
					lastChunk = parts.pop();
					parts.forEach(parseChunk);
				});
				rstream.on('end', () => {
					// finally parse last chunk
					parseChunk(lastChunk);
					resolve(data);
				});
			} catch(err) {
				reject(err);
			}
		});
	}

	loadTags() {
		return this.query('SELECT * FROM tag').then((data) => {
			const result = {};
			data.forEach(e => result[e.name] = e.id);
			return result;
		});
	}

	loadGalleries() {
		return this.query('SELECT gid, posted, bytorrent FROM gallery').then((data) => {
			const result = {};
			data.forEach(e => result[e.gid] = e.posted);
			return result;
		});
	}

	async run() {
		const { connection } = this;

		const t = new Date();
		console.log(`loading gdata.json at ${t}`);

		const data = await this.readFile();
		// prefer to insert the smaller galleries first
		let ids = Object.keys(data).sort((a, b) => a - b);
		const length = ids.length;

		const lt = new Date();
		console.log(`loaded gdata.json at ${lt}, got ${length} records, total time ${lt - t}ms`);

		connection.connect(async (err) => {
			if (err) {
				console.error(err.stack);
				return;
			}
				console.log(`connected as id ${connection.threadId}`);
				const ct = new Date();
				console.log(`started inserting at ${ct}`);

			this.tagMap = await this.loadTags();
			const galleries = await this.loadGalleries();

			let index = 0;
			let inserted = 0;
			const { tagMap } = this;
			for (let id of ids) {
				index++;
				const item = data[id];
				// item may have other keys like `error`
				const {
					tags, gid, token, archiver_key, title, title_jpn, category, thumb, uploader,
					posted, filecount, filesize, expunged, rating, torrentcount, error
				} = item;
				if (error) {
					continue;
				}

				const newTags = tags.filter(e => !tagMap[e]);
				if (newTags.length) {
					const newTagPlaceholders = newTags.map(() => '(?)').join(', ');
					await this.query(`INSERT OR IGNORE INTO tag (name) VALUES ${newTagPlaceholders}`, newTags);
					const selectTagPlaceholders = newTags.map(() => '?').join(', ');
					const results = await this.query(`SELECT * FROM tag WHERE name IN (${selectTagPlaceholders})`, newTags);
					results.forEach((e) => tagMap[e.name] = e.id);
				}

				const queries = []; 
				if (!galleries.hasOwnProperty(gid)) {
					inserted++;
					queries.push(this.query(
						`INSERT INTO gallery (
							gid, token, archiver_key, title, title_jpn, category, thumb, uploader,
							posted, filecount, filesize, expunged, rating, torrentcount
						) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
						[
							gid, token, archiver_key, title, title_jpn, category, thumb, uploader,
							posted, filecount, filesize, expunged, rating, torrentcount
						]
					));
					if (tags.length) {
						const tagRows = tags.map(e => [+id, tagMap[e]]);
						const tagPlaceholders = tagRows.map(() => '(?, ?)').join(', ');
						queries.push(this.query(
							`INSERT OR IGNORE INTO gid_tid (gid, tid) VALUES ${tagPlaceholders}`,
							tagRows.flat()
						));
					}
				}
				else if (this.force || posted > galleries[gid] || galleries.bytorrent) {
					inserted++;
					const curTags = (await this.query('SELECT tid FROM gid_tid WHERE gid = ?', [gid])).map(e => e.tid);
					const tids = tags.map(e => tagMap[e]);
					const addTids = tids.filter(e => curTags.indexOf(e) < 0);
					const delTids = curTags.filter(e => tids.indexOf(e) < 0);
					queries.push(this.query(
						`UPDATE gallery SET
							token = ?, archiver_key = ?, title = ?, title_jpn = ?, category = ?, thumb = ?, uploader = ?,
							posted = ?, filecount = ?, filesize = ?, expunged = ?, rating = ?, torrentcount = ?, bytorrent = 0
						WHERE gid = ?`,
						[
							token, archiver_key, title, title_jpn, category, thumb, uploader,
							posted, filecount, filesize, expunged, rating, torrentcount, gid
						]
					));
					if (addTids.length) {
						const addRows = addTids.map(e => [+id, e]);
						const addPlaceholders = addRows.map(() => '(?, ?)').join(', ');
						queries.push(this.query(
							`INSERT OR IGNORE INTO gid_tid (gid, tid) VALUES ${addPlaceholders}`,
							addRows.flat()
						));
					}
					if (delTids.length) {
						const delRows = delTids.map(e => [+id, e]);
						const delPlaceholders = delRows.map(() => '(?, ?)').join(', ');
						queries.push(this.query(
							`DELETE FROM gid_tid WHERE (gid, tid) IN (${delPlaceholders})`,
							delRows.flat()
						));
					}
				}
				else {
					continue;
				}
				await Promise.all(queries);
				if (inserted % 1000 === 0 || index === length) {
					console.log(`inserted gid = ${id} (${index}/${length})`);
				}
			}
			
			console.log(`inserts complete, inserted ${inserted} galleries`);
			const nt = new Date();
			console.log(`finished at ${nt}, total time ${nt - ct}ms`);

			connection.destroy();
		});
	}
}

process.on('unhandledRejection', (err) => {
	console.log(err.stack);
	instance.connection.destroy();
});

const instance = new Import();
instance.run().catch(err => {
	console.log(err.stack);
	instance.connection.destroy();
});
