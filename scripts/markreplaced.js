const ConnectDB = require('../app/util/connectDB');

class MarkReplaced {
	constructor() {
		this.connection = new ConnectDB();

		this.query = this.query.bind(this);
		this.run = this.run.bind(this);
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
			} catch (err) {
				reject(err);
			}
		});
	}

	async run() {
		const { connection } = this;

		connection.connect(async (err) => {
			if (err) {
				console.error(err.stack);
				return;
			}

			await this.query(`
				WITH latest AS (
					SELECT COALESCE(root_gid, gid) AS group_gid, MAX(gid) AS max_gid
					FROM gallery
					GROUP BY COALESCE(root_gid, gid)
				)
				UPDATE gallery
				SET replaced = CASE
					WHEN gid IN (SELECT max_gid FROM latest) THEN 0
					ELSE 1
				END
			`);
			connection.destroy();
		});
	}
}

process.on('unhandledRejection', (err) => {
	console.log(err.stack);
	instance.connection.destroy();
});

const instance = new MarkReplaced();
instance.run().catch(err => {
	console.log(err.stack);
	instance.connection.destroy();
});
