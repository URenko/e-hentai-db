module.exports = {
	dbHost: process.env.DB_HOST || 'localhost',
	dbPort: parseInt(process.env.DB_PORT) || 3306,
	dbName: process.env.DB_NAME || 'e-hentai-db',
	dbUser: process.env.DB_USER || 'root',
	dbPass: process.env.DB_PASS || '',
	port: 8880,
	cors: false,
	corsOrigin: '*',
	webui: false,
	webuiPath: 'dist',
};