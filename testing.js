const fs = require('fs');
const mysql = require('mysql2/promise');
const csv = require('csv-parser');

const pool = mysql.createPool({
    host: "localhost",
    user: "root",
    password: "Time2rollB@100",
    database: "testing_database", 
    waitForConnections: true,
    connectionLimit: 10,
    queueLimit: 0
});

async function main() {
    const csvFilePath = "sample.csv";
    let conn;

    try {
        // Create database first with separate connection
        const adminConn = await mysql.createConnection({
            host: "localhost",
            user: "root",
            password: "Time2rollB@100"
        });
        await adminConn.query("CREATE DATABASE IF NOT EXISTS csv_database");
        await adminConn.end();

        conn = await pool.getConnection();

        // Get CSV headers
        const headers = await new Promise((resolve) => {
            const stream = fs.createReadStream(csvFilePath)
                .pipe(csv())
                .on('headers', h => resolve(h));
        });

        // Create table - exclude CSV's 'id' column since we have our own PK
        const filteredHeaders = headers.filter(h => h.toLowerCase() !== 'id');
        const columns = [
            'id INT AUTO_INCREMENT PRIMARY KEY',
            ...filteredHeaders.map(h => `\`${h}\` TEXT`)
        ].join(',');

        await conn.query(`CREATE TABLE IF NOT EXISTS csv_data (${columns})`);

        // Insert data (only CSV headers, excluding our PK)
        let batch = [];
        const stream = fs.createReadStream(csvFilePath)
            .pipe(csv())
            .on('data', (row) => {
                batch.push(row);
                if (batch.length >= 100) {
                    stream.pause();
                    processBatch(batch).then(() => {
                        batch = [];
                        stream.resume();
                    });
                }
            })
            .on('end', async () => {
                if (batch.length > 0) await processBatch(batch);
                console.log("CSV import complete");
                conn.release();
                pool.end();
            });

        async function processBatch(rows) {
            const values = rows.map(row => filteredHeaders.map(h => row[h]));
            const query = `INSERT INTO csv_data (${filteredHeaders.map(h => `\`${h}\``).join(',')}) VALUES ?`;
            await conn.query(query, [values]);
        }
    } catch (err) {
        console.error("Main error:", err);
        if (conn) conn.release();
        pool.end();
    }
}

main();