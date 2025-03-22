const fs = require('fs');
const mysql = require('mysql2/promise');
const csv = require('csv-parser');

const pool = mysql.createPool({
    host: "localhost",
    user: "root",
    password: "Time2rollB@100",
    database: "csv_database", 
    waitForConnections: true,
    connectionLimit: 10,
    queueLimit: 0
});

async function main() {
    const csvFilePath = "data.csv";  // Expiriment with different data
    const conn = await pool.getConnection();

    try {
        // Create database if needed
        await conn.query("CREATE DATABASE IF NOT EXISTS csv_database");
        await conn.query("USE csv_database"); 

        // Get CSV headers
        const headers = await new Promise((resolve) => {
            const stream = fs.createReadStream(csvFilePath)
                .pipe(csv())
                .on('headers', h => resolve(h));
        });

        // Create table
        const columns = [
            'id INT AUTO_INCREMENT PRIMARY KEY',
            ...headers.map(h => `\`${h}\` TEXT`)
        ].join(',');
        
        await conn.query(`CREATE TABLE IF NOT EXISTS csv_data (${columns})`);

        // Insert data
        let batch = [];
        fs.createReadStream(csvFilePath)
            .pipe(csv())
            .on('data', async (row) => {
                batch.push(row);
                if (batch.length >= 100) {
                    await processBatch(batch);
                    batch = [];
                }
            })
            .on('end', async () => {
                if (batch.length > 0) await processBatch(batch);
                console.log("CSV import complete");
                conn.release();
                pool.end();
            });

        async function processBatch(rows) {
            const values = rows.map(row => headers.map(h => row[h]));
            const query = `INSERT INTO csv_data (${headers.map(h => `\`${h}\``)}) VALUES ?`;
            
            await conn.query(query, [values]);
            }
    } catch (err) {
        console.error("Main error:", err);
    }
}

main();