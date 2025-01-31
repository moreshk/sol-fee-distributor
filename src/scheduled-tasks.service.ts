import { Injectable, Logger } from '@nestjs/common';
import { Cron, CronExpression } from '@nestjs/schedule';
import { Pool } from 'pg';

@Injectable()
export class ScheduledTasksService {
  private readonly logger = new Logger(ScheduledTasksService.name);
  private pool: Pool;

  constructor() {
    const config = {
      host: process.env.DB_HOST,
      database: process.env.DB_NAME,
      password: process.env.DB_PASSWORD,
      port: parseInt(process.env.DB_PORT),
      user: process.env.DB_USERNAME,
      ssl: {
        rejectUnauthorized: false,
      },
    };

    this.logger.debug('Attempting to connect with config:', {
      ...config,
      password: '***hidden***',
    });

    this.pool = new Pool(config);

    // Test connection
    this.pool
      .connect()
      .then(() => this.logger.log('Successfully connected to database'))
      .catch((err) => this.logger.error('Failed to connect to database:', err));
  }

  @Cron(CronExpression.EVERY_30_SECONDS)
  async handleCron() {
    try {
      await this.processTransactions();
    } catch (error) {
      this.logger.error(
        'Error processing transactions:',
        error.stack || error.message || error,
      );
    }
  }

  async processTransactions() {
    const client = await this.pool.connect();
    try {
      await client.query('BEGIN');

      // Get the largest max transaction id
      const maxIdResult = await client.query(
        'SELECT max_transaction_id FROM transaction_tracker ORDER BY timestamp DESC LIMIT 1',
      );

      let maxTransactionId = null;
      if (maxIdResult.rows.length > 0) {
        maxTransactionId = maxIdResult.rows[0].max_transaction_id;
        this.logger.log(`Last processed transaction ID: ${maxTransactionId}`);
      } else {
        this.logger.log('No transactions processed yet');
      }

      // Get summed sol quantity for each creator wallet address
      const query = `
        SELECT c.creator_wallet_address, SUM(t.sol_quantity) as total_sol
        FROM transactions t
        JOIN coins c ON t.coin_id = c.mint_address
        WHERE t.id > $1
        GROUP BY c.creator_wallet_address
      `;
      const params = [maxTransactionId || 0];
      const result = await client.query(query, params);

      this.logger.log('Transaction fees by creator:');
      result.rows.forEach((row) => {
        this.logger.log(`${row.creator_wallet_address}: ${row.total_sol} SOL`);
      });

      if (result.rows.length > 0) {
        // Get the new max transaction id
        const newMaxIdResult = await client.query(
          'SELECT MAX(id) as max_id FROM transactions',
        );
        const newMaxTransactionId = newMaxIdResult.rows[0].max_id;

        // Update the transaction_tracker
        await client.query(
          'INSERT INTO transaction_tracker (max_transaction_id) VALUES ($1)',
          [newMaxTransactionId],
        );

        this.logger.log(
          `Updated max transaction ID to: ${newMaxTransactionId}`,
        );
      } else {
        this.logger.log('No new transactions to process');
      }

      await client.query('COMMIT');
    } catch (error) {
      await client.query('ROLLBACK');
      throw error;
    } finally {
      client.release();
    }
  }
}
