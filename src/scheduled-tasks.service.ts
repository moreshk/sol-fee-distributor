import { Injectable, Logger } from '@nestjs/common';
import { Cron, CronExpression } from '@nestjs/schedule';
import { Pool } from 'pg';
import {
  Connection,
  Keypair,
  LAMPORTS_PER_SOL,
  PublicKey,
  SystemProgram,
  Transaction,
} from '@solana/web3.js';
import bs58 from 'bs58';

@Injectable()
export class ScheduledTasksService {
  private readonly logger = new Logger(ScheduledTasksService.name);
  private pool: Pool;
  private connection: Connection;
  private feeMasterKeypair: Keypair;

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

    // Initialize Solana connection
    this.connection = new Connection(
      'https://mainnet.helius-rpc.com/?api-key=1bd7151b-2c57-45f5-8172-b32538120d8e',
      'confirmed',
    );

    // Initialize fee master wallet
    const privateKeyBytes = bs58.decode(process.env.FEE_MASTER_PRIVATE_KEY);
    this.feeMasterKeypair = Keypair.fromSecretKey(privateKeyBytes);

    this.logger.log(
      `Fee master wallet initialized: ${this.feeMasterKeypair.publicKey.toString()}`,
    );
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

  async sendSolanaTransactions(
    distributions: { recipient: string; amount: number }[],
  ) {
    const BATCH_SIZE = 10;
    const FEE_ADJUSTMENT = 0.99;
    const MAX_RETRIES = 3;
    const RETRY_DELAY = 2000;
    const MIN_AMOUNT_THRESHOLD = 0.001; // 0.001 SOL minimum to avoid rent issues

    // Filter out distributions below minimum threshold
    const validDistributions = distributions.filter(
      (dist) => dist.amount >= MIN_AMOUNT_THRESHOLD,
    );

    if (validDistributions.length === 0) {
      this.logger.log('No distributions above minimum threshold');
      return;
    }

    for (let i = 0; i < validDistributions.length; i += BATCH_SIZE) {
      const batch = validDistributions.slice(i, i + BATCH_SIZE);
      let retryCount = 0;
      let success = false;

      while (!success && retryCount < MAX_RETRIES) {
        const transaction = new Transaction();

        for (const dist of batch) {
          const adjustedAmount = Math.floor(
            dist.amount * LAMPORTS_PER_SOL * FEE_ADJUSTMENT,
          );

          transaction.add(
            SystemProgram.transfer({
              fromPubkey: this.feeMasterKeypair.publicKey,
              toPubkey: new PublicKey(dist.recipient),
              lamports: adjustedAmount,
            }),
          );
        }

        try {
          const latestBlockhash = await this.connection.getLatestBlockhash();
          transaction.recentBlockhash = latestBlockhash.blockhash;
          transaction.feePayer = this.feeMasterKeypair.publicKey;

          const signature = await this.connection.sendTransaction(transaction, [
            this.feeMasterKeypair,
          ]);

          // Wait for confirmation
          const confirmation = await this.connection.confirmTransaction({
            signature,
            blockhash: latestBlockhash.blockhash,
            lastValidBlockHeight: latestBlockhash.lastValidBlockHeight,
          });

          if (confirmation.value.err) {
            throw new Error(`Transaction failed: ${confirmation.value.err}`);
          }

          this.logger.log(`Batch transaction successful: ${signature}`);

          // Insert distribution records within a transaction
          const client = await this.pool.connect();
          try {
            await client.query('BEGIN');

            for (const dist of batch) {
              await client.query(
                'INSERT INTO fee_distributions (receiver_address, transaction_hash, amount) VALUES ($1, $2, $3)',
                [dist.recipient, signature, dist.amount],
              );

              // Update user's fees_claimed_so_far
              await client.query(
                'UPDATE users SET fees_claimed_so_far = fees_claimed_so_far + $1 WHERE wallet_address = $2',
                [dist.amount, dist.recipient],
              );
            }

            await client.query('COMMIT');
            success = true;
          } catch (dbError) {
            await client.query('ROLLBACK');
            throw dbError;
          } finally {
            client.release();
          }
        } catch (error) {
          retryCount++;
          if (retryCount < MAX_RETRIES) {
            this.logger.warn(
              `Failed to process batch (attempt ${retryCount}/${MAX_RETRIES}): ${error.message}`,
            );
            await new Promise((resolve) => setTimeout(resolve, RETRY_DELAY));
          } else {
            this.logger.error(
              `Failed to process batch after ${MAX_RETRIES} attempts: ${error.message}`,
            );
            throw error;
          }
        }
      }
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

      // Modify the query to accumulate amounts until they reach the minimum threshold
      const query = `
        WITH accumulated_fees AS (
          SELECT 
            c.creator_wallet_address,
            SUM(t.sol_quantity * 0.002) as total_sol
          FROM transactions t
          JOIN coins c ON t.coin_id = c.mint_address
          WHERE t.id > $1
          GROUP BY c.creator_wallet_address
          HAVING SUM(t.sol_quantity * 0.002) >= 0.001  -- Only include amounts >= 0.001 SOL
        )
        SELECT creator_wallet_address, total_sol
        FROM accumulated_fees
      `;
      const params = [maxTransactionId || 0];
      const result = await client.query(query, params);

      this.logger.log('Transaction fees by creator:');
      result.rows.forEach((row) => {
        this.logger.log(`${row.creator_wallet_address}: ${row.total_sol} SOL`);
      });

      if (result.rows.length > 0) {
        const distributions = result.rows.map((row) => ({
          recipient: row.creator_wallet_address,
          amount: parseFloat(row.total_sol),
        }));

        this.logger.log('Processing distributions:', distributions);
        await this.sendSolanaTransactions(distributions);

        // Update max transaction id after successful distributions
        const newMaxIdResult = await client.query(
          'SELECT MAX(id) as max_id FROM transactions',
        );
        const newMaxTransactionId = newMaxIdResult.rows[0].max_id;

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
