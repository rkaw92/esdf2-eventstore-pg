import { AggregateCommitLocation, Commit, CommitLocation, EventLocation, QualifiedDomainEvent } from "esdf2-interfaces";
import { Connection, Pool, PoolClient } from "pg";

export type IsolationLevel = "READ COMMITTED" | "REPEATABLE READ" | "SERIALIZABLE";
interface EventRow {
    aggregate_name: string;
    type: string;
    payload: object;
    id: string;
    sequence: string;
    slot: string;
    index: string;
    committed_at: Date;
};
interface OutboxRow {
    aggregate_name: string;
    sequence: string;
    slot: string;
    committed_at: Date;
}

function rowToEvent(row: EventRow): QualifiedDomainEvent {
    return {
        id: row.id,
        location: {
            aggregateName: row.aggregate_name,
            sequence: row.sequence,
            index: Number(row.index)
        },
        type: row.type,
        payload: row.payload
    };
}
function outboxRowToLocation(row: OutboxRow): AggregateCommitLocation {
    return {
        aggregateName: row.aggregate_name,
        sequence: row.sequence,
        slot: Number(row.slot)
    };
}

export class PostgresEventStore {
    private pool: Pool;
    private loadBatchSize: number;
    constructor(pool: Pool, {
        loadBatchSize = 100
    }: {
        loadBatchSize?: number
    } = {}) {
        this.pool = pool;
        this.loadBatchSize = loadBatchSize;
    }
    private async transaction<ResolvedType>(level: IsolationLevel, workFunction: (conn: PoolClient) => Promise<ResolvedType>) {
        const connection = await this.pool.connect();
        try {
            await connection.query(`BEGIN TRANSACTION ISOLATION LEVEL ${level}`);
            const result = await workFunction(connection);
            await connection.query('COMMIT');
            return result;
        } catch (error) {
            await connection.query('ROLLBACK');
            throw error;
        } finally {
            connection.release();
        }
    }
    async save(commit: Commit): Promise<void> {
        const commitTimestamp = (new Date()).toISOString();
        await this.transaction('READ COMMITTED', async function(client) {
            // TODO: Optimize this - generate bulk-insert DMLs instead of individual INSERTs.
            for (let event of commit.events) {
                await client.query('INSERT INTO eventstore.events (aggregate_name, type, payload, id, sequence, slot, index, committed_at) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)', [
                    event.location.aggregateName,
                    event.type,
                    event.payload,
                    event.id,
                    commit.location.sequence,
                    commit.location.slot,
                    event.location.index,
                    commitTimestamp
                ]);
            }
            await client.query('INSERT INTO eventstore.outbox (aggregate_name, sequence, slot) VALUES ($1, $2, $3)', [
                commit.location.aggregateName,
                commit.location.sequence,
                commit.location.slot
            ]);
        });
    }

    async load(since: AggregateCommitLocation, processorFunction: (event: QualifiedDomainEvent) => void): Promise<{ lastCommit?: CommitLocation, lastEvent?: EventLocation }> {
        const loadBatchSize = this.loadBatchSize;
        return await this.transaction('READ COMMITTED', async function(client) {
            let slot: number = since.slot;
            let lastCommit: CommitLocation | undefined;
            let lastEvent: EventLocation | undefined;
            let processedEventCount: number;
            do {
                const eventRows = (await client.query<EventRow>('SELECT aggregate_name, type, payload, id, sequence, slot, index FROM eventstore.events WHERE "aggregate_name" = $1 AND "sequence" = $2 AND "slot" > $3 ORDER BY index LIMIT $4', [
                    since.aggregateName,
                    since.sequence,
                    slot,
                    loadBatchSize
                ])).rows;
                for (let eventRow of eventRows) {
                    const event = rowToEvent(eventRow);
                    processorFunction(event);
                    slot = Number(eventRow.slot);
                    lastEvent = event.location;
                    lastCommit = {
                        sequence: event.location.sequence,
                        slot: slot
                    };
                }
                processedEventCount = eventRows.length;
            } while (processedEventCount === loadBatchSize);
            return {
                lastCommit,
                lastEvent
            };
        });
    }
    private async doPublish(client: PoolClient, location: AggregateCommitLocation, publisher: (events: QualifiedDomainEvent[]) => Promise<void>): Promise<void> {
        // TODO: Benchmark this FOR UPDATE lock; consider using advisory locks for performance if needed.
        const pendingSlots = (await client.query<{ sequence: string, slot: string }>('SELECT aggregate_name, sequence, slot FROM eventstore.outbox WHERE aggregate_name = $1 AND sequence = $2 AND slot <= $3 ORDER BY slot FOR UPDATE', [
            location.aggregateName,
            location.sequence,
            location.slot
        ])).rows;
        if (pendingSlots.length === 0) {
            return;
        }
        const first = pendingSlots[0];
        const last = pendingSlots[pendingSlots.length - 1];
        // Assume no holes (in the worst case, we'll end up re-publishing something):
        // TODO: LIMIT to guard against really big commits?
        const pendingEvents = (await client.query('SELECT aggregate_name, type, payload, id, sequence, slot, index FROM eventstore.events WHERE aggregate_name = $1 AND sequence = $2 AND slot >= $3 AND slot <= $4 ORDER BY index', [
            location.aggregateName,
            location.sequence,
            first.slot,
            last.slot
        ])).rows;
        await publisher(pendingEvents.map(rowToEvent));
        await client.query('DELETE FROM eventstore.outbox WHERE aggregate_name = $1 AND sequence = $2 AND slot >= $3 AND slot <= $4', [
            location.aggregateName,
            location.sequence,
            first.slot,
            last.slot
        ]);
    }
    private async doFindOutstanding(client: PoolClient, minAgeSeconds: number, limit: number): Promise<AggregateCommitLocation[]> {
        const intervalString = `${minAgeSeconds} SECOND`;
        const outboxRows = (await client.query<OutboxRow>('SELECT aggregate_name, sequence, slot FROM eventstore.outbox WHERE committed_at < (NOW() - $1::interval) LIMIT $2 FOR SHARE SKIP LOCKED', [
            intervalString,
            limit
        ])).rows;
        return outboxRows.map(outboxRowToLocation);
    }

    public async publishSequence(location: AggregateCommitLocation, publisher: (events: QualifiedDomainEvent[]) => Promise<void>) {
        return this.transaction('READ COMMITTED', (client) => this.doPublish(client, location, publisher));
    }

    public async publishOutstanding(publisher: (events: QualifiedDomainEvent[]) => Promise<void>, minAgeSeconds = 30, limit = 100) {
        return this.transaction('READ COMMITTED', async (client) => {
            const outboxItems = await this.doFindOutstanding(client, minAgeSeconds, limit);
            for (let location of outboxItems) {
                await this.doPublish(client, location, publisher);
            }
        });
    }
};
