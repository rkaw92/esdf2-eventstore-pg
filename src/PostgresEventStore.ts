import { Commit, CommitLocation, EventLocation, QualifiedDomainEvent } from "esdf2-interfaces";
import { Connection, Pool, PoolClient } from "pg";

export type IsolationLevel = "READ COMMITTED" | "REPEATABLE READ" | "SERIALIZABLE";
interface EventRow {
    type: string;
    payload: object;
    id: string;
    sequence: string;
    slot: string;
    index: string;
    committed_at: Date;
};
interface OutboxRow {
    sequence: string;
    slot: string;
    committed_at: Date;
}

function rowToEvent(row: EventRow): QualifiedDomainEvent {
    return {
        id: row.id,
        location: {
            sequence: row.sequence,
            index: Number(row.index)
        },
        type: row.type,
        payload: row.payload
    };
}
function outboxRowToLocation(row: OutboxRow): CommitLocation {
    return {
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
                await client.query('INSERT INTO eventstore.events (type, payload, id, sequence, slot, index, committed_at) VALUES ($1, $2, $3, $4, $5, $6, $7)', [
                    event.type,
                    event.payload,
                    event.id,
                    commit.location.sequence,
                    commit.location.slot,
                    event.location.index,
                    commitTimestamp
                ]);
            }
            await client.query('INSERT INTO eventstore.outbox (sequence, slot) VALUES ($1, $2)', [
                commit.location.sequence,
                commit.location.slot
            ]);
        });
    }

    async load(since: CommitLocation, processorFunction: (event: QualifiedDomainEvent) => void): Promise<{ lastCommit?: CommitLocation, lastEvent?: EventLocation }> {
        const loadBatchSize = this.loadBatchSize;
        return await this.transaction('READ COMMITTED', async function(client) {
            let slot: number = since.slot;
            let lastCommit: CommitLocation | undefined;
            let lastEvent: EventLocation | undefined;
            let processedEventCount: number;
            do {
                const eventRows = (await client.query<EventRow>('SELECT type, payload, id, sequence, slot, index FROM eventstore.events WHERE "sequence" = $1 AND "slot" > $2 ORDER BY index LIMIT $3', [
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
    private async doPublish(client: PoolClient, location: CommitLocation, publisher: (events: QualifiedDomainEvent[]) => Promise<void>): Promise<void> {
        // TODO: Benchmark this FOR UPDATE lock; consider using advisory locks for performance if needed.
        const pendingSlots = (await client.query<{ sequence: string, slot: string }>('SELECT sequence, slot FROM eventstore.outbox WHERE sequence = $1 AND slot <= $2 ORDER BY slot FOR UPDATE', [
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
        const pendingEvents = (await client.query('SELECT type, payload, id, sequence, slot, index FROM eventstore.events WHERE sequence = $1 AND slot >= $2 AND slot <= $3 ORDER BY index', [
            location.sequence,
            first.slot,
            last.slot
        ])).rows;
        await publisher(pendingEvents.map(rowToEvent));
        await client.query('DELETE FROM eventstore.outbox WHERE sequence = $1 AND slot >= $2 AND slot <= $3', [
            location.sequence,
            first.slot,
            last.slot
        ]);
    }
    private async doFindOutstanding(client: PoolClient, minAgeSeconds: number, limit: number): Promise<CommitLocation[]> {
        const intervalString = `${minAgeSeconds} SECOND`;
        const outboxRows = (await client.query<OutboxRow>('SELECT sequence, slot FROM eventstore.outbox WHERE committed_at < (NOW() - $1::interval) LIMIT $2 FOR SHARE SKIP LOCKED', [
            intervalString,
            limit
        ])).rows;
        return outboxRows.map(outboxRowToLocation);
    }

    public async publishSequence(location: CommitLocation, publisher: (events: QualifiedDomainEvent[]) => Promise<void>) {
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
