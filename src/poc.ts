import { Pool } from 'pg';
import { PostgresEventStore } from './PostgresEventStore';
import * as uuid from 'uuid';
import { QualifiedDomainEvent } from 'esdf2-interfaces';

const publisher = async function(events: QualifiedDomainEvent[]) {
    for (let event of events) {
        console.log('Will publish event %j', event.location);
        await new Promise(function(resolve) { setTimeout(resolve, 1000); });
        console.log('Published event %j', event.location);
    }
};

(async function() {
    // Init:
    const pool = new Pool();
    const store = new PostgresEventStore(pool);

    // Work:
    const sequence = uuid.v4();
    const commit = {
        location: {
            sequence: sequence,
            slot: 1,
            aggregateName: 'Customer'
        },
        events: [{
            id: uuid.v4(),
            location: {
                sequence: sequence,
                index: 1,
                aggregateName: 'Customer'
            },
            type: 'Registered',
            payload: {
                date: new Date(),
                name: 'New Company 1',
                VATID: '1234567890'
            }
        }]
    };
    await store.save(commit);
    await store.publishSequence(commit.location, publisher);
    await store.publishOutstanding(publisher);

    await store.load({ aggregateName: 'Product', sequence: commit.location.sequence, slot: 0 }, function(event) {
        console.log('loaded:', event);
    });

    // Teardown:
    await pool.end();
})();
