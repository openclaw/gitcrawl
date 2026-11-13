package remotedelivery

const seedOutboxApplicationID = 0x4743444f // GCDO
const seedOutboxVersion = 1

// Seed-only custody. Future deltas extend this owner through a deliberate
// migration; there is no second checkpoint or qualified-empty registration.
const seedOutboxSchema = `
pragma application_id=1195590735;
pragma user_version=1;
create table binding (
 singleton integer primary key check(singleton=1),
 origin text not null, archive text not null, binding_id text not null,
 registration_json blob not null,
 baseline_state text not null check(baseline_state in ('unqualified','qualified')),
 baseline_native text, baseline_sha256 text, server_cursor text not null default '',
 check ((baseline_state='unqualified' and baseline_native is null and baseline_sha256 is null and server_cursor='')
     or (baseline_state='qualified' and baseline_native is not null and baseline_sha256 is not null and server_cursor<>''))
);
create table captures (
 seed_id text primary key not null,
 anchor_json blob not null,
 native_through text not null, native_sha256 text not null,
 native_rows integer not null check(native_rows>0),
 native_bytes integer not null check(native_bytes>0),
 part_count integer not null check(part_count between 2 and 8194),
 completed integer not null default 0 check(completed in (0,1))
);
create unique index one_seed on captures((1));
create table parts (
 seed_id text not null references captures(seed_id),
 part_index integer not null check(part_index between 0 and 8193),
 body blob not null check(length(body) between 1 and 4194304),
 payload_sha256 text not null check(length(payload_sha256)=64),
 receipt_json blob,
 primary key(seed_id,part_index)
);
`
