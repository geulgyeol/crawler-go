-- links table stores URLs and its last accessed timestamp (for re-crawling purposes)

CREATE TABLE IF NOT EXISTS bundles (
    id           bigint generated always as identity primary key,
    object_key   text        not null unique,
    created_at timestamp not null default now()
);

CREATE TABLE IF NOT EXISTS bundle_entries (
    bundle_id     bigint not null references bundles(id),
    frame_offset  bigint not null check (frame_offset >= 0),
    url           text   not null,

    PRIMARY KEY (bundle_id, frame_offset)
);

CREATE INDEX bundle_entries_url_idx
    ON bundle_entries USING HASH (url);

-- users table stores users of the blog platforms for re-crawling purposes

CREATE TABLE IF NOT EXISTS users (
    blog_platform text not null,
    user_id text not null,
    last_enqueued_at timestamp not null default current_timestamp,
    created_at timestamp not null default current_timestamp,
    primary key (blog_platform, user_id)
);