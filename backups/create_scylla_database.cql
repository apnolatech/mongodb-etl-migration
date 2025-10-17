-- ScyllaDB/Cassandra Database Creation Script
-- This script creates all tables for the chat application

-- First, create the keyspace (database)
-- Adjust the replication strategy and factor based on your cluster configuration
CREATE KEYSPACE IF NOT EXISTS appcampana
WITH replication = {
    'class': 'SimpleStrategy',
    'replication_factor': 3
};

-- Use the keyspace
USE appcampana;

-- #############################################################################
-- # Core Tables
-- #############################################################################

-- Table to store messages for each room.
-- OPTIMIZED FOR: GetMessagesFromRoom (read chat history).
CREATE TABLE IF NOT EXISTS messages_by_room (
    room_id uuid,           -- Partition Key: Groups all messages from a room together.
    message_id timeuuid,    -- Clustering Key: Orders messages chronologically within the room.
    sender_id int,
    content text,
    content_decrypted text, -- To match SQL implementation
    type text,
    created_at timestamp,
    updated_at timestamp,
    deleted_at timestamp,
    edited boolean,
    is_deleted boolean,     -- Used for soft-delete.
    reply_to_message_id timeuuid,
    forwarded_from_message_id timeuuid,
    file_url text,
    file_name text,         -- Added: file name
    file_size bigint,       -- Added: file size in bytes
    file_thumbnail_url text,
    audio_duration int,     -- Added: audio duration in seconds
    event text,
    sender_message_id text, -- Optional client ID for idempotency.
    poll_question text,
    poll_is_multiple_options boolean,
    mime_type text,
    PRIMARY KEY ((room_id), message_id)
) WITH CLUSTERING ORDER BY (message_id DESC);


-- Denormalized table for a user's room list.
-- OPTIMIZED FOR: GetRooms (render the user's main chat screen).
CREATE TABLE IF NOT EXISTS rooms_by_user (
    user_id int,            -- Partition Key
    is_pinned boolean,      -- Clustering Key 1
    last_message_at timestamp, -- Clustering Key 2
    room_id uuid,           -- Clustering Key 3
    
    -- --- DENORMALIZED DATA FROM room_details ---
    room_name text,
    description text,
    room_image text,
    room_type text,
    encryption_data text,
    created_at timestamp,
    updated_at timestamp,
    join_all_user boolean,
    send_message boolean,
    add_member boolean,
    edit_group boolean,

    -- --- DENORMALIZED DATA FROM LAST MESSAGE ---
    last_message_id timeuuid,
    last_message_preview text,
    last_message_type text,
    last_message_sender_id int,
    last_message_sender_name text,
    last_message_sender_phone text,
    last_message_mime_type text,
    last_message_file_url text,
    last_message_file_thumbnail_url text,
    last_message_status int,
    last_message_event text,
    last_message_updated_at timestamp,

    -- --- MEMBERSHIP DATA ---
    is_muted boolean,
    role text,

    PRIMARY KEY ((user_id), is_pinned, last_message_at, room_id)
) WITH CLUSTERING ORDER BY (is_pinned DESC, last_message_at DESC, room_id DESC);


-- Table dedicated exclusively to unread counters.
CREATE TABLE IF NOT EXISTS room_counters_by_user (
    user_id int,            -- Partition Key
    room_id uuid,           -- Clustering Key
    unread_count counter,   -- The unread message counter
    PRIMARY KEY ((user_id), room_id)
);


-- Table to store static room details.
-- OPTIMIZED FOR: GetRoom (get information that doesn't change with each message).
CREATE TABLE IF NOT EXISTS room_details (
    room_id uuid PRIMARY KEY, -- Partition Key: Direct search by room ID.
    name text,
    description text,
    image text,
    type text,
    encryption_data text,
    created_at timestamp,
    updated_at timestamp,
    join_all_user boolean,
    send_message boolean,
    add_member boolean,
    edit_group boolean
);


-- Table to store members of each room.
-- OPTIMIZED FOR: GetRoomParticipants, IsPartnerMuted.
CREATE TABLE IF NOT EXISTS participants_by_room (
    room_id uuid,           -- Partition Key: Groups all members of a room.
    user_id int,            -- Clustering Key: Uniquely identifies the member within the room.
    role text,
    joined_at timestamp,
    is_muted boolean,       -- For user mute logic in the room
    is_partner_blocked boolean, -- For blocking logic in p2p chats.
    PRIMARY KEY ((room_id), user_id)
);


-- #############################################################################
-- # Metadata and Reverse Lookup Tables
-- #############################################################################

-- Table to search for existing P2P rooms and avoid duplicates.
CREATE TABLE IF NOT EXISTS p2p_room_by_users (
    user1_id int, -- Partition key (always the lower ID)
    user2_id int, -- Clustering key (always the higher ID)
    room_id uuid,
    PRIMARY KEY ((user1_id), user2_id)
);

-- Table for reactions to a message.
-- OPTIMIZED FOR: GetMessageReactions.
CREATE TABLE IF NOT EXISTS reactions_by_message (
    message_id timeuuid,    -- Partition Key: Groups all reactions to a message.
    user_id int,            -- Clustering Key: Identifies which user reacted.
    reaction text,
    created_at timestamp,
    PRIMARY KEY ((message_id), user_id)
);


-- Table for read receipts of a message.
-- OPTIMIZED FOR: GetMessageRead.
CREATE TABLE IF NOT EXISTS read_receipts_by_message (
    message_id timeuuid,    -- Partition Key: Groups all readers of a message.
    user_id int,            -- Clustering Key: Identifies which user read the message.
    read_at timestamp,
    PRIMARY KEY ((message_id), user_id)
);


-- Reverse lookup table to find a message by its client ID.
-- OPTIMIZED FOR: GetSenderMessage.
CREATE TABLE IF NOT EXISTS message_by_sender_message_id (
    sender_message_id text PRIMARY KEY, -- Partition Key: Direct search by client ID.
    room_id uuid,
    message_id timeuuid
);


-- Reverse lookup table to find a message's room by its ID.
-- OPTIMIZED FOR: GetMessage, UpdateMessage, DeleteMessage.
CREATE TABLE IF NOT EXISTS room_by_message (
    message_id timeuuid PRIMARY KEY, -- Partition Key: Direct search by message ID.
    room_id uuid
);

-- Lookup table to find the clustering key of a member in a room.
-- OPTIMIZED FOR: Avoid ALLOW FILTERING when updating/deleting from rooms_by_user.
CREATE TABLE IF NOT EXISTS room_membership_lookup (
    user_id int,
    room_id uuid,
    is_pinned boolean,
    last_message_at timestamp,
    role text,              -- Added: user role in the room
    PRIMARY KEY ((user_id), room_id)
);

CREATE TABLE IF NOT EXISTS associated_rooms_by_broadcast (
    broadcast_room_id uuid,      -- Partition Key: The ID of the 'broadcast' type room.
    associated_room_id uuid,     -- Clustering Key: The ID of the associated room (p2p or group).
    
    associated_room_name text,
    associated_room_description text,
    associated_room_photo_url text,

    PRIMARY KEY ((broadcast_room_id), associated_room_id)
);

-- Table to record room/member deletions for synchronization.
-- OPTIMIZED FOR: GetRoomListDeleted.
CREATE TABLE IF NOT EXISTS deleted_rooms_by_user (
    user_id int,            -- Partition Key: Groups all deletions for a user.
    deleted_at timestamp,    -- Clustering Key 1: Orders deletions chronologically.
    room_id uuid,           -- Clustering Key 2: Ensures uniqueness.
    reason text,            -- 'deleted' (room deleted) or 'removed' (user removed from room)
    PRIMARY KEY ((user_id), deleted_at, room_id)
) WITH CLUSTERING ORDER BY (deleted_at DESC);

-- Table to track the status of each message for each user.
-- OPTIMIZED FOR: Get message status without affecting other users.
CREATE TABLE IF NOT EXISTS message_status_by_user (
    user_id int,
    room_id uuid,
    message_id timeuuid,
    status int, -- 0:UNSPECIFIED, 1:SENDING, 2:SENT, 3:DELIVERED, 4:READ, 5:ERROR
    PRIMARY KEY ((user_id, room_id), message_id)
) WITH CLUSTERING ORDER BY (message_id DESC);

CREATE TABLE IF NOT EXISTS users (
    user_id int PRIMARY KEY,
    name text,
    phone text,
    email text,
    avatar text,
    role_id int,
    is_bot boolean,
    created_at timestamp,
    updated_at timestamp
);

-- Lookup table to get all user IDs in a scalable way.
-- Responds to: GetAllUserIDs (for join_all_user functionality)
-- Partitioned into "buckets" to allow parallel scans instead of a full table scan.
CREATE TABLE IF NOT EXISTS all_user_ids (
    bucket int,          -- Partition key: a fixed number of buckets (e.g., 256)
    user_id int,         -- Clustering key: the user ID
    PRIMARY KEY (bucket, user_id)
);

-- Poll Tables

-- Table for votes per message.
-- OPTIMIZED FOR:
--  - Verify existing user votes: SELECT option_id FROM room_message_poll_vote WHERE message_id = ? AND user_id = ?
--  - List message votes (simple time pagination): SELECT user_id, created_at, option_id FROM room_message_poll_vote WHERE message_id = ? [LIMIT ?]
--  - Atomically delete/insert votes per user/option.
CREATE TABLE IF NOT EXISTS room_message_poll_vote (
    message_id timeuuid,   -- Partition Key: groups all votes for a poll (message)
    user_id int,           -- Clustering Key 1: allows querying and deleting by user
    option_id uuid,        -- Clustering Key 2: allows toggling by option
    room_id uuid,
    created_at timestamp,
    updated_at timestamp,
    PRIMARY KEY ((message_id), user_id, option_id)
) WITH CLUSTERING ORDER BY (user_id ASC, option_id ASC);

-- Secondary index to filter by option within a message when results per option are required.
-- Allows queries like: SELECT ... FROM room_message_poll_vote WHERE message_id = ? AND option_id = ?
CREATE INDEX IF NOT EXISTS room_message_poll_vote_option_idx ON room_message_poll_vote (option_id);


-- Table for counters per option in a poll.
-- OPTIMIZED FOR: increment/decrement counters concurrently without expensive aggregate reads.
CREATE TABLE IF NOT EXISTS poll_option_counters_by_message (
    message_id timeuuid,   -- Partition Key
    option_id uuid,        -- Clustering Key
    votes counter,         -- Vote counter for the option
    PRIMARY KEY ((message_id), option_id)
);

-- Table for poll options per message.
-- OPTIMIZED FOR: store available options for each poll.
CREATE TABLE IF NOT EXISTS poll_options_by_message (
    message_id timeuuid,   -- Partition Key: groups all options for a poll
    option_id uuid,        -- Clustering Key: identifies each option within the poll
    room_id uuid,
    option_text text,      -- The option text
    PRIMARY KEY ((message_id), option_id)
) WITH CLUSTERING ORDER BY (option_id ASC);


-- #############################################################################
-- # Chatbot Tables
-- #############################################################################

-- Table for bot conversations (active).
-- OPTIMIZED FOR:
--  - Create conversation: INSERT INTO bot_conversation (id, user_id, created_at, updated_at) VALUES (?, ?, ?, ?)
--  - Get conversation: SELECT id FROM bot_conversation WHERE user_id = ? AND id = ?
--  - List active conversations: SELECT id FROM bot_conversation WHERE user_id = ?
-- NOTE: When deleting, move to bot_conversation_deleted and delete from this table.
CREATE TABLE IF NOT EXISTS bot_conversation (
    user_id int,            -- Partition Key: Groups all conversations for a user
    id timeuuid,            -- Clustering Key: Identifies each conversation and allows chronological ordering
    last_message text,
    last_message_created_at timestamp,
    last_message_sender text, 
    created_at timestamp,
    updated_at timestamp,
    PRIMARY KEY ((user_id), id)
) WITH CLUSTERING ORDER BY (id DESC);


-- Table for bot conversations (deleted).
-- OPTIMIZED FOR:
--  - List deleted conversations: SELECT id, deleted_at FROM bot_conversation_deleted WHERE user_id = ?
--  - Get deleted conversation: SELECT id FROM bot_conversation_deleted WHERE user_id = ? AND id = ?
--  - Restore conversation: move from bot_conversation_deleted to bot_conversation
CREATE TABLE IF NOT EXISTS bot_conversation_deleted (
    user_id int,            -- Partition Key: Groups all deleted conversations for a user
    id timeuuid,            -- Clustering Key: Identifies each conversation
    created_at timestamp,
    updated_at timestamp,
    deleted_at timestamp,   -- Timestamp of when it was deleted
    PRIMARY KEY ((user_id), id)
) WITH CLUSTERING ORDER BY (id DESC);


-- Table for bot messages organized by conversation.
-- OPTIMIZED FOR:
--  - Add message: INSERT INTO bot_message (id, conversation_id, message, sender, reply_to, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?)
--  - Get messages from a conversation: SELECT id, message, sender, reply_to FROM bot_message WHERE conversation_id = ?
--  - Get messages from multiple conversations: SELECT id, message, sender, reply_to FROM bot_message WHERE conversation_id IN ?
CREATE TABLE IF NOT EXISTS bot_message (
    conversation_id timeuuid, -- Partition Key: Groups all messages from a conversation
    id timeuuid,              -- Clustering Key: Identifies each message and allows chronological ordering
    message text,             -- Message content
    sender text,              -- Sender identifier ('user' or 'bot')
    reply_to text,            -- ID of the message it replies to (user question when it's a bot response)
    created_at timestamp,
    updated_at timestamp,
    PRIMARY KEY ((conversation_id), id)
) WITH CLUSTERING ORDER BY (id DESC);

-- Table to store message mentions.
-- OPTIMIZED FOR: GetMessage (enrich a message with its mentions).
CREATE TABLE IF NOT EXISTS mentions_by_message (
    message_id timeuuid,    -- Partition Key: Groups all mentions for a message.
    user_id int,            -- Clustering Key: Identifies the mentioned user.
    tag text,               -- The mention text (e.g., "@juan").
    created_at timestamp,
    PRIMARY KEY ((message_id), user_id)
);

CREATE TABLE IF NOT EXISTS rooms_by_mongo (
    mongo_id text PRIMARY KEY,
    room_id uuid
);

CREATE TABLE IF NOT EXISTS role (
    id int PRIMARY KEY,
    name text,
    contacts_hide boolean
);

CREATE TABLE IF NOT EXISTS organizations (
    room_id uuid PRIMARY KEY
);

-- #############################################################################
-- # Meets Tables
-- #############################################################################


CREATE TABLE IF NOT EXISTS meets (
    meet_id text PRIMARY KEY, 
    title text,
    created_by_user int,
    created_at timestamp,
    updated_at timestamp,
    finished_at timestamp,
    session_key text,
    is_protected_with_key boolean
);


-- Table for meets by user.
CREATE TABLE IF NOT EXISTS meets_by_user (
    user_id int,         
    meet_id text,   
    role text,      
    joined_at timestamp,
    finished_at timestamp,
    PRIMARY KEY ((user_id), meet_id)
) WITH CLUSTERING ORDER BY (meet_id DESC);

