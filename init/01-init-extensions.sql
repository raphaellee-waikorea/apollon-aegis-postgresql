-- Runs once, on first initialization of the data directory, against the
-- database named by POSTGRES_DB (default: apollon).

-- Enable pgvector so embedding columns (vector(N)) can be used directly
-- from this database, mirroring the vector-data handling in the
-- apollon-aegis stack.
CREATE EXTENSION IF NOT EXISTS vector;
