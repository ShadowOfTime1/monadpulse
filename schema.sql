-- MonadPulse database schema
-- Generated from pg_dump --schema-only on the live DB.
-- Regenerate with:
--   sudo -u postgres pg_dump --schema-only --no-owner --no-privileges monadpulse > schema.sql
--
-- Disaster recovery:
--   createdb monadpulse
--   psql monadpulse < schema.sql
--
--
-- PostgreSQL database dump
--


-- Dumped from database version 16.13 (Ubuntu 16.13-0ubuntu0.24.04.1)
-- Dumped by pg_dump version 16.13 (Ubuntu 16.13-0ubuntu0.24.04.1)

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: alerts; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.alerts (
    id integer NOT NULL,
    "timestamp" timestamp with time zone DEFAULT now() NOT NULL,
    alert_type text NOT NULL,
    severity text DEFAULT 'info'::text NOT NULL,
    title text NOT NULL,
    description text,
    data_json jsonb,
    network text DEFAULT 'testnet'::text NOT NULL
);


--
-- Name: alerts_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.alerts_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: alerts_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.alerts_id_seq OWNED BY public.alerts.id;


--
-- Name: blocks; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.blocks (
    block_number bigint NOT NULL,
    "timestamp" timestamp with time zone NOT NULL,
    proposer_address text NOT NULL,
    tx_count integer DEFAULT 0 NOT NULL,
    gas_used bigint DEFAULT 0 NOT NULL,
    base_fee bigint,
    block_time_ms integer,
    network text DEFAULT 'testnet'::text NOT NULL
);


--
-- Name: client_versions; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.client_versions (
    version text NOT NULL,
    release_date timestamp with time zone,
    description text,
    is_mandatory boolean DEFAULT false
);


--
-- Name: collector_state; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.collector_state (
    key text NOT NULL,
    value text NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    network text DEFAULT 'testnet'::text NOT NULL
);


--
-- Name: epoch_validators; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.epoch_validators (
    epoch_number integer NOT NULL,
    validator_id text NOT NULL,
    stake numeric DEFAULT 0 NOT NULL,
    commission integer DEFAULT 0 NOT NULL,
    status text DEFAULT 'stayed'::text NOT NULL,
    network text DEFAULT 'testnet'::text NOT NULL
);


--
-- Name: epochs; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.epochs (
    epoch_number integer NOT NULL,
    boundary_block bigint NOT NULL,
    "timestamp" timestamp with time zone NOT NULL,
    validator_count integer DEFAULT 0 NOT NULL,
    network text DEFAULT 'testnet'::text NOT NULL
);


--
-- Name: health_scores; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.health_scores (
    id integer NOT NULL,
    validator_id text NOT NULL,
    "timestamp" timestamp with time zone DEFAULT now() NOT NULL,
    total_score numeric DEFAULT 0 NOT NULL,
    uptime_score numeric DEFAULT 0 NOT NULL,
    miss_score numeric DEFAULT 0 NOT NULL,
    upgrade_score numeric DEFAULT 0 NOT NULL,
    stake_score numeric DEFAULT 0 NOT NULL,
    age_score numeric DEFAULT 0 NOT NULL,
    network text DEFAULT 'testnet'::text NOT NULL
);


--
-- Name: health_scores_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.health_scores_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: health_scores_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.health_scores_id_seq OWNED BY public.health_scores.id;


--
-- Name: hourly_gas_stats; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.hourly_gas_stats (
    hour_timestamp timestamp with time zone NOT NULL,
    avg_gas numeric DEFAULT 0 NOT NULL,
    total_gas numeric DEFAULT 0 NOT NULL,
    tx_count integer DEFAULT 0 NOT NULL,
    avg_base_fee numeric DEFAULT 0 NOT NULL,
    burned_mon numeric DEFAULT 0 NOT NULL,
    network text DEFAULT 'testnet'::text NOT NULL
);


--
-- Name: stake_events; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.stake_events (
    id integer NOT NULL,
    block_number bigint NOT NULL,
    "timestamp" timestamp with time zone NOT NULL,
    event_type text NOT NULL,
    validator_id text NOT NULL,
    delegator text NOT NULL,
    amount numeric DEFAULT 0 NOT NULL,
    network text DEFAULT 'testnet'::text NOT NULL,
    tx_hash text,
    log_index integer
);


--
-- Name: stake_events_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.stake_events_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: stake_events_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.stake_events_id_seq OWNED BY public.stake_events.id;


--
-- Name: top_contracts; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.top_contracts (
    contract_address text NOT NULL,
    total_gas_used bigint DEFAULT 0 NOT NULL,
    tx_count integer DEFAULT 0 NOT NULL,
    first_seen timestamp with time zone
);


--
-- Name: upgrade_events; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.upgrade_events (
    id integer NOT NULL,
    "timestamp" timestamp with time zone NOT NULL,
    from_version text,
    to_version text
);


--
-- Name: upgrade_events_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.upgrade_events_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: upgrade_events_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.upgrade_events_id_seq OWNED BY public.upgrade_events.id;


--
-- Name: validator_geo; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.validator_geo (
    validator_id text NOT NULL,
    name text,
    country text,
    city text,
    lat double precision,
    lon double precision,
    provider text,
    network text DEFAULT 'testnet'::text NOT NULL
);


--
-- Name: validator_stake_history; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.validator_stake_history (
    validator_id text NOT NULL,
    epoch integer NOT NULL,
    total_stake numeric DEFAULT 0 NOT NULL,
    self_stake numeric DEFAULT 0 NOT NULL,
    delegator_count integer DEFAULT 0 NOT NULL,
    network text DEFAULT 'testnet'::text NOT NULL
);


--
-- Name: alerts id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.alerts ALTER COLUMN id SET DEFAULT nextval('public.alerts_id_seq'::regclass);


--
-- Name: health_scores id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.health_scores ALTER COLUMN id SET DEFAULT nextval('public.health_scores_id_seq'::regclass);


--
-- Name: stake_events id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.stake_events ALTER COLUMN id SET DEFAULT nextval('public.stake_events_id_seq'::regclass);


--
-- Name: upgrade_events id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.upgrade_events ALTER COLUMN id SET DEFAULT nextval('public.upgrade_events_id_seq'::regclass);


--
-- Name: alerts alerts_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.alerts
    ADD CONSTRAINT alerts_pkey PRIMARY KEY (id);


--
-- Name: blocks blocks_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.blocks
    ADD CONSTRAINT blocks_pkey PRIMARY KEY (block_number, network);


--
-- Name: client_versions client_versions_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.client_versions
    ADD CONSTRAINT client_versions_pkey PRIMARY KEY (version);


--
-- Name: collector_state collector_state_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.collector_state
    ADD CONSTRAINT collector_state_pkey PRIMARY KEY (key, network);


--
-- Name: epoch_validators epoch_validators_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.epoch_validators
    ADD CONSTRAINT epoch_validators_pkey PRIMARY KEY (epoch_number, validator_id);


--
-- Name: epochs epochs_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.epochs
    ADD CONSTRAINT epochs_pkey PRIMARY KEY (epoch_number, network);


--
-- Name: health_scores health_scores_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.health_scores
    ADD CONSTRAINT health_scores_pkey PRIMARY KEY (id);


--
-- Name: hourly_gas_stats hourly_gas_stats_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.hourly_gas_stats
    ADD CONSTRAINT hourly_gas_stats_pkey PRIMARY KEY (hour_timestamp, network);


--
-- Name: stake_events stake_events_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.stake_events
    ADD CONSTRAINT stake_events_pkey PRIMARY KEY (id);


--
-- Name: top_contracts top_contracts_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.top_contracts
    ADD CONSTRAINT top_contracts_pkey PRIMARY KEY (contract_address);


--
-- Name: upgrade_events upgrade_events_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.upgrade_events
    ADD CONSTRAINT upgrade_events_pkey PRIMARY KEY (id);


--
-- Name: validator_geo validator_geo_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.validator_geo
    ADD CONSTRAINT validator_geo_pkey PRIMARY KEY (validator_id, network);


--
-- Name: validator_stake_history validator_stake_history_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.validator_stake_history
    ADD CONSTRAINT validator_stake_history_pkey PRIMARY KEY (validator_id, epoch);


--
-- Name: idx_alerts_network; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_alerts_network ON public.alerts USING btree (network);


--
-- Name: idx_alerts_time; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_alerts_time ON public.alerts USING btree ("timestamp" DESC);


--
-- Name: idx_alerts_type; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_alerts_type ON public.alerts USING btree (alert_type);


--
-- Name: idx_blocks_network; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_blocks_network ON public.blocks USING btree (network);


--
-- Name: idx_blocks_proposer; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_blocks_proposer ON public.blocks USING btree (proposer_address);


--
-- Name: idx_blocks_timestamp; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_blocks_timestamp ON public.blocks USING btree ("timestamp");


--
-- Name: idx_health_network; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_health_network ON public.health_scores USING btree (network);


--
-- Name: idx_health_validator; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_health_validator ON public.health_scores USING btree (validator_id, "timestamp");


--
-- Name: idx_stake_events_time; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_stake_events_time ON public.stake_events USING btree ("timestamp");


--
-- Name: idx_stake_events_validator; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_stake_events_validator ON public.stake_events USING btree (validator_id);


--
-- Name: stake_events_dedup_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX stake_events_dedup_idx ON public.stake_events USING btree (network, tx_hash, log_index);


--
-- PostgreSQL database dump complete
--


