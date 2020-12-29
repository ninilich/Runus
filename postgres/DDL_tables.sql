CREATE TABLE public.stg_news (
	news_datetime timestamptz NULL,
	news_src bpchar(3) NULL,
	news_uid varchar(255) NULL,
	entity_nm varchar(255) NULL,
	entity_type bpchar(3) NULL,
	load_date timestamp NULL DEFAULT now()
);

CREATE TABLE public.ref_synonyms (
	synonim_entity_nm varchar(50) NOT NULL,
	entity_nm varchar(50) NOT NULL,
	load_time timestamp NOT NULL DEFAULT now(),
	CONSTRAINT ref_synonyms_pkey PRIMARY KEY (entity_nm, synonim_entity_nm)
);

CREATE TABLE public.ref_news_src (
	news_src_id bpchar(3) NOT NULL,
	news_src_name varchar(25) NOT NULL,
	CONSTRAINT ref_news_src_pkey PRIMARY KEY (news_src_id)
);

CREATE TABLE public.ref_exeptions (
	entity_name varchar(50) NOT NULL,
	load_time timestamp NOT NULL DEFAULT now(),
	entity_type_id bpchar(3) NULL,
	CONSTRAINT ref_exeptions_pkey PRIMARY KEY (entity_name)
);

CREATE TABLE public.ref_entity_type (
	entity_type_id bpchar(3) NOT NULL,
	entity_type_name varchar(20) NOT NULL,
	CONSTRAINT ref_entity_type_pkey PRIMARY KEY (entity_type_id)
);

CREATE TABLE public.news (
	news_id serial NOT NULL,
	news_date date NOT NULL,
	load_time timestamp NOT NULL DEFAULT now(),
	news_src_id bpchar(3) NOT NULL,
	news_external_uid varchar NULL,
	CONSTRAINT news_pkey PRIMARY KEY (news_id),
	CONSTRAINT news_news_src_id_fkey FOREIGN KEY (news_src_id) REFERENCES ref_news_src(news_src_id)
);
CREATE INDEX idx_news_date ON public.news USING btree (news_date);
CREATE INDEX idx_news_external_id ON public.news USING btree (news_external_uid);
CREATE INDEX news_date_idx ON public.news USING btree (news_date);

CREATE TABLE public.entity (
	entity_id serial NOT NULL,
	entity_name varchar(50) NULL,
	load_time timestamp NOT NULL DEFAULT now(),
	entity_type_id bpchar(3) NULL,
	CONSTRAINT entity_pkey PRIMARY KEY (entity_id),
	CONSTRAINT entity_enity_type_id_fkey FOREIGN KEY (entity_type_id) REFERENCES ref_entity_type(entity_type_id)
);
CREATE INDEX idx_entity_name ON public.entity USING btree (entity_name);


CREATE TABLE public.news_x_entity (
	news_id int4 NOT NULL,
	entity_id int4 NOT NULL,
	load_time timestamp NOT NULL DEFAULT now(),
	CONSTRAINT news_x_entity_pkey PRIMARY KEY (news_id, entity_id),
	CONSTRAINT news_x_entity_entity_id_fkey FOREIGN KEY (entity_id) REFERENCES entity(entity_id),
	CONSTRAINT news_x_entity_news_id_fkey FOREIGN KEY (news_id) REFERENCES news(news_id)
);

CREATE TABLE public.dm_mentions (
	entity_id int4 NOT NULL,
	news_date date NOT NULL,
	news_src_id bpchar(3) NOT NULL,
	mention_qnt numeric(18) NULL,
	load_time timestamp NOT NULL DEFAULT now(),
	entity_type_id bpchar(3) NOT NULL,
	CONSTRAINT dm_mentions_pkey PRIMARY KEY (entity_id, news_date, news_src_id, entity_type_id),
	CONSTRAINT dm_mentions_enity_type_id_fkey FOREIGN KEY (entity_type_id) REFERENCES ref_entity_type(entity_type_id),
	CONSTRAINT dm_mentions_entity_id_fkey FOREIGN KEY (entity_id) REFERENCES entity(entity_id),
	CONSTRAINT dm_mentions_news_src_id_fkey FOREIGN KEY (news_src_id) REFERENCES ref_news_src(news_src_id)
);
CREATE INDEX xif1dm_mentions ON public.dm_mentions USING btree (news_date);


CREATE TABLE public.dm_mentions_stat (
	actual_date date NOT NULL,
	news_src_id bpchar(3) NOT NULL,
	entity_type_id bpchar(3) NOT NULL,
	entity_id int4 NOT NULL,
	daily_rank int4 NULL,
	daily_qnt int4 NULL,
	weekly_rank int4 NULL,
	weekly_qnt int4 NULL,
	monthly_rank int4 NULL,
	monthly_qnt int4 NULL,
	load_time timestamp NOT NULL DEFAULT now(),
	CONSTRAINT dm_mentions_stat_pkey PRIMARY KEY (actual_date, entity_id, news_src_id, entity_type_id),
	CONSTRAINT dm_mentions_stat_enity_type_id_fkey FOREIGN KEY (entity_type_id) REFERENCES ref_entity_type(entity_type_id),
	CONSTRAINT dm_mentions_stat_entity_id_fkey FOREIGN KEY (entity_id) REFERENCES entity(entity_id),
	CONSTRAINT dm_mentions_stat_news_src_id_fkey FOREIGN KEY (news_src_id) REFERENCES ref_news_src(news_src_id)
);
