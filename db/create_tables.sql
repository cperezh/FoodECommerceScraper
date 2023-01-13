-- Table: public.producto_dim

 DROP TABLE IF EXISTS public.producto_dim;

CREATE TABLE IF NOT EXISTS public.producto_dim
(
    product character(200) COLLATE pg_catalog."default",
    brand character(200) COLLATE pg_catalog."default",
    categories character(200) COLLATE pg_catalog."default",
    id_producto integer NOT NULL DEFAULT nextval('producto_dim_id_producto_seq'::regclass),
    ts_load timestamp without time zone,
    product_id character(10) COLLATE pg_catalog."default",
	date date,
    categoria character(200) COLLATE pg_catalog."default",
    CONSTRAINT producto_dim_pkey PRIMARY KEY (id_producto)
)
WITH (
    OIDS = FALSE
)
TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.producto_dim
    OWNER to postgres;

-- Table: public.date_dim

DROP TABLE IF EXISTS public."date_dim";

CREATE TABLE public.date_dim
(
    id_date serial NOT NULL,
    date date,
	ts_load timestamp with time zone,
    PRIMARY KEY (id_date)
)
WITH (
    OIDS = FALSE
);

ALTER TABLE IF EXISTS public.date_dim
    OWNER to postgres;

-- Table: public.producto_dia_fact

DROP TABLE IF EXISTS public."producto_dia_fact";

CREATE TABLE IF NOT EXISTS public."producto_dia_fact"
(
   id_producto integer,
    id_date integer,
    price double precision,
    unit_price double precision,
    units integer,
    discount double precision,
    ts_load timestamp without time zone
)
WITH (
    OIDS = FALSE
)
TABLESPACE pg_default;

ALTER TABLE IF EXISTS public."producto_dia_fact"
    OWNER to postgres;