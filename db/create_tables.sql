-- Table: public.producto_dim

DROP TABLE IF EXISTS public."producto_dim";

CREATE TABLE IF NOT EXISTS public."producto_dim"
(
    id_producto integer NOT NULL DEFAULT nextval('"producto_dim_id_producto_seq"'::regclass),
    product_id integer,
    product character(1) COLLATE pg_catalog."default",
    brand character(1) COLLATE pg_catalog."default",
    categories character(1) COLLATE pg_catalog."default",
    CONSTRAINT "producto_dim_pkey" PRIMARY KEY (id_producto)
)
WITH (
    OIDS = FALSE
)
TABLESPACE pg_default;

ALTER TABLE IF EXISTS public."producto_dim"
    OWNER to postgres;

-- Table: public.date_dim

DROP TABLE IF EXISTS public."date_dim";

CREATE TABLE IF NOT EXISTS public."date_dim"
(
    id_date integer NOT NULL DEFAULT nextval('"date_dim_id_date_seq"'::regclass),
    date date,
    CONSTRAINT "date_dim_pkey" PRIMARY KEY (id_date)
)
WITH (
    OIDS = FALSE
)
TABLESPACE pg_default;

ALTER TABLE IF EXISTS public."date_dim"
    OWNER to postgres;

-- Table: public.producto_dia_fact

DROP TABLE IF EXISTS public."producto_dia_fact";

CREATE TABLE IF NOT EXISTS public."producto_dia_fact"
(
    id_producto integer,
    id_date integer,
    precio double precision,
    unit_price double precision,
    units integer,
    discount double precision
)
WITH (
    OIDS = FALSE
)
TABLESPACE pg_default;

ALTER TABLE IF EXISTS public."producto_dia_fact"
    OWNER to postgres;