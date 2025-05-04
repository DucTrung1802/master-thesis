CREATE TABLE "market"(
    "id" INTEGER NOT NULL,
    "symbol" VARCHAR(255) NOT NULL,
    "name" VARCHAR(255) NOT NULL,
    "create_date" TIMESTAMP(0) WITHOUT TIME ZONE NOT NULL,
    "update_date" TIMESTAMP(0) WITHOUT TIME ZONE NULL,
    "delete_date" TIMESTAMP(0) WITHOUT TIME ZONE NULL
);
ALTER TABLE
    "market" ADD PRIMARY KEY("id");
CREATE TABLE "security"(
    "id" INTEGER NOT NULL,
    "symbol" VARCHAR(255) NOT NULL,
    "name" VARCHAR(255) NOT NULL,
    "issued_shares" BIGINT NOT NULL DEFAULT '0',
    "outstanding_shares" BIGINT NOT NULL DEFAULT '0',
    "outstanding_rate" DECIMAL(8, 2) NOT NULL DEFAULT '0',
    "market_cap" BIGINT NOT NULL DEFAULT '0',
    "market_id" INTEGER NOT NULL,
    "security_type" INTEGER NOT NULL,
    "create_date" TIMESTAMP(0) WITHOUT TIME ZONE NOT NULL,
    "update_date" TIMESTAMP(0) WITHOUT TIME ZONE NULL,
    "delete_date" TIMESTAMP(0) WITHOUT TIME ZONE NULL
);
ALTER TABLE
    "security" ADD PRIMARY KEY("id");
COMMENT
ON COLUMN
    "security"."outstanding_rate" IS '= outstanding_shares / issued_shares';
CREATE TABLE "security_type"(
    "id" INTEGER NOT NULL,
    "symbol" VARCHAR(255) NOT NULL,
    "name" VARCHAR(255) NOT NULL,
    "create_date" TIMESTAMP(0) WITHOUT TIME ZONE NOT NULL,
    "update_date" TIMESTAMP(0) WITHOUT TIME ZONE NULL,
    "delete_date" TIMESTAMP(0) WITHOUT TIME ZONE NULL
);
ALTER TABLE
    "security_type" ADD PRIMARY KEY("id");
CREATE TABLE "daily_price"(
    "date" DATE NOT NULL,
    "symbol" VARCHAR(255) NOT NULL,
    "market_id" INTEGER NOT NULL,
    "open" DECIMAL(8, 2) NOT NULL,
    "high" DECIMAL(8, 2) NOT NULL,
    "low" DECIMAL(8, 2) NOT NULL,
    "close" DECIMAL(8, 2) NOT NULL,
    "volume" BIGINT NOT NULL
);
ALTER TABLE
    "daily_price" ADD PRIMARY KEY("date");
CREATE TABLE "gdp"(
    "year" INTEGER NOT NULL,
    "quarter" INTEGER NOT NULL,
    "agriculture_share" DECIMAL(8, 2) NOT NULL,
    "industry_share" DECIMAL(8, 2) NOT NULL,
    "service_share" DECIMAL(8, 2) NOT NULL,
    "gdp_true_growth_acc" DECIMAL(8, 2) NOT NULL,
    "agriculture_true_growth_acc" DECIMAL(8, 2) NOT NULL,
    "industry_true_growth_acc" DECIMAL(8, 2) NOT NULL,
    "service_true_growth_acc" DECIMAL(8, 2) NOT NULL
);
ALTER TABLE
    "gdp" ADD PRIMARY KEY("year");
ALTER TABLE
    "gdp" ADD PRIMARY KEY("quarter");
ALTER TABLE
    "daily_price" ADD CONSTRAINT "daily_price_market_id_foreign" FOREIGN KEY("market_id") REFERENCES "market"("id");
ALTER TABLE
    "security" ADD CONSTRAINT "security_security_type_foreign" FOREIGN KEY("security_type") REFERENCES "security_type"("id");
ALTER TABLE
    "daily_price" ADD CONSTRAINT "daily_price_symbol_foreign" FOREIGN KEY("symbol") REFERENCES "security"("symbol");
ALTER TABLE
    "security" ADD CONSTRAINT "security_market_id_foreign" FOREIGN KEY("market_id") REFERENCES "market"("id");