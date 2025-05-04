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
ALTER TABLE
    "security" ADD CONSTRAINT "security_security_type_foreign" FOREIGN KEY("security_type") REFERENCES "security_type"("id");
ALTER TABLE
    "security" ADD CONSTRAINT "security_market_id_foreign" FOREIGN KEY("market_id") REFERENCES "market"("id");