create_user_table = \
    """
    CREATE TABLE IF NOT EXISTS public."user"
    (
        uuid uuid NOT NULL DEFAULT uuid_generate_v4(),
        "phoneNumber" character varying,
        "createdAt" timestamp without time zone NOT NULL DEFAULT now(),
        "updatedAt" timestamp without time zone NOT NULL DEFAULT now(),
        "nTransactions" bigint, 
        CONSTRAINT "PK_a95e949168be7b7ece1a2382fed" PRIMARY KEY (uuid),
        CONSTRAINT "UQ_f2578043e491921209f5dadd080" UNIQUE ("phoneNumber")
    );
    """

create_transaction_table = \
    """
    CREATE TABLE IF NOT EXISTS public.transaction
    (
        uuid uuid NOT NULL DEFAULT uuid_generate_v4(),
        mobile character varying, 
        status character varying,
        category character varying,
        "userUuid" uuid NOT NULL,
        balance numeric(20,2),
        commission numeric(20,2),
        amount numeric(20,2),
        "requestTimestamp" bigint NOT NULL,
        "updateTimestamp" bigint NOT NULL,
        source character varying, 
        "externalId" character varying,
        CONSTRAINT "PK_fcce0ce5cc7762e90d2cc7e2307" PRIMARY KEY (uuid),
        CONSTRAINT "FK_00197c2fde23b7c0f6b69d0b6a2" FOREIGN KEY ("userUuid")
            REFERENCES public."user" (uuid) MATCH SIMPLE
            ON UPDATE NO ACTION
            ON DELETE NO ACTION
    );
    """
users_insert = \
    """
    INSERT INTO 
    public."user"("uuid", "phoneNumber", "nTransactions")
    VALUES(%(uuid)s, %(agentPhoneNumber)s, %(nTransactions)s)
    ON CONFLICT ("phoneNumber")
    DO NOTHING
    """

transaction_insert = \
    """
    INSERT INTO 
    public.transaction("mobile", "category", "userUuid", "balance", "commission",
                       "amount", "requestTimestamp", "updateTimestamp", "externalId")
    VALUES(%(receiverPhoneNumber)s, %(transactionType)s, %(userUuid)s, %(balance)s, %(commission)s, 
           %(amount)s, %(requestTimestamp)s, %(updateTimestamp)s, %(externalId)s)
    """
