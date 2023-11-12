users_insert = \
    """
    INSERT INTO 
    public."user_airflow"("phoneNumber")
    VALUES(%(agentPhoneNumber)s)
    ON CONFLICT ("phoneNumber")
    DO NOTHING
    """

transaction_insert = \
    """
    INSERT INTO 
    public.transaction_airflow("rowId", "mobile", "category", "userUuid", "balance", "commission",
                       "amount", "requestTimestamp", "updateTimestamp", "externalId")
    VALUES(%(rowId)s, %(receiverPhoneNumber)s, %(transactionType)s, %(userUuid)s, %(balance)s, %(commission)s, 
           %(amount)s, %(requestTimestamp)s, %(updateTimestamp)s, %(externalId)s)
    ON CONFLICT ("rowId")
    DO NOTHING
    """
