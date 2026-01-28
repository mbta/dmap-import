CUBIC_ODS_TABLES = [
    # WA160
    #    "EDW.DATE_DIMENSION", # Temporarily disabled, as not part of a view
    "EDW.FARE_PROD_USERS_LIST_DIMENSION",
    "EDW.FARE_PRODUCT_DIMENSION",
    "EDW.MEDIA_TYPE_DIMENSION",
    "EDW.OPERATOR_DIMENSION",
    "EDW.RIDE_TYPE_DIMENSION",
    "EDW.ROUTE_DIMENSION",
    #    "EDW.STOP_POINT_DIMENSION", # Temporarily disabled, as not part of a view
    "EDW.TRANSIT_ACCOUNT_DIMENSION",
    "EDW.TXN_STATUS_DIMENSION",
    "EDW.CARD_DIMENSION",
    #    "EDW.DEVICE_DIMENSION", # Temporarily disabled, as not part of a view
    "EDW.USE_TRANSACTION",
    # COMP B
    "EDW.TXN_CHANNEL_MAP",
    "EDW.CCH_AFC_TRANSACTION",
    #    "EDW.PATRON_TRIP", # Temporarily disabled, as not part of a view
    #    "EDW.TRIP_PAYMENT", # Temporarily disabled, as not part of a view
    #    "EDW.SALE_TRANSACTION", # Temporarily disabled, as not part of a view
    "EDW.PAYMENT_TYPE_DIMENSION",
    # "EDW.TRANSACTION_HISTORY", # table is taking days to load, blocking other tables from loading
    #    "EDW.FARE_REVENUE_REPORT_SCHEDULE", # Temporarily disabled, as not part of a view  # addendum support
    # FMIS
    "EDW.FNP_GENERAL_JRNL_ACCOUNT_ENTRY",
    "EDW.PAYMENT_SUMMARY",
    # KPI
    #    "EDW.DEVICE_LAST_STATE", # Temporarily disabled, as not part of a view
    "EDW.DEVICE_EVENT",  # Not used in view, but Will said it's important
    #    "EDW.EVENT_TYPE_DIMENSION", # Temporarily disabled, as not part of a view
    "EDW.ABP_TAP",
    #    "EDW.KPI_MONTHLY_SLDC", # Temporarily disabled, as not part of a view
    #    "EDW.KPI_SUMMARY_BY_DAY", # Temporarily disabled, as not part of a view
    #    "EDW.KPI_AVAILABILITY_EVENT", # Temporarily disabled, as not part of a view
    #    "EDW.KPI_TARGET", # Temporarily disabled, as not part of a view
    #    "EDW.KPI", # Temporarily disabled, as not part of a view
    #    "EDW.KPI_DETAIL_EVENTS_BY_DAY", # Temporarily disabled, as not part of a view
    #    "EDW.KPI_RULE", # Temporarily disabled, as not part of a view
    #    "EDW.SVN_U_FS_EVENT_CODE", # Temporarily disabled, as not part of a view
    #    "EDW.SVN_U_FS_FAULT_CODES", # Temporarily disabled, as not part of a view
    #    "EDW.SVN_U_FS_FAULTY_ITEMS", # Temporarily disabled, as not part of a view
    #    "EDW.SVN_U_FS_RPIR_CODE_RT_CAUSE_ID", # Temporarily disabled, as not part of a view
    #    "EDW.SVN_U_KPI_LEVEL", # Temporarily disabled, as not part of a view
    #    "EDW.SVN_WM_ORDER", # Temporarily disabled, as not part of a view
    #    "EDW.SVN_WM_TASK", # Temporarily disabled, as not part of a view
    #    "EDW.REVENUE_LOSS_ASSESSMENT", # Temporarily disabled, as not part of a view
    # WC231
    "CCH_STAGE.CATEGORY",
    "EDW.BUSINESS_ENTITY_DIMENSION",
    # "EDW.DISTRIBUTOR_DIMENSION", # no data
    "EDW.SALE_TYPE_DIMENSION",
    "CCH_STAGE.REPROCESS_ACTION",
    "EDW.CREDIT_CARD_TYPE_DIMENSION",
    "EDW.TRANSACTION_ORIGIN_DIMENSION",
    "CCH_STAGE.CATEGORIZATION_RULE",
    "CCH_STAGE.TRANSACTION_TYPE",
    #    "EDW.CHGBK_ACTIVITY_TYPE_DIMENSION", # Temporarily disabled, as not part of a view
    "EDW.RIDER_CLASS_DIMENSION",
    "EDW.PURSE_TYPE_DIMENSION",
    # "EDW.DISTRIBUTOR_ORDER", # no data
    #    "EDW.CASHBOX_EVENT_DIMENSION", # Temporarily disabled, as not part of a view
    #    "EDW.PASS_LIAB_EVENT_TYPE_DIMENSION", # Temporarily disabled, as not part of a view
    # POLICY
    "EDW.MEMBER_DIMENSION",
    # WC700
    "EDW.FAREREV_RECOVERY_TXN",
    "EDW.REASON_DIMENSION",
    # Unprocessed Taps
    #    "EDW.ABP_REPROCESS_LOG", # Temporarily disabled, as not part of a view
    # WC320
    #    "EDW.FRM_SRC_CRDB_ACQUIRER_CHGBK", # Temporarily disabled, as not part of a view
    #    "EDW.TRAVEL_MODE_DIMENSION", # Temporarily disabled, as not part of a view
    #    "EDW.PAL_CONFIRMATION", # Temporarily disabled, as not part of a view
    #    "EDW.JOURNAL_ENTRY", # Temporarily disabled, as not part of a view
    # no association to need indicated
    #    "EDW.DEVICE_END_OF_DAY_MSG_COUNT", # Temporarily disabled, as not part of a view
    #    "EDW.TAP_USAGE_SUMMARY", # Temporarily disabled, as not part of a view
    #    "EDW.VEHICLE_TRIP", # Temporarily disabled, as not part of a view
    #    "EDW.FACILITY_DIMENSION", # Temporarily disabled, as not part of a view
    #    "EDW.KPI_OPERATING_DAY_SCHEDULE", # Temporarily disabled, as not part of a view
    #    "EDW.CITATION", # Temporarily disabled, as not part of a view
    #    "EDW.KPI_AGENCY_MAP", # Temporarily disabled, as not part of a view
    #    "EDW.TOKEN_HISTORY", # Temporarily disabled, as not part of a view
    #    "EDW.FRM_CRDB_RECON_SYSCONF_ACQCONF", # Temporarily disabled, as not part of a view
    #    "EDW.TRANSIT_ACCOUNT_BALANCE", # Temporarily disabled, as not part of a view
    #    "EDW.CUSTOMER_DIMENSION", # Temporarily disabled, as not part of a view
    #    "EDW.PATRON_ORDER", # Temporarily disabled, as not part of a view
    #    "EDW.PATRON_ORDER_LINE_ITEM", # Temporarily disabled, as not part of a view
    #    "EDW.PATRON_ORDER_PAYMENT", # Temporarily disabled, as not part of a view
    #    "EDW.READ_TRANSACTION", # Temporarily disabled, as not part of a view
    #    "EDW.SALE_TXN_PAYMENT", # Temporarily disabled, as not part of a view
    #    "EDW.SERVICE_TYPE_DIMENSION", # Temporarily disabled, as not part of a view
    #    "EDW.SVN_INCIDENT", # Temporarily disabled, as not part of a view
    #    "EDW.UNSETTLED_CCA_CASH_COUNT", # Temporarily disabled, as not part of a view
    #    "EDW.UNSETTLED_CRDB_ACQ_CONF", # Temporarily disabled, as not part of a view
    #    "EDW.UNSETTLED_CRDB_CHGBK", # Temporarily disabled, as not part of a view
    #    "EDW.UNSETTLED_DEVICE_CASH_STC", # Temporarily disabled, as not part of a view
    #    "EDW.UNSETTLED_DIST_ORDER", # Temporarily disabled, as not part of a view
    #    "EDW.UNSETTLED_MISC", # Temporarily disabled, as not part of a view
    #    "EDW.UNSETTLED_PATRON_ORDER", # Temporarily disabled, as not part of a view
    #    "EDW.UNSETTLED_SALE", # Temporarily disabled, as not part of a view
    #    "EDW.UNSETTLED_USE", # Temporarily disabled, as not part of a view
    #    "EDW.FRM_CRDB_CHGBK_ACTIVITY", # Temporarily disabled, as not part of a view
    #    "EDW.FRM_CRDB_CHGBK_CASE", # Temporarily disabled, as not part of a view
    #    "EDW.FRM_CRDB_CHGBK_MASTER", # Temporarily disabled, as not part of a view
    #    "EDW.BE_INVOICE_STATUS_DIMENSION", # Temporarily disabled, as not part of a view
    #    "EDW.BNFT_INVOICE_STATUS_DIMENSION", # Temporarily disabled, as not part of a view
    "EDW.SVN_TASK",
]
