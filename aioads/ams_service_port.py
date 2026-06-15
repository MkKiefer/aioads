from enum import IntEnum


class AmsServicePort(IntEnum):
    # --- Core / Router ---
    ADS_ROUTER = 1
    AMS_DEBUGGER = 2
    LICENSE_SERVER = 30

    # --- Logging / Events ---
    LOGGER = 100
    EVENT_LOGGER = 110
    EVENT_LOGGER_USER_V2 = 130
    EVENT_LOGGER_RT_V2 = 131
    EVENT_LOGGER_PUBLISHER_V2 = 132

    # --- System / Core Services ---
    SYSTEM_SERVICE = 10000
    TCPIP_SERVER = 10201
    SYSTEM_MANAGER = 10300
    SMS_SERVER = 10400
    MODBUS_SERVER = 10500
    AMS_LOGGER = 10502
    XML_DATA_SERVER = 10600
    AUTO_CONFIGURATION = 10700
    PLC_CONTROL = 10800
    FTP_CLIENT = 10900

    # --- NC / Motion / Automation ---
    NC_CONTROL = 11000
    NC_INTERPRETER = 11500
    GST_INTERPRETER = 11600
    TRACK_CONTROL = 12000
    CAM_CONTROL = 13000

    # --- Monitoring / Diagnostics ---
    SCOPE_SERVER = 14000
    CONDITION_MONITORING = 14100

    # --- Communication / Integration ---
    CONTROL_NET = 16000
    OPC_SERVER = 17000
    OPC_CLIENT = 17500
    MAIL_SERVER = 18000

    # --- Management / Infrastructure ---
    MGMT_SERVER = 19100
    HMI_SERVER = 19800
    DATABASE_SERVER = 21372

    # --- PLC Runtime (TwinCAT 2) ---
    TC2_PLC = 800
    TC2_RUNTIME_1 = 801
    TC2_RUNTIME_2 = 811
    TC2_RUNTIME_3 = 821
    TC2_RUNTIME_4 = 831

    # --- PLC Runtime (TwinCAT 3) ---
    TC3_PLC_BASE = 850
    TC3_RUNTIME_1 = 851
    TC3_RUNTIME_2 = 852
    TC3_RUNTIME_3 = 853
    TC3_RUNTIME_4 = 854

    # --- Real-time / Ring 0 ---
    R0_REALTIME = 200
    R0_TRACE = 290
    R0_IO = 300
    R0_PLC_LEGACY = 400
    R0_NC = 500
    R0_NC_SAF = 501
    NC_INSTANCE = 520
    R0_CNC = 600
    R0_LINE = 700

    # --- Misc / Optional services ---
    CAM_CONTROLLER = 900
    CAM_TOOL = 950
    FTP = 10900  # alias
