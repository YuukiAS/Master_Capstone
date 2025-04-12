"""Constants used for database operations and table configurations in the UK Biobank project."""
import datetime


class DatabaseConfig:
    """
    Database configuration constants.

    Attributes:
        CSV_PATH (str): Path to the main UKBB CSV file.
        TOTAL_ROWS (int): Total number of rows in the database.
        USED_ROWS (int): Number of rows actually used in analysis.
        DB_PATH (str): Path to the SQLite database file.
        ECG_FOLDER (str): Path to the ECG folder.
        SAMPLING_RATE (int): Sampling rate of the ECG data.
        CENSOR_DATE (datetime): Cutoff date for data censoring.
    """

    CSV_PATH = "/work/users/y/u/yuukias/BIOS-Material/BIOS992/data/ukbiobank.csv"
    COLUMN_DEFS_PATH = "/work/users/y/u/yuukias/BIOS-Material/BIOS992/data/column_defs.pkl"
    TOTAL_ROWS = 502368
    USED_ROWS = 77888

    # The primary key is eid for all tables
    DB_PATH = "/work/users/y/u/yuukias/BIOS-Material/BIOS992/data/ukbiobank.db"

    ECG_FOLDER = "/users/y/u/yuukias/database/UKBiobank/6025"
    SAMPLING_RATE = 500

    CENSOR_DATE = datetime.datetime(2022, 10, 31)


class TableNames:
    """
    Database table names.

    Attributes:
        VARIABLES (str): Name of the variables table.
        ICD10 (str): Name of the ICD10 codes table.
        PROCESSED (str): Name of the processed data table.
    """
    CONFOUNDERS = "Confounders"  # correspond to CONFOUNDER_COLUMNS_ID
    ECG = "ECG"  # correspond to ECG_COLUMNS_ID
    ICD = "ICD"  # correspond to ICD_COLUMNS_ID

    # HRV indices extracted using neurokit2
    HRV_TIME = "HRV_time"  
    HRV_FREQ = "HRV_freq"
    HRV_POINCARE = "HRV_poincare"
    HRV_ENTROPY = "HRV_entropy"
    HRV_FRACTAL = "HRV_fractal"

    PROCESSED = "Processed"  # processed variables

    COVARIATES = "Covariates"  # covariates that will be used for survival analysis
    STATUS = "Status"  # health status of that will be used for participant selection and survival analysis


class ColumnIDs:
    """
    Column ID groups for different types of data in UK Biobank.

    Attributes:
        CONFOUNDER_COLUMNS_ID (list): Column IDs for confounding variables including age, 
            smoking status, gender, diabetes, blood pressure, ethnicity, BMI, etc.
        ECG_COLUMNS_ID (list): Column IDs for ECG-related measurements including workload, 
            heart rate, chest pain, bike method, and test phases.
        ICD10_COLUMNS_ID (list): Column IDs for ICD10 diagnosis codes and dates.
    """

    CONFOUNDER_COLUMNS_ID = [
        # age
        34,
        52,
        # smoking status
        20116,
        # gender
        31,
        # diabetes
        2443,
        # systolic blood pressure
        4080,
        # ethnicity
        21000,
        # BMI
        50,
        21002,
        # treatment for hypertension
        6177,
        6153,
        6150,
        # total cholesterol
        30690,
        # HDL cholesterol
        30760,
        # education attainment
        6138,
        # IPAQ activity group
        22032,
    ]

    ECG_COLUMNS_ID = [
        # maximum workload
        6032,
        # maximum heart rate
        6033,
        # chest pain felt
        6015,
        6016,
        # bike method for fitness test
        6019,
        # completion status of test
        6020,
        # phase name (pretest, exercise, rest)
        5991,
        # phase duration in seconds
        5992,
        # number of stages in a phase
        5993,
        # number of trend entries
        6038,
        # Following fields have at most 114 values  -------
        # heart rate
        5983,
        # load
        5984,
        # bicycle speed
        5985,
        # phase time
        5986,
        # trend phase name (pretest, exercise, ...)
        5987,
        # stage name (steady, constant, ...)
        5988,
    ]

    ICD_COLUMNS_ID = [
        # ICD10 code: primary + secondary
        41270,
        # date of diagnosis
        41280,
        # treatment code, used to check statin use
        20003
    ]


class ColumnNames:
    """
    Column names for different types of Heart Rate Variability (HRV) measurements in UK Biobank.
    
    This class defines lists of column names for different categories of HRV analysis:
    
    Attributes:
        POINCARE_COLUMNS_NAME (list): Poincaré plot-based measures
            - Geometric measures (SD1, SD2, Area)
            - Asymmetry indices (GI, SI, AI, PI)
            - Acceleration/Deceleration patterns
            - Cardiac indices (CSI, CVI)
            
        ENTROPY_COLUMNS_NAME (list): Various entropy-based measures
            - Approximate Entropy (ApEn)
            - Sample Entropy (SampEn)
            - Multiscale Entropy variants (MSEn, CMSEn, RCMSEn)
            - Other entropy measures (ShanEn, FuzzyEn)
            
        FRACTAL_COLUMNS_NAME (list): Fractal and complexity measures
            - Fractal Dimensions (CD, HFD, KFD)
            - Detrended Fluctuation Analysis (DFA)
            - Multifractal DFA (MFDFA) indices
            - Complexity measures (LZC)
            
    Notes:
        - All column names are prefixed with 'HRV_'
        - MFDFA indices include multiple parameters (Width, Peak, Mean, etc.)
        - These measures are calculated using the neurokit2 package
        - Values are stored as floating-point numbers in the database
    """

    POINCARE_COLUMNS_NAME = [
        "HRV_SD1",          # Short-term variability
        "HRV_SD2",          # Long-term variability
        "HRV_SD1SD2",       # Ratio of SD1 to SD2
        "HRV_S",            # Area of ellipse
        "HRV_CSI",          # Cardiac Sympathetic Index
        "HRV_CVI",          # Cardiac Vagal Index
        "HRV_CSI_Modified",  # Modified CSI
        "HRV_PIP",          # Poincaré plot indices
        "HRV_IALS",         # Index of asymmetry
        "HRV_PSS",          # Phase space symmetry
        "HRV_PAS",          # Phase space area
        "HRV_GI",           # Guzik's Index
        "HRV_SI",           # Slope Index
        "HRV_AI",           # Area Index
        "HRV_PI",           # Porta's Index
        "HRV_C1d",          # Deceleration contribution (short-term)
        "HRV_C1a",          # Acceleration contribution (short-term)
        "HRV_SD1d",         # Short-term deceleration variance
        "HRV_SD1a",         # Short-term acceleration variance
        "HRV_C2d",          # Deceleration contribution (long-term)
        "HRV_C2a",          # Acceleration contribution (long-term)
        "HRV_SD2d",         # Long-term deceleration variance
        "HRV_SD2a",         # Long-term acceleration variance
        "HRV_Cd",           # Total deceleration contribution
        "HRV_Ca",           # Total acceleration contribution
        "HRV_SDNNd",        # SDNN of decelerations
        "HRV_SDNNa",        # SDNN of accelerations
    ]

    ENTROPY_COLUMNS_NAME = [
        "HRV_ApEn",         # Approximate Entropy
        "HRV_SampEn",       # Sample Entropy
        "HRV_ShanEn",       # Shannon Entropy
        "HRV_FuzzyEn",      # Fuzzy Entropy
        "HRV_MSEn",         # Multiscale Entropy
        "HRV_CMSEn",        # Composite Multiscale Entropy
        "HRV_RCMSEn",       # Refined Composite Multiscale Entropy
    ]

    FRACTAL_COLUMNS_NAME = [
        "HRV_CD",           # Correlation Dimension
        "HRV_HFD",          # Higuchi Fractal Dimension
        "HRV_KFD",          # Katz Fractal Dimension
        "HRV_LZC",          # Lempel-Ziv Complexity
        "HRV_DFA_alpha1",   # Detrended Fluctuation Analysis (short-term)
        "HRV_DFA_alpha2",   # Detrended Fluctuation Analysis (long-term)
        # MFDFA (Multifractal Detrended Fluctuation Analysis) indices
        "HRV_MFDFA_alpha1_Width",
        "HRV_MFDFA_alpha1_Peak",
        "HRV_MFDFA_alpha1_Mean",
        "HRV_MFDFA_alpha1_Max",
        "HRV_MFDFA_alpha1_Delta",
        "HRV_MFDFA_alpha1_Asymmetry",
        "HRV_MFDFA_alpha1_Fluctuation",
        "HRV_MFDFA_alpha1_Increment",
        "HRV_MFDFA_alpha2_Width",
        "HRV_MFDFA_alpha2_Peak",
        "HRV_MFDFA_alpha2_Mean",
        "HRV_MFDFA_alpha2_Max",
        "HRV_MFDFA_alpha2_Delta",
        "HRV_MFDFA_alpha2_Asymmetry",
        "HRV_MFDFA_alpha2_Fluctuation",
        "HRV_MFDFA_alpha2_Increment",
    ]