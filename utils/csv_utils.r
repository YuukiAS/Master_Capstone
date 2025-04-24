clear <- function() {
    cat("\033[2J\033[H")
}

select_subset <- function(df, type) {
    # We don't need eid for the model.
    base_cols <- c("event", "time")
    covariate_cols <- c(
        "age", "sex", "ethnicity", "BMI",
        "smoking", "diabetes", "systolic_bp", "hypertension_treatment",
        "total_chol", "hdl_chol", "education", "activity",
        "max_workload", "max_heart_rate"
    )

    hrv_traditional_cols <- c(
        # Time domain
        "HRV_MeanNN", "HRV_SDNN", "HRV_RMSSD", "HRV_SDSD", "HRV_CVNN",
        "HRV_CVSD", "HRV_MedianNN", "HRV_MadNN", "HRV_MCVNN", "HRV_IQRNN",
        "HRV_SDRMSSD", "HRV_Prc20NN", "HRV_Prc80NN", "HRV_pNN50", "HRV_pNN20",
        "HRV_MinNN", "HRV_MaxNN", "HRV_HTI", "HRV_TINN",
        # Frequency domain
        "HRV_LF", "HRV_HF", "HRV_VHF", "HRV_TP", "HRV_LFHF", "HRV_LFn",
        "HRV_HFn", "HRV_LnHF"
    )

    # 1. SampEn should be excluded due to infinite values
    # 2. All variables for MFDFA_alpha2 should be excluded due to very high missing rate:
    # "HRV_DFA_alpha2", "HRV_MFDFA_alpha2_Width", "HRV_MFDFA_alpha2_Peak",
    # "HRV_MFDFA_alpha2_Mean", "HRV_MFDFA_alpha2_Max", "HRV_MFDFA_alpha2_Delta",
    # "HRV_MFDFA_alpha2_Asymmetry", "HRV_MFDFA_alpha2_Fluctuation", "HRV_MFDFA_alpha2_Increment"
    hrv_nonlinear_cols <- c(
        # Poincare
        "HRV_SD1", "HRV_SD2", "HRV_SD1SD2", "HRV_S", "HRV_CSI", "HRV_CVI",
        "HRV_CSI_Modified", "HRV_PIP", "HRV_IALS", "HRV_PSS", "HRV_PAS",
        "HRV_GI", "HRV_SI", "HRV_AI", "HRV_PI", "HRV_C1d", "HRV_C1a",
        "HRV_SD1d", "HRV_SD1a", "HRV_C2d", "HRV_C2a", "HRV_SD2d", "HRV_SD2a",
        "HRV_Cd", "HRV_Ca", "HRV_SDNNd", "HRV_SDNNa",
        # Entropy
        "HRV_ApEn", "HRV_ShanEn", "HRV_FuzzyEn", "HRV_MSEn",
        "HRV_CMSEn", "HRV_RCMSEn",
        # Fractal
        "HRV_CD", "HRV_HFD", "HRV_KFD", "HRV_LZC", "HRV_DFA_alpha1",
        "HRV_MFDFA_alpha1_Width", "HRV_MFDFA_alpha1_Peak",
        "HRV_MFDFA_alpha1_Mean", "HRV_MFDFA_alpha1_Max", "HRV_MFDFA_alpha1_Delta",
        "HRV_MFDFA_alpha1_Asymmetry", "HRV_MFDFA_alpha1_Fluctuation",
        "HRV_MFDFA_alpha1_Increment"
    )


    if (type == "full") { # fully-adjusted model
        selected_cols <- c(base_cols, covariate_cols, hrv_traditional_cols, hrv_nonlinear_cols)
        return(df[, selected_cols])
    } else if (type == "partial") { # partially-adjusted model
        selected_cols <- c(base_cols, hrv_traditional_cols, hrv_nonlinear_cols)
        return(df[, selected_cols])
    } else if (type == "minimal") { # minimally-adjusted model
        selected_cols <- c(base_cols, hrv_nonlinear_cols)
        return(df[, selected_cols])
    } else {
        stop("Invalid model type")
    }
}

# Determine whether a variable is categorical or numeric
determine_type <- function(variable_name) {
    if (variable_name %in% c("sex", "ethnicity", "smoking", "diabetes", "hypertension_treatment", "education", "activity")) {
        return("categorical")
    } else {
        return("numeric")
    }
}

# Determine the category of a variable
determine_category <- function(variable_name) {
    if (variable_name %in% c("age", "sex", "ethnicity", "BMI", "smoking", "diabetes", "systolic_bp", "hypertension_treatment", "total_chol", "hdl_chol", "education", "activity", "max_workload", "max_heart_rate")) {
        return("covariate")
    } else if (variable_name %in% c("HRV_MeanNN", "HRV_SDNN", "HRV_RMSSD", "HRV_SDSD", "HRV_CVNN", "HRV_CVSD", "HRV_MedianNN", "HRV_MadNN", "HRV_MCVNN", "HRV_IQRNN", "HRV_SDRMSSD", "HRV_Prc20NN", "HRV_Prc80NN", "HRV_pNN50", "HRV_pNN20", "HRV_MinNN", "HRV_MaxNN", "HRV_HTI", "HRV_TINN")) {
        return("time")
    } else if (variable_name %in% c("HRV_LF", "HRV_HF", "HRV_VHF", "HRV_TP", "HRV_LFHF", "HRV_LFn", "HRV_HFn", "HRV_LnHF")) {
        return("frequency")
    } else if (variable_name %in% c("HRV_SD1", "HRV_SD2", "HRV_SD1SD2", "HRV_S", "HRV_CSI", "HRV_CVI", "HRV_CSI_Modified", "HRV_PIP", "HRV_IALS", "HRV_PSS", "HRV_PAS", "HRV_GI", "HRV_SI", "HRV_AI", "HRV_PI", "HRV_C1d", "HRV_C1a", "HRV_SD1d", "HRV_SD1a", "HRV_C2d", "HRV_C2a", "HRV_SD2d", "HRV_SD2a", "HRV_Cd", "HRV_Ca", "HRV_SDNNd", "HRV_SDNNa")) {
        return("poincare")
    } else if (variable_name %in% c("HRV_ApEn", "HRV_ShanEn", "HRV_FuzzyEn", "HRV_MSEn", "HRV_CMSEn", "HRV_RCMSEn")) {
        return("entropy")
    } else if (variable_name %in% c("HRV_CD", "HRV_HFD", "HRV_KFD", "HRV_LZC", "HRV_DFA_alpha1", "HRV_MFDFA_alpha1_Width", "HRV_MFDFA_alpha1_Peak", "HRV_MFDFA_alpha1_Mean", "HRV_MFDFA_alpha1_Max", "HRV_MFDFA_alpha1_Delta", "HRV_MFDFA_alpha1_Asymmetry", "HRV_MFDFA_alpha1_Fluctuation", "HRV_MFDFA_alpha1_Increment")) {
        return("fractal")
    } else {
        return("unknown")
    }
}


count_missing_rate <- function(df) {
    missing_rate <- sapply(df, function(x) sum(is.na(x)) / length(x))
    return(missing_rate)
}


get_data_path <- function(prefix, adjust_type, impute_type, include_statin, model = NULL) {
    if (is.null(model)) {
        path <- if (include_statin == "yes") {
            paste0(prefix, "_", adjust_type, "_", impute_type, "_statin.RData")
        } else {
            paste0(prefix, "_", adjust_type, "_", impute_type, ".RData")
        }
        return(path)
    } else if (model == "cox") {
        work_dir <- "/work/users/y/u/yuukias/BIOS-Material/BIOS992/src/step4_build_survival_model/Cox"
        path <- if (include_statin == "yes") {
            paste0(work_dir, "/", prefix, "_", adjust_type, "_", impute_type, "_statin.RData")
        } else {
            paste0(work_dir, "/", prefix, "_", adjust_type, "_", impute_type, ".RData")
        }
        return(path)
    } else if (model == "rsf") {
        work_dir <- "/work/users/y/u/yuukias/BIOS-Material/BIOS992/src/step4_build_survival_model/RSF"
        path <- if (include_statin == "yes") {
            paste0(work_dir, "/", prefix, "_", adjust_type, "_", impute_type, "_statin.RData")
        } else {
            paste0(work_dir, "/", prefix, "_", adjust_type, "_", impute_type, ".RData")
        }
        return(path)
    } else if (model == "xgb") {
        work_dir <- "/work/users/y/u/yuukias/BIOS-Material/BIOS992/src/step4_build_survival_model/XGBoost"
        path <- if (include_statin == "yes") {
            paste0(work_dir, "/", prefix, "_", adjust_type, "_", impute_type, "_statin.RData")
        } else {
            paste0(work_dir, "/", prefix, "_", adjust_type, "_", impute_type, ".RData")
        }
        return(path)
    } else if (model == "sensitivity") {
        work_dir <- "/work/users/y/u/yuukias/BIOS-Material/BIOS992/src/step8_sensitivity_analysis"
        path <- if (include_statin == "yes") {
            paste0(work_dir, "/", prefix, "_", adjust_type, "_", impute_type, "_statin.RData")
        } else {
            paste0(work_dir, "/", prefix, "_", adjust_type, "_", impute_type, ".RData")
        }
        return(path)
    } else {
        stop("Invalid model type!")
    }
}
