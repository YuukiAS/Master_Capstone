# * We will only use MissForest imputation. MICE will not be used.
setwd("/work/users/y/u/yuukias/BIOS-Material/BIOS992/src/step3_impute_split_data/impute_data")
load("eligible_data_imputed_missForest.RData")

OOBerror <- data_imputed$OOBerror
print(OOBerror)

ximp <- data_imputed$ximp
write.csv(ximp, "eligible_data_imputed_missForest.csv", row.names = FALSE)

# NRMSE (Normalized Root Mean Square Error) = 5.42e-06 is close to 0, meaning that the imputation for continuous variables is good.

# PFC (Proportion of Falsely Classified) = 0.2732, meaning that the imputation for categorical variables is a bit worse than that for continuous variables.











