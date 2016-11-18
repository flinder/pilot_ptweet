# Estimate size of German (speaking) Twitter user population with a 
# capture recapture design


# Given a data frame (queried ids, hits, hits_germany, recaptured) get estimated
# population size with 95% confidence intervals
estimate_population <- function(df) {
   csums <- colSums(df) 
   n_recap <- csums[4]
   n_hits_de <- csums[3]
   n_first_sample <- 25000
   pop_est <- n_hits_de * n_first_sample / n_recap
   se_pop_est <- sqrt(n_first_sample^2 * n_hits_de * (n_hits_de - n_recap) / 
       (n_recap^2 * (n_recap + 1)))
   ci <- pop_est + 1.96 * c(-se_pop_est, se_pop_est)
   out <- c(ci[1], pop_est, ci[2])
   names(out) <- c("lower", "point", "upper")
   return(out)
}

df <- read.csv('crc_results.csv', nrows = 50000)
estimate_population(df)