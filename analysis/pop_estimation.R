# Estimate size of German (speaking) Twitter user population with a 
# capture recapture design

library(ggplot2)
# Given a data frame (queried ids, hits, hits_germany, recaptured) get estimated
# population size with 95% confidence intervals
estimate_population <- function(df) {
   csums <- colSums(df) 
   n_recap <- csums[4]
   n_hits_de <- csums[3]
   n_hits <- csums[2]
   n_first_sample <- 25000
   pop_est <- n_hits_de * n_first_sample / n_recap
   se_pop_est <- sqrt(n_first_sample^2 * n_hits_de * (n_hits_de - n_recap) / 
       (n_recap^2 * (n_recap + 1)))
   ci <- pop_est + 1.96 * c(-se_pop_est, se_pop_est)
   out <- c(ci[1], pop_est, ci[2], se_pop_est)
   cat(rep('=', 40))
   cat('\n')
   cat(paste('Previous sample size:', prettyNum(n_first_sample, big.mark = ','), 
                                                '\n'))
   cat(paste0('Ids queried: ', prettyNum(nrow(df), big.mark = ','), '\n'))
   cat(paste0('Twitter users found: ', prettyNum(n_hits, big.mark = ','), '\n'))
   cat(paste('German Twitter Users:', prettyNum(n_hits_de, big.mark = ','), '\n'))
   cat(paste('Number recaptured:', n_recap, '\n'))
   cat(rep('=', 40))
   cat('\n')
   cat(paste0('Estimated population size: ', prettyNum(pop_est, big.mark = ','), 
              " (+/- ", prettyNum(1.96 * se_pop_est, big.mark = ','), ')\n'))
   names(out) <- c("lower", "point_estimate", "upper", "sd")
   return(out)
}

# Get estimate for specified y (inverse sampling)
y <- 30
df1 <- read.csv('../data/crc_results.csv')
csum <- cumsum(df1[, 4])
i <- which(csum == y)[1] 
df <- df1[1:i, ]
res <- estimate_population(df)

# Use proportion estimates (see proposal manuscript for now) to get absolute
# numbers and carry uncertainty forward with bootstrapping

## Bootstrap point estimates
bs_abs_est <- function(point, prop) {
    set.seed(29866638)
    taus <- rnorm(10000, point[1], point[2])
    prop_estimates <- rnorm(10000, prop[1], prop[2])
    abs_ci <- quantile(taus * prop_estimates, c(0.025, 0.975))
    abs_mean <- mean(taus * prop_estimates)
    out <- c(abs_ci[1], abs_mean, abs_ci[2])
    names(out) <- c("lower", "mean", "upper")
    return(out)
}

### For users with at least 1 tweet
point <- c(res["point_estimate"], res["sd"])
prop <- c(0.06, 0.0039 / 1.96)
res_1 <- bs_abs_est(point, prop)

### For users with at least 1 tweet
point <- c(res["point_estimate"], res["sd"])
prop <- c(0.008, 0.002 / 1.96)
res_10 <- bs_abs_est(point, prop)

### For users with at least 1 tweet
point <- c(res["point_estimate"], res["sd"])
prop <- c(0.006, 0.0003 / 1.96)
res_100 <- bs_abs_est(point, prop)

res_1
res_10
res_100

# Make plot of the sampling progression
stepsize <- 100000
maxrow <- NULL
if(is.null(maxrow)) {
    df1 <- read.csv('crc_results.csv')
} else {
    df1 <- read.csv('crc_results.csv', nrow = maxrow)
}

ests <- lapply(seq(250000, nrow(df1), stepsize), function(x) {
        df <- head(df1, x)
        estimate_population(df)
    }
)
ests_df <- as.data.frame(do.call(rbind, ests))
ests_df$sample_size <- seq(250000, nrow(df1), stepsize)

p <- ggplot(ests_df) + 
    geom_ribbon(aes(x = sample_size, ymin = lower, ymax = upper),
                alpha = 0.5, fill = 'blue') +
    geom_line(aes(x = sample_size, y = point_estimate)) + 
    theme_bw()
ggsave(plot = p, 'pop_estimation.png')


