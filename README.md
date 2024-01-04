# fin-big-data_project
EPFL - Financial Big Data Project - Autumn 2023

# Project: Building trading strategy using market states

## Find market states and trading signals
- Start with us equities
- 	Upload and clean data
- 	divide in insample and outsample data
- Eigenvalue clipping for the covariance
- Find clusters from insample
- In a certain time cluster the signal can be positive, neutral or negative and based on that we adapt the strategy -> We need to find the signal which is the average of the average return of the same type of market state in the past (previous day).
- Hypothesis testing, H0: average return different from 0. If H0 not rejected, we can say if the (Same)signal is positive or negative. We use the T-statistic and a false-discovery rate (see last lecture).
- Test clusters with outsample data

## Evaluate and apply strategy
- Build strategy update using the signal
- Visualize the results (log returns plot, see lecture notes for other metrics like some heat map)

## Risk management step?
- Do not concentrate on that

## Analysis and report


TO DO:

- Data: US equities, crypto and or forex exchange?\\
- Clean data\\
- For different time frames (say 1 month?)\\
	- Cluster for intraday data\\
	- Derive a strategy, how? -> using market states returns (see slide 6, lecture 13)\\
	- Backtest strategy\\
- Visualise\\
- Conclusion on our strategies?\\
