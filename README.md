# EPFL - Financial Big Data Project - Autumn 2023

# Performance of Market States Based Trading Strategies

Final project of the course MATH-525 Financial Big Data (EPFL, Fall 2023).

The report is available [here](report.pdf).

## Installation and Requirements

The requirements are listed in the [requirements.txt](requirements.txt) file. To install them, run the following command in the root directory of the project:

``` bash
pip install -r requirements.txt
```
To run the code of the project one has to run the notebooks described below with the data provided in the hard-disk.
## Brief description of the repository

```         
fin-big-data_project/
│
├── [upload_data.py](upload_data.py)                            # Functions to upload and clean data
├── [utils.py](utils.py)                                        # Various utils functions
├── [data_preprocessing.py](data_preprocessing.py)              # Functions to further clean the data for pre-processing
├── [market_state_strategy.py](market_state_strategy.py)        # Functions to implement the clustering and the strategies
├── [data_cleaning.ipynb](data_cleaning.ipynb)                  # Notebook to clean the data
├── [data_preprocessing.ipynb](data_preprocessing.ipynb)        # Notebook of the preprocessing of the data
├── [data_observation.ipynb](data_observation.ipynb)            # Notebook to observe the clean and processed data and to compute statistics 
├── [strateg_test.ipynb](strateg_test.ipynb)                    # Notebook containing all the strategies and their performance
├── ResearchNotebooks/                                          # Folder with deprecated notebooks used for exploration and various tests
|
├── [requirements.txt](requirements.txt)                        # Requirement files
|
└── README.md 
```

### Authors

-   [Zetong Liu](https://github.com/ZetongLiu)
-   [Charles Gendreau](https://github.com/kalos11)

### Supervisors

-   [Damien Challet](https://people.epfl.ch/damien.challet)

-   [Federico Baldi Lanfranchi](https://people.epfl.ch/federico.baldilanfranchi)

Last edited: 2024-02-01
